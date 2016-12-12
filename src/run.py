#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

import os
import json
import errno
import random
import socket
import getpass
import logging
import itertools

import yaml
import sys
import ansible
import ansible.playbook

from taransible.utils    import ansible_run, ansible_display, log, formatter
from taransible.database import TarantoolWrapper, snapshot
from taransible.client   import ClientWrapper
from taransible.nginx    import NginxWrapper

# Hack ansible to log to our logger object
ansible.callbacks.display = ansible_display

def build_inventory(srv_groups):
    def construct_host(cfg_entry):
        dbhost = cfg_entry
        host = dbhost['host']
        port = dbhost.get('port', None)
        user = dbhost.get('user', None)
        opts = dbhost.get('ansible_options', {})
        if user is not None:
            host = '%s@%s' % (user, host)
        if port is not None:
            host = '%s:%s' % (host, port)
        opts = ['%s=%s' % (k, repr(v)) for k, v in opts.iteritems() if bool(v) == True]
        opts = ' '.join(opts)
        if opts:
            opts = ' ' + opts
        host = host + opts
        return host

    def build_section(buf, name, servers):
        buf.write('[%s]\n' % name)
        for srv in servers:
            buf.write(construct_host(srv))
            buf.write('\n')
        buf.write('\n')

    buf = StringIO()
    for k, v in srv_groups.iteritems():
        build_section(buf, k, v)
    return buf.getvalue()

def get_inventory(cfg):
    client_port   = 80

    storage_hosts = cfg.get('storage', {}).get('hosts', [])
    app_hosts     = cfg.get('app', {}).get('hosts', [])
    client_hosts  = cfg.get('client', {}).get('hosts', [])
    for host in client_hosts:
        target = {
            'host': host.get('target', ''),
            'port': client_port
        }
        host['ansible_options']['target'] = json.dumps(target)

    inv = build_inventory({
        'storage': storage_hosts,
        'app':     app_hosts,
        'client':  client_hosts,
        'node_down': [storage_hosts[0], ] # turn off tarantools in first storage server
    })

    with open('hosts', 'w') as f:
        f.write(inv)
        deploy = cfg.get('deploy', {})
        if len(deploy.keys()):
            confs = deploy.get('hosts', [])
            f.write('\n[deploy]\n')
            for conf in confs:
                f.write('%s@%s\n' % (
                    conf.get('user'), conf.get('host')
                ))

    return ansible.inventory.Inventory('hosts')

def deploy_tarantool(cfg):
    shard_hosts = []

    storage_tnts  = []
    storage_hosts = cfg.get('storage', {}).get('hosts', [])
    ansible_opts  = cfg.get('storage', {}).get('ansible_options', {})
    for i, host in enumerate(storage_hosts): # 'i' becames zone number
        tnt_inst = host.get('tarantool_instances', [])
        tnt_name = ansible_opts['name'] # Mandatory Field
        for j, tnt_inst in enumerate(tnt_inst, start=1): # 'j' becames instance number
            uri = '%s:%s' % (host['host'], tnt_inst)
            opts = ansible_opts.copy()
            opts['number'] = j
            opts['shards'] = shard_hosts
            opts['listen'] = tnt_inst
            opts['redundancy'] = ansible_opts.get('redundancy', 2)
            opts['pair'] = host.get('pair', 0)
            opts['role'] = host.get('role', 0)
            shard_hosts.append({ 'zone': i, 'uri': uri })
            storage_tnts.append(TarantoolWrapper(opts, host))

    nginx_list    = []
    app_tnts      = []
    app_hosts     = cfg.get('app', {}).get('hosts', [])
    ansible_opts  = cfg.get('app', {}).get('ansible_options', {})
    for host in app_hosts:
        tnt_inst   = host.get('tarantool_instances', [])
        tnt_name   = ansible_opts['name'] # Mandatory Field
        apps_hosts = []
        for j, tnt_inst in enumerate(tnt_inst, start=1): # 'j' becames instance number
            uri = '127.0.0.1:%s' % tnt_inst
            opts = ansible_opts.copy()
            opts['number'] = j
            opts['shards'] = shard_hosts
            opts['listen'] = tnt_inst
            opts['redundancy'] = ansible_opts.get('redundancy', 2)
            opts['expire_ttl'] = ansible_opts.get('expire_ttl', 0)
            apps_hosts.append({ 'uri': uri })
            app_tnts.append(TarantoolWrapper(opts, host))
        nginx_list.append(NginxWrapper(host, apps_hosts, shard_hosts))

    for db in itertools.chain(storage_tnts, app_tnts):
        db.cleanup()
        db.deploy()
        db.start()

    for nginx in nginx_list:
        nginx.deploy()

    return [storage_tnts, app_tnts, nginx_list]

def parse_config(cfg_path, name):
    log.debug('cfg: %s parsing', name)
    with open(cfg_path, 'r') as cfgfile:
        cfg = yaml.load(cfgfile)
    log.debug('cfg: done')
    return cfg

def main():
    cfg = parse_config('config.yml', 'benchmark config')

    timeout = cfg.get('ansible', {}).get('timeout', 1200)
    sudo = cfg.get('ansible', {}).get('sudo', False)

    # Set ansible
    inventory = get_inventory(cfg)
    stats = ansible.callbacks.AggregateStats()
    playbook_cb = ansible.callbacks.PlaybookCallbacks(
        verbose=ansible.utils.VERBOSITY
    )
    runner_cb = ansible.callbacks.PlaybookRunnerCallbacks(
        stats, verbose=ansible.utils.VERBOSITY
    )

    result_dir = cfg.get('ansible', {}).get('output', 'out')
    tmp_result_dir = result_dir
    num = 0
    while True:
        try:
            os.mkdir(tmp_result_dir)
        except OSError as e:
            if e.errno == errno.EEXIST:
                num += 1
                tmp_result_dir = '%s-%0.2d' % (result_dir, num)
                continue
            raise
        break
    result_dir = tmp_result_dir

    fh = logging.FileHandler(os.path.join(result_dir, "benchmark.log"))
    fh.setFormatter(formatter)
    log.addHandler(fh)

    # Cleanup servers, stop instances
    log.info("cleanup")
    pb = ansible.playbook.PlayBook(
        playbook='01_cleanup.yml',
        inventory=inventory,
        forks=len(inventory.get_hosts('hosts_all')) + 5,
        callbacks=playbook_cb,
        runner_callbacks=runner_cb,
        stats=stats,
        sudo=sudo,
        timeout=timeout,
        any_errors_fatal=True
    )
    if not ansible_run(pb):
        return 1

    log.info("ping")
    pb = ansible.playbook.PlayBook(
        playbook='03_ping.yml',
        inventory=inventory,
        forks=len(inventory.get_hosts('hosts_all')) + 5,
        callbacks=playbook_cb,
        runner_callbacks=runner_cb,
        stats=stats,
        sudo=sudo,
        timeout=timeout,
        any_errors_fatal=True
    )
    if not ansible_run(pb):
        return 1

    storages, apps, nginxs = deploy_tarantool(cfg)
    storage_hosts = cfg.get('storage', {}).get('hosts', [])
    app_hosts     = cfg.get('app',     {}).get('hosts', [])
    client_hosts  = cfg.get('client',  {}).get('hosts', [])

    # Run benchmarks
    client = ClientWrapper(
        client_hosts, cfg.get('ansible', {}).get('tank',{}),
        result_dir
    )
    print 'Done'
    return 0

if __name__ == '__main__':
    exit(main())
