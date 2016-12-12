import time
import logging

import ansible
from ansible.playbook import PlayBook
from ansible.inventory import Inventory

from pprint import pprint

import socket

log = logging.getLogger('main')

def ansible_run(pb):
    results = pb.run()
    failed = False
    for host, info in results.iteritems():
        if info['failures'] != 0:
            log.error("host %s failed", host)
            failed = True
    if failed:
        log.error("failed to continue benchmark")
        return False
    return True


class NginxWrapper(object):
    playbook = '04_nginx_%s.yml'
    timeout  = 1200
    forks    = 30

    def __init__(self, nginx_host, tnts, shards):
        self.host = nginx_host
        self.opts = {}
        self.opts['apps'] = tnts
        self.opts['shards'] = shards

    def base_playbook(self, cmd, custom = None):
        binv  = ansible.inventory.Inventory('hosts')
        binv.subset('*' + self.host['host'])
        stats = ansible.callbacks.AggregateStats()
        pb_cb = ansible.callbacks.PlaybookCallbacks(verbose=ansible.utils.VERBOSITY)
        rn_cb = ansible.callbacks.PlaybookRunnerCallbacks(stats,
                verbose=ansible.utils.VERBOSITY)

        if custom is None:
            custom = {}
        opts = self.opts.copy()
        opts.update(custom)

        pb = PlayBook(
            playbook = self.playbook % cmd, inventory = binv,
            forks = self.forks, callbacks = pb_cb, runner_callbacks = rn_cb,
            stats = stats, timeout = self.timeout, any_errors_fatal = True,
            extra_vars = opts
        )
        return ansible_run(pb)

    def deploy(self):
        return self.base_playbook('deploy')
