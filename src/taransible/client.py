import random
import socket
import logging

import ansible
from ansible.playbook import PlayBook
from ansible.inventory import Inventory

from taransible.utils import construct_name

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

class ClientWrapper(object):
    playbook    = '11_client_%s.yml'
    timeout     = 1200
    forks       = 10
    client_port = 80

    def __init__(self, client_host, custom, result_dir):
        self.opts = {}
        self.opts.update(custom)
        self.opts['result_dir'] = result_dir

    def base_playbook(self, cmd, custom = None):
        binv  = ansible.inventory.Inventory('hosts')
        #binv.subset('*' + self.host['host'])
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

    def prepare(self):
        self.base_playbook('prepare')

    def run(self, fname, name='tank'):
        tank = {
            'name': fname,
        }
        self.opts['tanks'] = [tank]
        self.opts['dir_name'] = name
        self.prepare()
        self.base_playbook('run')

    def run_write(self, fnames, name='tank'):
        if len(fnames) != 2:
            return -1
        tanks = []
        for i, fname in enumerate(fnames):
            tank = {
                'name': fname,
            }
            tanks.append(tank)
        self.opts['tanks'] = tanks
        self.opts['dir_name'] = name
        self.prepare()
        self.base_playbook('run')

    def fetch(self):
        self.base_playbook('fetch')
