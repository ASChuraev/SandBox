import math
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

class WrapperException(Exception):
    pass

def snapshot(number):
    inv   = ansible.inventory.Inventory('hosts')
    stats = ansible.callbacks.AggregateStats()
    pb_cb = ansible.callbacks.PlaybookCallbacks(verbose=ansible.utils.VERBOSITY)
    rn_cb = ansible.callbacks.PlaybookRunnerCallbacks(stats,
            verbose=ansible.utils.VERBOSITY)

    opts = {
        'name':    'storage',
        'numbers': range(1, number + 1)
    }

    pb = PlayBook(
        playbook = "10_tarantool_snapshot.yml", inventory = inv,
        forks = 30, callbacks = pb_cb, runner_callbacks = rn_cb,
        stats = stats, timeout = 3600, any_errors_fatal = True,
        extra_vars = opts
    )
    return ansible_run(pb)


class TarantoolWrapper(object):
    playbook = '10_tarantool_%s.yml'
    timeout  = 1200
    forks    = 30

#     def check_tarantool(self):
#         while True:
#             try:
#                 temp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#                 temp.connect((self.host, self.port))
#                 return True
#             except socket.error as e:
#                 if e.errno == errno.ECONNREFUSED:
#                     time.sleep(0.1)
#                     continue
#                 raise

    def __init__(self, db, dbhost):
        self.init = False
        proc = 0
        self.host = dbhost
        self.opts = db
        if db['name'].find('app') != -1:
            proc += math.pow(2, db['number'])
            self.opts['mask'] = '0x%0.8x' % proc
        elif db['name'].find('storage') != -1:
            proc += math.pow(2, (db['number'] - 1) * 2)
            proc += math.pow(2, (db['number'] - 1) * 2 + 1)
            self.opts['mask'] = '0x%0.8x' % proc
        else:
            self.opts['mask'] = '0xFFFFFFFF'

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
        self.init = True
        return self.base_playbook('deploy')

    def start(self):
        if not self.init:
            return False
        stat = self.base_playbook('start')
#         self.check_tarantool()
        return stat

    def stop(self):
        if not self.init:
            return False
        return self.base_playbook('stop')

    def status(self):
        if not self.init:
            return False
        return self.base_playbook('status')

    def cleanup(self):
        return self.base_playbook('cleanup')

    def destroy(self):
        if not self.init:
            return False
        self.init = False
        return self.base_playbook('destroy')

    def fetch_logs(self, custom):
        if not self.init:
            return False
        return self.base_playbook('fetch_logs', custom)

    def __del__(self):
#        self.stop()
      pass
