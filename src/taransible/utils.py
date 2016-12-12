import logging

log = logging.getLogger('benchmark')
log.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)-5s: %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
log.addHandler(console_handler)
file_handler = logging.FileHandler('all.log')
file_handler.setFormatter(formatter)
log.addHandler(file_handler)

def ansible_display(msg, color=None, stderr=False, screen_only=False,
                    log_only=False, runner=None):
    if stderr:
        log.error(msg)
    else:
        log.info(msg)

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

def construct_name(old_name, num):
    fparts = old_name.rsplit('.', 1)
    fparts[1] = ('' if len(fparts) < 2 else '.%s' % fparts[1])
    return '%s-%d%s' % (fparts[0], num, fparts[1])
