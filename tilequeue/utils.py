import psutil
import os
import signal
import sys


def kill_proc_tree(pid, including_parent=True):
    parent = psutil.Process(pid)
    for child in parent.get_children(recursive=True):
        child.kill()
    if including_parent:
        parent.kill()


def receive_signal(signum, stack):
    if signum in [1, 2, 3, 15]:
        print 'Caught signal %s, exiting.' % (str(signum))
        kill_proc_tree(os.getpid())
        sys.exit()
    else:
        print 'Caught signal %s, ignoring.' % (str(signum))


def trap_signal():
    uncatchable = ['SIG_DFL', 'SIGSTOP', 'SIGKILL']
    for i in [x for x in dir(signal) if x.startswith("SIG")]:
        if i not in uncatchable:
            signum = getattr(signal, i)
            signal.signal(signum, receive_signal)
