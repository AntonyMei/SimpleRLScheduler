import time
from subprocess import *
import psutil


def main():
    p = Popen("exec bash workload_train.sh", shell=True)
    time.sleep(1)
    kill(p.pid)
    time.sleep(1)


def kill(proc_pid):
    process = psutil.Process(proc_pid)
    for proc in process.children(recursive=True):
        proc.kill()
    process.kill()


if __name__ == '__main__':
    main()
