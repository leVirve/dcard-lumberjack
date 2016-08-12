import subprocess
from subprocess import PIPE


def run(args, **kwargs):
    return subprocess.Popen(args, shell=True, **kwargs)

p1 = run('redis-server', stdout=PIPE, stderr=PIPE)
p2 = run('mongod --dbpath data'.split(), stdout=PIPE, stderr=PIPE)
p3 = run('celery -A lumberjack worker --pool=solo'.split())

while True:
    if input('>') == 'q':
        p1.terminate()
        p2.terminate()
        p3.terminate()
        break
