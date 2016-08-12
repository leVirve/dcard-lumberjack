from subprocess import Popen, PIPE


def run(args, **kwargs):
    return Popen(args, shell=True, **kwargs)

ps = [
    run('redis-server', stdout=PIPE, stderr=PIPE),
    run('mongod --dbpath data'.split(), stdout=PIPE, stderr=PIPE),
    run('celery -A lumberjack worker --pool=solo'.split()),
]

while True:
    if input('>') == 'q':
        for p in ps:
            p.terminate()
        break
