import subprocess
from subprocess import PIPE

resis_server = 'D:/VirtualMachines/Redis-x64-3.2/redis-server.exe'
mongo_server = 'C:/Program Files/MongoDB/Server/3.2/bin/mongod'


def run(args):
    return subprocess.Popen(args, stdout=PIPE, stderr=PIPE)

p1 = run(resis_server)
p2 = run([mongo_server, '--dbpath', 'data'])

while True:
    if input('>') == 'q':
        break
