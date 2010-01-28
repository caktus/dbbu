#!/usr/bin/python

import os
import sys
import time
import logging
import subprocess


HOST = 'root@192.168.56.3'
ROOT = '/Users/copelco/Desktop/backup/'
DATABASES_TO_IGNORE = ('template0',)
logging.basicConfig(stream=sys.stdout,
                    level=logging.DEBUG)


def comm(*args, **kwargs):
    kwargs['shell'] = True
    p = subprocess.Popen(*args, **kwargs)
    out, err = p.communicate()
    if err:
        pass#logging.error(err)
    return out, err


def remote(cmd, **kwargs):
    ssh = ['ssh', '-C', HOST] + [cmd]
    args = [ssh]
    logging.debug(' '.join(ssh))
    p = subprocess.Popen(*args, **kwargs)
    out, err = p.communicate()
    if err:
        pass#logging.error(err)
    return out, err


def as_postgres(cmd, **kwargs):
    return remote("sudo -u postgres " + cmd, **kwargs)


def get_postgres_databases():
    sql = "SELECT datname FROM pg_database"
    out, err = \
        comm("psql -h 127.0.0.1 -p 7777 -U postgres -c '%s'" % sql,
             stdout=subprocess.PIPE,
             stderr=subprocess.PIPE)
    out = out.split('\n')
    out = out[2:len(out) - 3]
    out = map(lambda x: x.strip(), out)
    return out


def backup_postgres_globals():
    path = os.path.join(ROOT, 'globals.gzip')
    f = open(path, 'w+')
    comm("pg_dumpall -h 127.0.0.1 -p 7777 -U postgres --globals-only | gzip",
         stdout=f,
         stderr=subprocess.PIPE)
    f.close()


def backup_postgres_database(database):
    path = os.path.join(ROOT, database + '.gzip')
    f = open(path, 'w+')
    comm("pg_dump -h 127.0.0.1 -p 7777 -U postgres -i %s | gzip" % database,
         stdout=f,
         stderr=subprocess.PIPE)
    f.close()


def create_tunnel():
    return subprocess.Popen(["ssh -C -L 7777:localhost:5432 postgres@dev"],
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

tun = create_tunnel()
logging.info('tunnel PID %s', tun.pid)
time.sleep(3)
databases = \
    filter(lambda x: x not in DATABASES_TO_IGNORE, get_postgres_databases())
logging.info('databases %s', databases)
logging.info('backing up postgres globals')
backup_postgres_globals()
for database in databases:
    logging.info('backing up %s' % database)
    backup_postgres_database(database)
tun.terminate()

# tunnel
# real  1m9.881s
# user  0m9.895s
# sys   0m2.259s

# ssh
# real  1m25.423s
# user  0m1.305s
# sys   0m1.119s