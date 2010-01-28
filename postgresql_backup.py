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


def remote(cmd, **kwargs):
    ssh = ['ssh', '-C', HOST] + [cmd]
    args = [ssh]
    logging.debug(' '.join(ssh))
    p = subprocess.Popen(*args, **kwargs)
    out, err = p.communicate()
    if err:
        logging.error(err)
    return out, err


def as_postgres(cmd, **kwargs):
    return remote("sudo -u postgres " + cmd, **kwargs)


def get_postgres_databases():
    sql = "SELECT datname FROM pg_database"
    out, err = \
        as_postgres("psql -c '%s'" % sql, stdout=subprocess.PIPE)
    out = out.split('\n')
    out = out[2:len(out) - 3]
    out = map(lambda x: x.strip(), out)
    return out


def backup_postgres_globals():
    path = os.path.join(ROOT, 'globals.gzip')
    f = open(path, 'w+')
    as_postgres("pg_dumpall --globals-only | gzip", stdout=f)
    f.close()


def backup_postgres_database(database):
    path = os.path.join(ROOT, database + '.gzip')
    f = open(path, 'w+')
    as_postgres("pg_dump -i %s | gzip" % database, stdout=f)
    f.close()


databases = \
    filter(lambda x: x not in DATABASES_TO_IGNORE, get_postgres_databases())
logging.info('databases %s', databases)
logging.info('backing up postgres globals')
backup_postgres_globals()
for database in databases:
    logging.info('backing up %s' % database)
    backup_postgres_database(database)
