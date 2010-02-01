#!/usr/bin/python

import os
import sys
import time
import logging
import subprocess
from optparse import OptionParser


ROOT = '/Users/copelco/Desktop/backup/'
DATABASES_TO_IGNORE = ('template0', )
logging.basicConfig(stream=sys.stdout,
                    level=logging.DEBUG)


usage = "usage: %prog [options] host-1 host-2 ... host-n"
parser = OptionParser(usage=usage)
parser.add_option("-c", "--compression", default="gzip",
                  help="compression algorithm (gzip, bzip2, etc.)")


class Backup(object):

    def __init__(self, host, sudo='', compression='gzip'):
        self.host = host
        self.sudo_user = sudo
        self.compression = compression

    def execute(self, cmd, comm=True, **kwargs):
        if 'stderr' not in kwargs:
            kwargs['stderr'] = subprocess.PIPE
        logging.debug(cmd)
        p = subprocess.Popen(cmd, shell=True, **kwargs)
        if comm:
            out, err = p.communicate()
            if err:
                logging.error(err)
            p.out = out
            p.err = err
        return p

    def remote(self, cmd, **kwargs):
        args = {'host': self.host, 'cmd': cmd}
        ssh = 'ssh -C {host} {cmd}'.format(**args)
        return self.execute(ssh, **kwargs)

    def sudo(self, cmd):
        args = {'cmd': cmd}
        if self.sudo_user:
            args['user'] = ' -u %(user)s '.format({'user': self.sudo_user})
        else:
            args = {'user': ''}
        return 'sudo %(user)s %(cmd)s'.format(args)


class PostgreSQL(Backup):

    def run(self):
        self.backup_postgres_globals()
        databases = filter(lambda x: x not in DATABASES_TO_IGNORE,
                           self.get_postgres_databases())
        logging.info('databases: %s', databases)
        for database in databases:
            logging.info('backing up %s' % database)
            self.backup_postgres_database(database)

    def get_postgres_databases(self):
        psql = "psql -q -c \\'SELECT datname FROM pg_database\\'"
        proc = self.remote(psql, stdout=subprocess.PIPE)
        out = proc.out
        out = out.split('\n')
        out = out[2:len(out) - 3]
        out = map(lambda x: x.strip(), out)
        return out

    def backup_postgres_globals(self):
        filename = 'globals.{0}'.format(self.compression)
        path = os.path.join(ROOT, filename)
        fh = open(path, 'w+')
        cmd = "pg_dumpall --globals-only | {0}".format(self.compression)
        self.remote(cmd, stdout=fh)
        fh.close()

    def backup_postgres_database(self, database):
        filename = '{0}.{1}'.format(database, self.compression)
        path = os.path.join(ROOT, filename)
        cmd = "pg_dump -i {0} | {1}".format(database, self.compression)
        fh = open(path, 'w+')
        self.remote(cmd, stdout=fh)
        fh.close()


def main():
    (options, hosts) = parser.parse_args()
    if not hosts:
        parser.print_usage()
        return -1
    for host in hosts:
        pg = PostgreSQL(host='postgres@192.168.56.3',
                        compression=options.compression)
        pg.run()


if __name__ == "__main__":
    sys.exit(main())
