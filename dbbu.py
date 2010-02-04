#!/usr/bin/python
"""
A simple script to automate remote PostgreSQL backups
"""

import os
import sys
import time
import logging
import subprocess


__all__ = (
    'Backup',
    'PostgreSQL',
    'MySQL',
)

logger = logging.getLogger('dbbu')
DATABASES_TO_IGNORE = set(['template0'])


class Backup(object):
    """ Base backup class to handle common remote commands """

    def __init__(self, host, dest, **kwargs):
        self.host = host
        self.dest = os.path.join(dest, self.host)
        self.compression = kwargs.pop('compression', 'gzip')
        self.sudo_user = kwargs.pop('sudo_user', '')
        self.fmod = kwargs.pop('fmod', 0600)
        databases = kwargs.pop('databases', [])
        self.databases = set(databases)
        if not os.path.exists(self.dest):
            os.makedirs(self.dest)
        self.logger = logging.getLogger('dbbu.%s' % self.host.replace('.', '-'))

    def execute(self, cmd, comm=True, **kwargs):
        if 'stderr' not in kwargs:
            kwargs['stderr'] = subprocess.PIPE
        self.logger.debug(cmd)
        p = subprocess.Popen(cmd, shell=True, **kwargs)
        if comm:
            out, err = p.communicate()
            if err:
                self.logger.error(err)
            p.out = out
            p.err = err
        return p

    def remote(self, cmd, **kwargs):
        args = {'host': self.host, 'cmd': cmd}
        ssh = 'ssh -C %(host)s %(cmd)s' % args
        return self.execute(ssh, **kwargs)

    def sudo(self, cmd):
        args = {'cmd': cmd}
        if self.sudo_user:
            args['user'] = ' -u %(user)s '.format({'user': self.sudo_user})
        else:
            args = {'user': ''}
        return 'sudo %(user)s %(cmd)s'.format(args)
    
    def chmod(self, path):
        os.chmod(path, self.fmod)
    
    


class PostgreSQL(Backup):
    """
    PostgreSQL backup class to dump globals and all databases individually
    """

    def run(self):
        self.backup_postgres_globals()
        databases = set(self.get_postgres_databases())
        if self.databases:
            databases = self.databases
        databases -= DATABASES_TO_IGNORE
        self.logger.info('databases: %s', databases)
        for database in databases:
            self.logger.info('backing up %s' % database)
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
        filename = 'globals.%s' % self.compression
        path = os.path.join(self.dest, filename)
        fh = open(path, 'w+')
        cmd = "pg_dumpall --globals-only | %s" % self.compression
        self.remote(cmd, stdout=fh)
        fh.close()
        self.chmod(path)

    def backup_postgres_database(self, database):
        filename = '%s.%s' % (database, self.compression)
        path = os.path.join(self.dest, filename)
        cmd = "pg_dump -i %s | %s" % (database, self.compression)
        fh = open(path, 'w+')
        self.remote(cmd, stdout=fh)
        fh.close()
        self.chmod(path)


class MySQL(Backup):
    """ MySQL backup recipe """

    def run(self):
        self.backup_all_databases()

    def backup_all_databases(self):
        filename = 'mysqldumpall.sql.%s' % self.compression
        path = os.path.join(self.dest, filename)
        cmd = "mysqldump -uroot --all-databases | %s" % self.compression
        fh = open(path, 'w+')
        self.remote(cmd, stdout=fh)
        fh.close()
        self.chmod(path)
