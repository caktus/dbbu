#!/usr/bin/python
"""
A simple script to automate remote PostgreSQL backups
"""

import os
import sys
import time
import logging
import subprocess
#import pdb
#pdb.set_trace()


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
        self.dest = dest
        self.user = kwargs.pop('user', None)
        self.compression = kwargs.pop('compression', 'gzip')
        self.sudo_user = kwargs.pop('sudo_user', '')
        self.fmod = kwargs.pop('fmod', 0600)
        databases = kwargs.pop('databases', [])
        self.databases = set(databases)
        if not os.path.exists(self.dest):
            os.makedirs(self.dest)
        self.logger = \
            logging.getLogger('dbbu.%s' % self.host.replace('.', '-'))

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
        if self.sudo_user:
            cmd = self.sudo(cmd)
        remote_cmd = ['ssh -C']
        if self.user:
            remote_cmd.extend(['-l', self.user])
        remote_cmd.extend([self.host, cmd])
        self.logger.debug(remote_cmd)
        return self.execute(' '.join(remote_cmd), **kwargs)

    def sudo(self, cmd):
        sudo_cmd = ['sudo']
        if self.sudo_user:
            sudo_cmd.append('-u %s' % self.sudo_user)
        sudo_cmd.append(cmd)
        self.logger.debug(sudo_cmd)
        return ' '.join(sudo_cmd)

    def chmod(self, path):
        os.chmod(path, self.fmod)


class PostgreSQL(Backup):
    """
    PostgreSQL backup class to dump globals and all databases individually
    """

    def __init__(self, host, dest, **kwargs):
        dest = os.path.join(dest, host, 'postgres')
        super(PostgreSQL, self).__init__(host, dest, **kwargs)

    def run(self):
        self.backup_postgres_globals()
        databases = set(self.get_postgres_databases())
        if self.databases:
            databases = self.databases
        databases -= DATABASES_TO_IGNORE
        self.logger.debug('databases: %s', databases)
        for database in databases:
            self.logger.info('backing up %s' % database)
            self.backup_postgres_database(database)

    def get_postgres_databases(self):
        proc = self.remote("psql -l | awk '/^ [^ ]/ { print $1 } '",
                           stdout=subprocess.PIPE)
        return [line for line in proc.out.split('\n') if line != '']

    def backup_postgres_globals(self):
        path = os.path.join(self.dest, 'globals.%s' % self.compression)
        fh = open(path, 'w+')
        cmd = "pg_dumpall --globals-only | %s" % self.compression
        self.remote(cmd, stdout=fh)
        fh.close()
        self.chmod(path)

    def backup_postgres_database(self, database):
        path = os.path.join(self.dest, '%s.%s' % (database, self.compression))
        cmd = "pg_dump -i %s | %s" % (database, self.compression)
        fh = open(path, 'w+')
        self.remote(cmd, stdout=fh)
        fh.close()
        self.chmod(path)


class MySQL(Backup):
    """ MySQL backup recipe """

    def __init__(self, host, dest, **kwargs):
        dest = os.path.join(dest, host, 'mysql')
        super(MySQL, self).__init__(host, dest, **kwargs)

    def run(self):
        self.backup_all_databases()

    def backup_all_databases(self):
        self.logger.info('backing up all MySQL databases')
        filename = 'mysqldumpall.sql.%s' % self.compression
        path = os.path.join(self.dest, filename)
        cmd = "mysqldump --all-databases | %s" % self.compression
        fh = open(path, 'w+')
        self.remote(cmd, stdout=fh)
        fh.close()
        self.chmod(path)
