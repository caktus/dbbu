#!/usr/bin/python

import os
import sys
import time
import string
import logging
import logging.handlers
import ConfigParser
from optparse import OptionParser
import dbbu


usage = """usage: %prog <path-to-configuration-file>"""
parser = OptionParser(usage=usage)

ALLOWED_LEVELS = ('NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
logger = logging.getLogger('dbbu')
formatter = \
    logging.Formatter('%(asctime)s %(name)s %(levelname)-8s %(message)s',
                      datefmt='%a, %d %b %Y %H:%M:%S')


def setup_logging(cfg):
    log_level = cfg.get('default', 'log_level')
    if log_level not in ALLOWED_LEVELS:
        sys.stderr.write('Invalid log level\n\n')
        log_level = logging.NOTSET
    else:
        log_level = getattr(logging, log_level)
    logger.setLevel(log_level)

    if cfg.has_option('default', 'log_file'):
        log_file = cfg.get('default', 'log_file')
        handler = logging.handlers.RotatingFileHandler(log_file,
                                                       backupCount=5)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    else:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)


def main():
    (options, cfg_path) = parser.parse_args()
    if not cfg_path:
        sys.stderr.write('Configuration file required!\n\n')
        parser.print_usage()
        sys.exit(-1)
    defaults = {'fmod': '0600',
                'compression': 'gzip',
                'dest': os.getcwd(),
                'log_level': 'INFO'}
    cfg = ConfigParser.RawConfigParser(defaults)
    cfg.read(cfg_path)
    setup_logging(cfg)
    shared = {
        'dest': cfg.get('default', 'dest'),
        'compression': cfg.get('default', 'compression'),
        'fmod': string.atoi(cfg.get('default', 'fmod'), 8),
    }
    engines = []
    hosts = [s for s in cfg.sections() if s.startswith('host_')]
    if not hosts:
        err = u'No hosts defined. Hosts are of the form host_example.org for a given domain example.org\n\n'
        logger.error(err)
        sys.stderr.write(err)
    for host in hosts:
        data = {'host': host[5:]}
        if cfg.has_option(host, 'user'):
            data['user'] = cfg.get(host, 'user')
        if cfg.has_option(host, 'ssh_port'):
            data['ssh_port'] = cfg.get(host, 'ssh_port')
        if cfg.has_option(host, 'sudo_user'):
            data['sudo_user'] = cfg.get(host, 'sudo_user')
        data.update(shared)
        if cfg.has_option(host, 'postgres'):
            pg_data = {}
            databases = cfg.get(host, 'postgres').strip()
            if databases not in ('ALL', ''):
                pg_data['databases'] = databases.split(',')
            if cfg.has_option(host, 'postgres_sudo_user'):
                pg_data['sudo_user'] = cfg.get(host, 'postgres_sudo_user')
            pg_data.update(data)
            engines.append(dbbu.PostgreSQL(**pg_data))
        if cfg.has_option(host, 'mysql'):
            engines.append(dbbu.MySQL(**data))
    for engine in engines:
        engine.run()


if __name__ == "__main__":
    main()
    sys.exit(0)
