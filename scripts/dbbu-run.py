#!/usr/bin/python

import os
import sys
import time
import string
import ConfigParser
from optparse import OptionParser
import dbbu


usage = """usage: %prog <path-to-configuration-file>

Sample Configuration:

[default]
# compress backup files (defaults to gzip)
compression: bzip2
# backup destination directory (defaults to current working directory)
dest: /backup/
# chmod backup files, must start with 0 (defaults to 0600)
fmod: 0755

# host 1
[example.com]
# ssh user
user: postgres
# database specifications
postgres: ALL
mysql: ALL

# host n
[myserver.com]"""
parser = OptionParser(usage=usage)

def main():
    (options, cfg_path) = parser.parse_args()
    if not cfg_path:
        sys.stderr.write('Configuration file required!\n\n')
        parser.print_usage()
        sys.exit(-1)
    defaults = {'fmod': '0600', 'compression': 'gzip', 'dest': os.getcwd()}
    cfg = ConfigParser.RawConfigParser(defaults)
    cfg.read(cfg_path)
    shared = {
        'dest': cfg.get('default', 'dest'),
        'compression': cfg.get('default', 'compression'),
        'fmod': string.atoi(cfg.get('default', 'fmod'), 8),
    }
    engines = []
    for host in cfg.sections():
        data = {'host': host}
        if cfg.has_option(host, 'user'):
            data['host'] = '%s@%s' % (cfg.get(host, 'user'), host)
        data.update(shared)
        if cfg.has_option(host, 'postgres'):
            databases = cfg.get(host, 'postgres').strip()
            if databases not in ('ALL', ''):
                data['databases'] = databases.split(',')
            engines.append(dbbu.PostgreSQL(**data))
        if cfg.has_option(host, 'mysql'):
            engines.append(dbbu.MySQL(**data))
    for engine in engines:
        engine.run()


if __name__ == "__main__":
    main()
    sys.exit(0)
