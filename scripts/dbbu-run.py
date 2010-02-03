#!/usr/bin/python

import os
import sys
import time
import string
import logging
import subprocess
import ConfigParser
from optparse import OptionParser
import dbbu


usage = "usage: %prog [options] cfg"
parser = OptionParser(usage=usage)
parser.add_option("-c", "--compression", default="gzip",
                  help="compression algorithm (gzip, bzip2, etc.)")
parser.add_option("-d", "--dest", default=os.getcwd(),
                  help="destination directory, defaults to cwd")


def main():
    (options, cfg_path) = parser.parse_args()
    defaults = {'fmod': '0600'}
    cfg = ConfigParser.RawConfigParser(defaults)
    cfg.read(cfg_path)
    shared = {
        'dest': options.dest,
        'compression': options.compression,
        'fmod': string.atoi(cfg.get('default', 'fmod'), 8),
    }
    engines = []
    for host in cfg.sections():
        data = {'host': host}
        if cfg.has_option(host, 'user'):
            data['host'] = '%s@%s' % (cfg.get(host, 'user'), host)
        data.update(shared)
        if cfg.has_option(host, 'postgres'):
            databases = cfg.get(host, 'postgres')
            if databases != 'ALL':
                data['databases'] = databases.split(',')
            engines.append(dbbu.PostgreSQL(**data))
        if cfg.has_option(host, 'mysql'):
            engines.append(dbbu.MySQL(**data))
    for engine in engines:
        engine.run()


if __name__ == "__main__":
    main()
    sys.exit(0)
