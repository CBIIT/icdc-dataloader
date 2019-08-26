#!/usr/bin/env python3
import os, sys
import glob
import argparse
import re
from neo4j import GraphDatabase, ServiceUnavailable
from icdc_schema import ICDC_Schema
from utils import *
from data_loader import DataLoader
from s3 import *

PSWD_ENV = 'NEO_PASSWORD'


def removeTrailingSlash(uri):
    if uri.endswith('/'):
        return re.sub('/+$', '', uri)
    else:
        return uri

# Data loader will try to load all TSV(.TXT) files from given directory into Neo4j
# optional arguments includes:
# -i or --uri followed by Neo4j server address and port in format like bolt://12.34.56.78:7687
def main():
    parser = argparse.ArgumentParser(description='Load TSV(TXT) files (from Pentaho) into Neo4j')
    parser.add_argument('-i', '--uri', help='Neo4j uri like bolt://12.34.56.78:7687')
    parser.add_argument('-u', '--user', help='Neo4j user')
    parser.add_argument('-p', '--password', help='Neo4j password')
    parser.add_argument('-s', '--schema', help='Schema files', action='append')
    parser.add_argument('-c', '--cheat-mode', help='Skip validations, aka. Cheat Mode', action='store_true')
    parser.add_argument('-b', '--bucket', help='S3 bucket name')
    parser.add_argument('-f', '--s3-folder', help='S3 folder')
    parser.add_argument('dir', help='Data directory')

    args = parser.parse_args()

    log = get_logger('Data Loader')
    dir = args.dir
    if args.s3_folder and not os.path.exists(args.dir):
        os.makedirs(dir)

    if args.s3_folder:
        if not args.bucket:
            log.error('Please specify S3 bucket name with -b/--bucket argument!')
            sys.exit(1)
        bucket = S3Bucket(args.bucket)
        if not os.path.isdir(dir):
            log.error('{} is not a directory!'.format(dir))
            sys.exit(1)
        if not bucket.download_files_in_folder(args.s3_folder, dir):
            log.error('Download files from S3 bucket "{}" failed!'.format(args.bucket))
            sys.exit(1)

    if not os.path.isdir(args.dir):
        log.error('{} is not a directory!'.format(args.dir))
        sys.exit(1)

    uri = args.uri if args.uri else "bolt://localhost:7687"
    uri = removeTrailingSlash(uri)

    password = args.password
    if not password:
        if PSWD_ENV not in os.environ:
            log.error('Password not specified! Please specify password with -p or --password argument, or set {} env var'.format(PSWD_ENV))
            sys.exit(1)
        else:
            password = os.environ[PSWD_ENV]
    user = args.user if args.user else 'neo4j'

    if not args.schema:
        log.error('Please specify schema file(s) with -s or --schema argument')
        sys.exit(1)

    for schema_file in args.schema:
        if not os.path.isfile(schema_file):
            log.error('{} is not a file'.format(schema_file))
            sys.exit(1)

    try:
        file_list = glob.glob('{}/*.txt'.format(args.dir))
        if file_list:
            schema = ICDC_Schema(args.schema)
            driver = GraphDatabase.driver(uri, auth=(user, password))
            loader = DataLoader(log, driver, schema, file_list)
            loader.load(args.cheat_mode)

            driver.close()
        else:
            log.info('No files to load.')

    except ServiceUnavailable as err:
        log.exception(err)
        log.critical("Can't connect to Neo4j server at: \"{}\"".format(uri))

if __name__ == '__main__':
    main()
