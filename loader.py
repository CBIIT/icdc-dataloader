#!/usr/bin/env python3
import argparse
import datetime
import glob
import os, sys
import subprocess

from neo4j import GraphDatabase, ServiceUnavailable

from common.icdc_schema import ICDC_Schema
from common.utils import get_logger, removeTrailingSlash, check_schema_files, DATETIME_FORMAT, get_host, \
     UPSERT_MODE, NEW_MODE, DELETE_MODE
from common.config import BACKUP_FOLDER, PSWD_ENV
from common.data_loader import DataLoader
from common.s3 import S3Bucket

def parse_arguments():
    parser = argparse.ArgumentParser(description='Load TSV(TXT) files (from Pentaho) into Neo4j')
    parser.add_argument('-i', '--uri', help='Neo4j uri like bolt://12.34.56.78:7687')
    parser.add_argument('-u', '--user', help='Neo4j user')
    parser.add_argument('-p', '--password', help='Neo4j password')
    parser.add_argument('-s', '--schema', help='Schema files', action='append', required=True)
    parser.add_argument('-c', '--cheat-mode', help='Skip validations, aka. Cheat Mode', action='store_true')
    parser.add_argument('-d', '--dry-run', help='Validations only, skip loading', action='store_true')
    parser.add_argument('--wipe-db', help='Wipe out database before loading, you\'ll lose all data!',
                        action='store_true')
    parser.add_argument('--no-backup', help='Skip backup step', action='store_true')
    parser.add_argument('-y', '--yes', help='Automatically confirm deletion and database wiping', action='store_true')
    parser.add_argument('-M', '--max-violations', help='Max violations to display', nargs='?', type=int, default=10)
    parser.add_argument('-b', '--bucket', help='S3 bucket name')
    parser.add_argument('-f', '--s3-folder', help='S3 folder')
    parser.add_argument('-m', '--mode', help='Loading mode', choices=[UPSERT_MODE, NEW_MODE, DELETE_MODE],
                        default=UPSERT_MODE)
    parser.add_argument('dir', help='Data directory')

    return parser.parse_args()


def process_arguments(args, log):
    directory = args.dir
    if args.s3_folder:
        if not os.path.exists(directory):
            os.makedirs(directory)
        else:
            exist_files = glob.glob('{}/*'.format(directory))
            if len(exist_files) > 0:
                log.error('Folder: "{}" is not empty, please empty it first'.format(directory))
                sys.exit(1)

    if args.s3_folder:
        if not args.bucket:
            log.error('Please specify S3 bucket name with -b/--bucket argument!')
            sys.exit(1)
        bucket = S3Bucket(args.bucket)
        if not os.path.isdir(directory):
            log.error('{} is not a directory!'.format(directory))
            sys.exit(1)
        if not bucket.download_files_in_folder(args.s3_folder, directory):
            log.error('Download files from S3 bucket "{}" failed!'.format(args.bucket))
            sys.exit(1)

    if not os.path.isdir(directory):
        log.error('{} is not a directory!'.format(directory))
        sys.exit(1)

    uri = args.uri if args.uri else "bolt://localhost:7687"
    uri = removeTrailingSlash(uri)

    password = args.password
    if not password:
        if PSWD_ENV not in os.environ:
            log.error('Password not specified! Please specify password with -p or --password argument,' +
                      ' or set {} env var'.format(PSWD_ENV))
            sys.exit(1)
        else:
            password = os.environ[PSWD_ENV]
    user = args.user if args.user else 'neo4j'
    return (user, password, directory, uri)


def backup_neo4j(backup_dir, name, address, log):
    try:
        restore_cmd = 'To restore DB from backup (to remove any changes caused by current data loading, run following commands:\n'
        restore_cmd += '#' * 160 + '\n'
        neo4j_cmd = 'neo4j-admin restore --from={}/{} --force'.format(BACKUP_FOLDER, name)
        cmds = [
            [
                'mkdir',
                '-p',
                backup_dir
            ],
            [
                'neo4j-admin',
                'backup',
                '--backup-dir={}'.format(backup_dir),
                '--name={}'.format(name),
            ]
        ]
        if address in ['localhost', '127.0.0.1']:
            restore_cmd += '\t$ neo4j stop && {} && neo4j start\n'.format(neo4j_cmd)
            for cmd in cmds:
                log.info(cmd)
                subprocess.call(cmd)
        else:
            second_cmd = 'sudo systemctl stop neo4j && {} && sudo systemctl start neo4j && exit'.format(neo4j_cmd)
            restore_cmd += '\t$ echo "{}" | ssh -t {} sudo su - neo4j\n'.format(second_cmd, address)
            for cmd in cmds:
                remote_cmd = ['ssh', address] + cmd
                log.info(' '.join(remote_cmd))
                subprocess.call(remote_cmd)
        restore_cmd += '#' * 160
        return restore_cmd
    except Exception as e:
        log.exception(e)
        return False

# Data loader will try to load all TSV(.TXT) files from given directory into Neo4j
# optional arguments includes:
# -i or --uri followed by Neo4j server address and port in format like bolt://12.34.56.78:7687
def main():
    log = get_logger('Loader')
    args = parse_arguments()
    user, password, directory, uri = process_arguments(args, log)


    if not check_schema_files(args.schema, log):
        sys.exit(1)

    try:
        file_list = glob.glob('{}/*.txt'.format(directory))
        if file_list:
            if args.wipe_db and not args.yes:
                if not confirm_deletion('Wipe out entire Neo4j database before loading?'):
                    sys.exit()

            if args.mode == DELETE_MODE and not args.yes:
                if not confirm_deletion('Delete all nodes and child nodes from data file?'):
                    sys.exit()
            backup_name = datetime.datetime.today().strftime(DATETIME_FORMAT)
            host = get_host(uri)
            restore_cmd = ''
            if not args.no_backup and not args.dry_run:
                restore_cmd = backup_neo4j(BACKUP_FOLDER, backup_name, host, log)
                if not restore_cmd:
                    log.error('Backup Neo4j failed, abort loading!')
                    sys.exit(1)
            schema = ICDC_Schema(args.schema)
            driver = None
            if not args.dry_run:
                driver = GraphDatabase.driver(uri, auth=(user, password))
            loader = DataLoader(driver, schema)

            loader.load(file_list, args.cheat_mode, args.dry_run, args.mode, args.wipe_db, args.max_violations)

            if driver:
                driver.close()
            if restore_cmd:
                log.info(restore_cmd)
        else:
            log.info('No files to load.')

    except ServiceUnavailable as err:
        log.exception(err)
        log.critical("Can't connect to Neo4j server at: \"{}\"".format(uri))

def confirm_deletion(message):
    print(message)
    confirm = input('Type "yes" and press enter to proceed (You\'ll LOSE DATA!!!), press enter to cancel:')
    confirm = confirm.strip().lower()
    return confirm == 'yes'

if __name__ == '__main__':
    main()
