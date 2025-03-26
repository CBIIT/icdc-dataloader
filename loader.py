#!/usr/bin/env python3
import argparse
import glob
import os
import sys
import zipfile

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from neo4j.exceptions import AuthError

from icdc_schema import ICDC_Schema
from props import Props
from bento.common.utils import get_logger, removeTrailingSlash, check_schema_files, UPSERT_MODE, NEW_MODE, DELETE_MODE, \
    get_log_file, LOG_PREFIX, APP_NAME, load_plugin, print_config
from create_index import NEO4J, MEMGRAPH

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Data_Loader'

os.environ[APP_NAME] = 'Data_Loader'

from config import BentoConfig
from data_loader import DataLoader
from bento.common.s3 import S3Bucket, upload_log_file

DEFAULT_MAX_VIOLATIONS = 1000000
DEFAULT_TEMP_FOLDER = "tmp"

def parse_arguments(args = None):
    parser = argparse.ArgumentParser(description='Load TSV(TXT) files (from Pentaho) into Neo4j')
    parser.add_argument('-i', '--uri', help='Neo4j uri like bolt://12.34.56.78:7687')
    parser.add_argument('-u', '--user', help='Neo4j user')
    parser.add_argument('-p', '--password', help='Neo4j password')
    parser.add_argument('-s', '--schema', help='Schema files', action='append')
    parser.add_argument('--prop-file', help='Property file, example is in config/props.example.yml')
    parser.add_argument('--backup-folder', help='Location to store database backup')
    parser.add_argument('config_file', help='Configuration file, example is in config/data-loader-config.example.yml',
                        nargs='?', default=None)
    parser.add_argument('-c', '--cheat-mode', help='Skip validations, aka. Cheat Mode', action='store_true')
    parser.add_argument('-d', '--dry-run', help='Validations only, skip loading', action='store_true')
    parser.add_argument('--wipe-db', help='Wipe out database before loading, you\'ll lose all data!',
                        action='store_true')
    parser.add_argument('--no-backup', help='Skip backup step', action='store_true')
    parser.add_argument('-v', '--verbose', help='Print the whole list of permissive values when the value is non-permissive value', action='store_true')
    parser.add_argument('-y', '--yes', help='Automatically confirm deletion and database wiping', action='store_true')
    parser.add_argument('-M', '--max-violations', help='Max violations to display', nargs='?', type=int)
    parser.add_argument('-b', '--bucket', help='S3 bucket name')
    parser.add_argument('-f', '--s3-folder', help='S3 folder')
    parser.add_argument('-m', '--mode', help='Loading mode', choices=[UPSERT_MODE, NEW_MODE, DELETE_MODE])
    parser.add_argument('--dataset', help='Dataset directory')
    parser.add_argument('--split-transactions', help='Creates a separate transaction for each file',
                        action='store_true')
    parser.add_argument('--upload-log-dir', help='Upload destination dir for log file,  if dir in s3, use the format, s3://[bucket]/[prefix]')
    parser.add_argument('--database-type', help='The database type, can be either neo4j or memgraph', choices=[NEO4J, MEMGRAPH])
    return parser.parse_args(args)


def process_arguments(args, log):
    config_file = None
    if args.config_file:
        config_file = args.config_file
    config = BentoConfig(config_file)

    # Required Fields
    if args.dataset:
        config.dataset = args.dataset
    if not config.dataset:
        log.error('No dataset specified! Please specify a dataset in config file or with CLI argument --dataset')
        sys.exit(1)

    if args.s3_folder:
        config.s3_folder = args.s3_folder
    if not config.s3_folder and not os.path.isdir(config.dataset):
        log.error('{} is not a directory!'.format(config.dataset))
        sys.exit(1)

    if args.prop_file:
        config.prop_file = args.prop_file
    if not config.prop_file:
        log.error('No properties file specified! ' +
                  'Please specify a properties file in config file or with CLI argument --prop-file')
        sys.exit(1)

    if args.schema:
        config.schema_files = args.schema
    if not config.schema_files:
        log.error('No schema file specified! ' +
                  'Please specify at least one schema file in config file or with CLI argument --schema')
        sys.exit(1)

    if config.PSWD_ENV in os.environ and not config.neo4j_password:
        config.neo4j_password = os.environ[config.PSWD_ENV]
    if args.password:
        config.neo4j_password = args.password
    if not config.neo4j_password:
        log.error('Password not specified! Please specify password with -p or --password argument,' +
                  ' or set {} env var'.format(config.PSWD_ENV))
        sys.exit(1)

    # Conditionally Required Fields
    if args.split_transactions:
        config.split_transactions = args.split_transactions
    if args.no_backup:
        config.no_backup = args.no_backup
    if args.backup_folder:
        config.backup_folder = args.backup_folder
    #if config.split_transactions and config.no_backup:
    #    log.error('--split-transaction and --no-backup cannot both be enabled, a backup is required when running'
    #              ' in split transactions mode')
    #    sys.exit(1)
    if not config.backup_folder and not config.no_backup:
        log.error('Backup folder not specified! A backup folder is required unless the --no-backup argument is used')
        sys.exit(1)

    if config.s3_folder:
        if not os.path.exists(config.dataset):
            os.makedirs(config.dataset)
        else:
            exist_files = glob.glob('{}/*.txt'.format(config.dataset))
            if len(exist_files) > 0:
                log.error('Folder: "{}" is not empty, please empty it first'.format(config.dataset))
                sys.exit(1)

        if args.bucket:
            config.s3_bucket = args.bucket
        if not config.s3_bucket:
            log.error('Please specify S3 bucket name with -b/--bucket argument!')
            sys.exit(1)
        bucket = S3Bucket(config.s3_bucket)
        if not os.path.isdir(config.dataset):
            log.error('{} is not a directory!'.format(config.dataset))
            sys.exit(1)
        log.info(f'Loading data from s3://{config.s3_bucket}/{config.s3_folder}')
        if not bucket.download_files_in_folder(config.s3_folder, config.dataset):
            log.error('Download files from S3 bucket "{}" failed!'.format(config.s3_bucket))
            sys.exit(1)

    # Optional Fields
    if args.uri:
        config.neo4j_uri = args.uri
    if not config.neo4j_uri:
        config.neo4j_uri = 'bolt://localhost:7687'
    config.neo4j_uri = removeTrailingSlash(config.neo4j_uri)
    log.info(f"Loading into Neo4j at: {config.neo4j_uri}")

    if args.user:
        config.neo4j_user = args.user
    if not config.neo4j_user:
        config.neo4j_user = 'neo4j'

    if args.wipe_db:
        config.wipe_db = args.wipe_db

    if args.yes:
        config.yes = args.yes
    if args.verbose:
        config.verbose = args.verbose
    if args.dry_run:
        config.dry_run = args.dry_run
    if args.cheat_mode:
        config.cheat_mode = args.cheat_mode

    if args.mode:
        config.loading_mode = args.mode
    if not config.loading_mode:
        config.loading_mode = UPSERT_MODE

    if args.max_violations:
        config.max_violations = int(args.max_violations)
    if not config.max_violations:
        config.max_violations = DEFAULT_MAX_VIOLATIONS

    if args.upload_log_dir:
        config.upload_log_dir = args.upload_log_dir
    
    if not config.database_type:
        config.database_type = NEO4J
    allowed_database_type = [NEO4J, MEMGRAPH]
    if config.database_type not in allowed_database_type:
            log.error('database_type is neither neo4j nor memgraph, abort loading')
            sys.exit(1)

    if args.database_type:
        config.database_type = args.database_type
    # Only applies when running in Prefect via loader_prefect.py, which doesn't have config files and temp_foldetemp_folderr
    # So plugins have to be passed in from Prefect parameters
    # In that case args is an object that contains all Prefect parameters
    if hasattr(args, 'plugins'):
        config.plugins = args.plugins

    if hasattr(args, 'temp_folder'):
        config.temp_folder = args.temp_folder

    if not config.temp_folder:
        config.temp_folder = DEFAULT_TEMP_FOLDER

    return config

def prepare_plugin(config, schema):
    if not config.params:
        config.params = {}
    config.params['schema'] = schema
    return load_plugin(config.module_name, config.class_name, config.params)


# Data loader will try to load all TSV(.TXT) files from given directory into Neo4j
# optional arguments includes:
# -i or --uri followed by Neo4j server address and port in format like bolt://12.34.56.78:7687
def main(args):
    log = get_logger('Loader')
    log_file = get_log_file()
    config = process_arguments(args, log)
    print_config(log, config)

    if not check_schema_files(config.schema_files, log):
        return

    driver = None
    mg_connection = None
    restore_cmd = ''
    load_result = None
    try:
        txt_files = glob.glob('{}/*.txt'.format(config.dataset))
        tsv_files = glob.glob('{}/*.tsv'.format(config.dataset))
        file_list = txt_files + tsv_files
        if file_list:
            if config.wipe_db and not config.yes:
                if not confirm_deletion('Wipe out entire Neo4j database before loading?'):
                    sys.exit(1)

            if config.loading_mode == DELETE_MODE and not config.yes:
                if not confirm_deletion('Delete all nodes and child nodes from data file?'):
                    sys.exit(1)

            prop_path = os.path.join(config.dataset, config.prop_file)
            if os.path.isfile(prop_path):
                props = Props(prop_path)
            else:
                props = Props(config.prop_file)
            schema = ICDC_Schema(config.schema_files, props)
            if not config.dry_run or config.loading_mode == DELETE_MODE:
                driver = GraphDatabase.driver(
                    config.neo4j_uri,
                    auth=(config.neo4j_user, config.neo4j_password),
                    encrypted=False
                )

            plugins = []
            memgraph_snapshot_dir = None
            if len(config.plugins) > 0:
                for plugin_config in config.plugins:
                    plugins.append(prepare_plugin(plugin_config, schema))
            if config.memgraph_snapshot_dir:
                memgraph_snapshot_dir = config.memgraph_snapshot_dir
            loader = DataLoader(driver, schema, config, memgraph_snapshot_dir, plugins)

            load_result = loader.load(file_list, config.cheat_mode, config.dry_run, config.loading_mode, config.wipe_db,
                        config.max_violations, config.temp_folder, config.verbose, split=config.split_transactions,
                        no_backup=config.no_backup, neo4j_uri=config.neo4j_uri, backup_folder=config.backup_folder, username=config.neo4j_user, password=config.neo4j_password)
            
            if load_result == False:
                if loader.validation_result_file_key != "":
                    zip_file_key = loader.validation_result_file_key.replace(".xlsx", ".zip")
                    with zipfile.ZipFile(zip_file_key, 'w') as zipf:
                        zipf.write(loader.validation_result_file_key, os.path.basename(loader.validation_result_file_key))
                        zipf.write(log_file, os.path.basename(log_file))
                    log.error('Data loading failed, validation result zip file was created at {}'.format(zip_file_key))
                else:
                    log.error('Data loading failed')
            else:
                zip_file_key = log_file.replace(".log", ".zip")
                with zipfile.ZipFile(zip_file_key, 'w') as zipf:
                    zipf.write(log_file, os.path.basename(log_file))
                log.info('Data loading succeeded, zip file was created at {}'.format(zip_file_key))

        else:
            log.info('No files to load.')


    except ServiceUnavailable:
        log.critical("Neo4j service not available at: \"{}\"".format(config.neo4j_uri))
        return
    except AuthError:
        log.error("Wrong Neo4j username or password!")
        return
    except KeyboardInterrupt:
        log.critical("User stopped the loading!")
        return
    finally:
        if driver:
            driver.close()
        if restore_cmd:
            log.info(restore_cmd)

    log_file = get_log_file()
    dest_log_dir = None
    #check if uploaded dir is configured
    if config.upload_log_dir:
        dest_log_dir = config.upload_log_dir
    else:
        #check if s3 bucket/folder are set.
        if config.s3_bucket and config.s3_folder: 
            dest_log_dir = f's3://{config.s3_bucket}/{config.s3_folder}/logs'

    if dest_log_dir:
        try:
            if load_result == False:
                if loader.validation_result_file_key != "":
                    upload_log_file(dest_log_dir, zip_file_key)
                    log.info(f'Uploading validation result zip file {zip_file_key} succeeded!')
            else:
                upload_log_file(dest_log_dir, zip_file_key)
                log.info(f'Uploading validation result zip file {zip_file_key} succeeded!')
            # upload_log_file(dest_log_dir, log_file)
            log.info(f'Uploading log file {log_file} succeeded!')
        except Exception as e:
            log.debug(e)
            log.exception('Copy file failed! Check debug log for detailed information')

    if load_result == False:
        sys.exit(1)

def confirm_deletion(message):
    print(message)
    confirm = input('Type "yes" and press enter to proceed (You\'ll LOSE DATA!!!), press enter to cancel:')
    confirm = confirm.strip().lower()
    return confirm == 'yes'


if __name__ == '__main__':
    main(parse_arguments())
