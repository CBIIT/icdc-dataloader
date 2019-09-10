#!/usr/bin/env python3

import argparse
from s3 import *
from os.path import isfile, join
import time
import hashlib
from icdc_schema import *
from data_loader import *
from utils import *
from loader import *
import subprocess
import errno

# MANIFEST FIELDS Based on data model uc-cdis manifest setting
# https://github.com/uc-cdis/indexd_utils/blob/master/manifest.tsv
# https://github.com/CBIIT/icdc-model-tool/blob/master/model-desc/icdc-model.yml and
MANIFEST_FIELDS = ["uuid", "file_size", "file_md5", "file_status", "file_locations", "file_format", "acl"]
log = get_logger('S3 Loader')
PSWD_ENV = 'NEO_PASSWORD'
BLOCKSIZE = 65536
Upload_Fails_Budget = 0


# Upload files to S3 on the files included in the manifest
def upload_files_based_on_manifest(bucket, s3_folder, directory, bucket_name, manifest):
    start = timer()
    log.info('Uploading Files to S3 {} .'.format(s3_folder + "/" + bucket_name))
    upload_fails_count = 0
    number_of_files_uploaded = 0
    number_of_files_need_to_upload = 0
    bits_uploaded = 0
    with open(manifest) as csv_file:
        tsv_reader = csv.DictReader(csv_file, delimiter='\t')
        for record in tsv_reader:
            file_name = record["file_name"]
            if upload_file(bucket, join(s3_folder, file_name), join(directory, file_name)):
                number_of_files_uploaded += 1
                number_of_files_need_to_upload += 1
                bits_uploaded += os.stat(join(directory, file_name)).st_size
                log.info('File :  {}  uploaded !'.format(file_name))

            else:
                log.info('==========> File :  {} upload fails !'.format(file_name))
                upload_fails_count += 1
                if upload_fails_count > Upload_Fails_Budget:
                    log.error(
                        '==========> Too many uploading failures, {} files upload fails !'.format(upload_fails_count))
                    return False
    end = timer()
    log.info('  {} File(s) uploaded, {} File(s) upload fails , Total {} file(s) need to be uploaded '.format(
        number_of_files_uploaded, upload_fails_count, number_of_files_need_to_upload))
    log.info('  {:.2f} MB uploaded '.format(bits_uploaded / (1024 * 1024)))
    log.info('  Uploading time: {:.2f} seconds'.format(end - start))  # Time in seconds, e.g. 5.38091952400282

    return True


# Upload file to S3
def upload_file(bucket, s3, f):
    try:
        log.info('Uploading {}'.format(f))
        bucket.upload_file(s3, f)
        return True
    except Exception as e:
        log.error('==========> Upload file to S3 fails!  Error {}'.format(e))
        return False


# Completing the initial manifest and then upload to S3
def export_result(manifest, bucket, bucket_name, folder_name, directory, input_s3_bucket, input_s3_folder):
    log.info('Completing the initial manifest .')
    try:
        data_matrix = []
        fieldnames = []
        with open(manifest) as csv_file:
            tsv_reader = csv.DictReader(csv_file, delimiter='\t')
            data_matrix = []
            for record in tsv_reader:
                del record["case_id"]
                del record["clinical_study_designation"]
                del record["patient_id"]
                if not fieldnames:
                    for key in record:
                        if key == "case_id" or key == "clinical_study_designation" or key == "patient_id":
                            continue
                        fieldnames.append(key)
                    fieldnames += MANIFEST_FIELDS
                f = join(directory, record["file_name"])
                record["file_size"] = os.stat(f).st_size
                record["file_locations"] = join("s3://", input_s3_bucket, input_s3_folder, record["file_name"])
                record["file_format"] = (os.path.splitext(f)[1]).split('.')[1].lower()
                record["uuid"] = get_uuid_for_node("file",record["file_locations"])
                hasher = hashlib.sha256()
                with open(f, 'rb') as afile:
                    buf = afile.read(BLOCKSIZE)
                    while len(buf) > 0:
                        hasher.update(buf)
                        buf = afile.read(BLOCKSIZE)
                record["file_md5"] = hasher.hexdigest()
                record["file_status"] = "uploaded"
                record["acl"] = "open"
                data_matrix.append(record)

        timestr = time.strftime("%Y%m%d-%H%M%S")
        output_file_name = timestr + ".txt"
        output_file = join(directory, folder_name, output_file_name)

        if not os.path.exists(join(directory, folder_name)):
            try:
                os.makedirs(join(directory, folder_name))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    print (exc)
                    return False

        write_tsv_file(output_file, data_matrix, fieldnames)
        log.info('Upload the manifest info into S3 .')
        if upload_file(bucket, join(folder_name, output_file_name), output_file):
            return True
        else:
            return False
    except Exception as e:
        print (e)
        return False


# Write data into tsv file
def write_tsv_file(f, data, fieldnames):
    try:
        with open(f, 'wt') as out_file:
            tsv_writer = csv.DictWriter(out_file, delimiter='\t', fieldnames=fieldnames)
            tsv_writer.writeheader()
            for index in range(len(data)):
                tsv_writer.writerow(data[index])
    except IOError as e:
        print (e)


# check_manifest exists or not
# check the field file_name/case id in the manifest which should not be null/empty
# check files included in the manifest exists or not
def check_manifest(manifest, dir):
    log.info('Validating manifest.')
    pass_check = True
    # check manifest
    if not os.path.isfile(manifest):
        log.error('==========>  Manifest: "{}" does not exists !'.format(args.manifest))
        pass_check = False
    else:
        log.info('Reading manifest .')
        log.info('validating fields in  manifest .')
        # check fields in the manifest, if missing fields stops
        with open(manifest) as csv_file:
            tsv_reader = csv.DictReader(csv_file, delimiter='\t')
            line_count = 1;
            for record in tsv_reader:
                line_count += 1
                if "file_name" in record and record["file_name"] != "":
                    if not isfile(os.path.join(dir, record["file_name"])):
                        log.error('==========> Invalid data at line {} : File "{}" doesn\'t exist!'.format(line_count,
                                                                                                           record[
                                                                                                               "file_name"]))
                        pass_check = False
                else:
                    log.error('==========> Invalid data at line {} : Empty file name in line!'.format(line_count))
                    pass_check = False
                if "case_id" not in record or record["case_id"] == "":
                    log.error('==========> Invalid data at line {} : Empty case id name!'.format(line_count))
                    pass_check = False
    return pass_check


# check file directory exist
def check_file_exist(dir):
    log.info('Checking the files included in manifest exists in given location .')
    pass_check = True

    if not os.path.exists(dir):
        log.error('==========> Folder: "{}" does not exists!'.format(dir))
        pass_check = False

    if not os.path.isdir(dir):
        log.error('==========> {} is not a directory!'.format(dir))
        pass_check = False

    return pass_check


# check file node's parent
def check_file_parent(manifest, loader):
    log.info('Validating the parent record of each file.')
    # call data loader function to validate the data
    return loader.validate_cases_exist_in_file(manifest, 100)


# validate input value
#  1. Check manifest
#  2. Check upload file directory
#  3. Check  file node's parent
def validate_input(args, loader):
    pass_check = []
    index = 0
    pass_check.append(check_file_exist(args.dir))
    if pass_check[index]:
        log.info('Pass checking file exists')
    index += 1
    pass_check.append(check_manifest(args.manifest, args.dir))
    if pass_check[index]:
        log.info('Pass validating manifest .')
    index += 1
    pass_check.append(check_file_parent(args.manifest, loader))
    if pass_check[index]:
        log.info('Pass validating parents .')

    if False in pass_check:
        return False
    else:
        return True


def call_data_loader(python, neo4j_password, schemas, dir):
    schema_cmd = []
    for schema in schemas:
        schema_cmd.append("-s")
        schema_cmd.append(schema)
    cmd = [python, "loader.py", "-c", "-p", neo4j_password] + schema_cmd
    cmd += [dir]
    process_status = subprocess.run(cmd, check=True)
    if process_status.returncode == 0:
        log.info('Finish load file into neo4j database.')
        return True
    else:
        log.error(process_status.stderr)
        return False


# File loader will try to upload all files from given directory into S3

def main():
    parser = argparse.ArgumentParser(description='Upload files from local to S3')
    parser.add_argument('-t', '--manifest', help='input manifest')
    parser.add_argument('-d', '--dir', help='upload files\'s location')
    parser.add_argument('-isb', '--input-s3-bucket', help='S3 bucket name for files')
    parser.add_argument('-isf', '--input-s3-folder', help='S3 folder for files')
    parser.add_argument('-osb', '--output-s3-bucket', help='s3 bucket for manifest')
    parser.add_argument('-osf', '--output-s3-folder', help='s3 folder for manifest')
    parser.add_argument('-s', '--schema', help='Schema files', action='append')
    parser.add_argument('-f', '--max-violations', help='Max violations to display', nargs='?', type=int, default=0)
    parser.add_argument('-md5', '--max-block-for-md5', help='Max Blocks for MD5 ', nargs='?', type=int, default=65536)
    parser.add_argument('-i', '--uri', help='Neo4j uri like bolt://12.34.56.78:7687')
    parser.add_argument('-u', '--user', help='Neo4j user')
    parser.add_argument('-p', '--password', help='Neo4j password')
    parser.add_argument('-python', '--python', help='python version', default="python")
    parser.add_argument('-manual', '--manual_load_data_into_db', help='True for Manual load data into db',
                        default="False")

    args = parser.parse_args()

    # check args
    print (args.manual_load_data_into_db)
    flag_are_args_completed = True
    if (not args.manifest):
        log.error('the following arguments are required: -t')
        flag_are_args_completed = False

    if (not args.dir):
        log.error('the following arguments are required: -d')
        flag_are_args_completed = False

    if (not args.input_s3_bucket):
        log.error('the following arguments are required: -isb')
        flag_are_args_completed = False

    if (not args.input_s3_folder):
        log.error('the following arguments are required: -isf')
        flag_are_args_completed = False

    if (not args.output_s3_bucket):
        log.error('the following arguments are required: -osb')
        flag_are_args_completed = False

    if (not args.output_s3_folder):
        log.error('the following arguments are required: -osf')
        flag_are_args_completed = False

    NEO4J_URI = args.uri if args.uri else "bolt://localhost:7687"
    NEO4J_URI = removeTrailingSlash(NEO4J_URI)

    NEO4J_PASSWORD = args.password
    if not NEO4J_PASSWORD:
        if PSWD_ENV not in os.environ:
            log.error(
                'Password not specified! Please specify password with -p or --password argument, or set {} env var'.format(
                    PSWD_ENV))
            flag_are_args_completed = False
        else:
            NEO4J_PASSWORD = os.environ[PSWD_ENV]

    NEO4J_USER = args.user if args.user else 'neo4j'

    if not args.schema:
        log.error('Please specify schema file(s) with -s or --schema argument')
        flag_are_args_completed = False

    for schema_file in args.schema:
        if not os.path.isfile(schema_file):
            log.error('{} is not a file'.format(schema_file))
            flag_are_args_completed = False

    BLOCKSIZE = args.max_block_for_md5 if args.max_block_for_md5 else 65536

    Upload_Fails_Budget = args.max_violations if args.max_violations else 0

    if (not flag_are_args_completed):
        sys.exit(1)

    try:
        SCHEMA = ICDC_Schema(args.schema)
        NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        Data_Loader = DataLoader(NEO4J_DRIVER, SCHEMA, [args.manifest])
        input_bucket = S3Bucket(args.input_s3_bucket)
        output_bucket = S3Bucket(args.output_s3_bucket)
        start = timer()
        if not validate_input(args, Data_Loader):
            log.error('Validate input fails')
            NEO4J_DRIVER.close()
            sys.exit(1)
        NEO4J_DRIVER.close()

        if not upload_files_based_on_manifest(input_bucket, args.input_s3_folder, args.dir, args.input_s3_bucket,
                                              args.manifest):
            log.error('Upload file(s) to S3 bucket "{}" failed!'.format(args.input_s3_bucket))
            sys.exit(1)

        if not export_result(args.manifest, output_bucket, args.output_s3_bucket, args.output_s3_folder, args.dir,
                             args.input_s3_bucket, args.input_s3_folder):
            log.error('Upload file(s) to S3 bucket "{}" failed!'.format(args.output_s3_bucket))
            sys.exit(1)

        # call data loader
        if args.manual_load_data_into_db.lower() == "false":
            if not call_data_loader(args.python, NEO4J_PASSWORD, args.schema, join(args.dir, args.output_s3_folder)):
                log.error('Load file(s) into database failed!')
                sys.exit(1)
        else:
            log.info('Choose to manually load file(s) into database')

        end = timer()
        log.info('Cheers, Job Finished !! Total Execution Time: {:.2f} seconds'.format(end - start))

    except ServiceUnavailable as err:
        log.exception(err)
        log.critical("Can't connect to Neo4j server at: \"{}\"".format(NEO4J_URI))


if __name__ == '__main__':
    main()
