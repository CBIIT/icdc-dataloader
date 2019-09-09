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

# MANIFEST FIELDS Based on data model uc-cdis manifest setting
# https://github.com/uc-cdis/indexd_utils/blob/master/manifest.tsv
# https://github.com/CBIIT/icdc-model-tool/blob/master/model-desc/icdc-model.yml and
MANIFEST_FIELDS =["uuid","file_size","file_md5","file_status","file_locations","file_format","acl"]
log = get_logger('S3 Loader')
Upload_Fails_Budget = 0
BLOCKSIZE = 65536  #number of bit for hashing

NEO4J_URI = 'bolt://localhost:7687'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = os.environ['NEO_PASSWORD']
NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth = (NEO4J_USER, NEO4J_PASSWORD))
SCHEMA = ICDC_Schema(['data/icdc-model.yml', 'data/icdc-model-props.yml'])
FILE_LIST=[]

def upload_files_based_on_manifest(bucket,s3_folder,directory,bucket_name,manifest):
    start = timer()
    log.info('Upload Files to S3 '.format(s3_folder + "/" + bucket_name))
    upload_fails_count = 0
    number_of_files_uploaded = 0
    number_of_files_need_to_upload=0
    bits_uploaded=0
    with open(manifest) as csv_file:
        tsv_reader = csv.DictReader(csv_file, delimiter='\t')
        for record in tsv_reader:
            file_name =record["file_name"]
            if upload_file(bucket,join(s3_folder, file_name), join(directory, file_name)):
                number_of_files_uploaded += 1
                number_of_files_need_to_upload+=1
                bits_uploaded+=os.stat(join(directory, file_name)).st_size
                log.info('File :  {}  uploaded'.format(file_name))

            else:
                log.info('==========> File :  {} upload fails'.format(file_name))
                upload_fails_count += 1
                if upload_fails_count > Upload_Fails_Budget:
                    log.error('==========> Too many uploading failures, {} files upload fails'.format(upload_fails_count))
                    return False
    end = timer()
    log.info('  {} File(s) uploaded, {} File(s) upload fails , Total {} file(s) need to be uploaded '.format(number_of_files_uploaded, upload_fails_count,number_of_files_need_to_upload))
    log.info('  {:.2f} MB uploaded '.format(bits_uploaded/(1024*1024)))
    log.info('  Uploading time: {:.2f} seconds'.format(end - start))  # Time in seconds, e.g. 5.38091952400282

    return True


def upload_file(bucket,s3,f):

    try:
        log.info('Uploading {}'.format(f))
        bucket.upload_file(s3,f)
        return True
    except Exception as e:
        log.error('==========> Upload file to S3 fails. Error {}'.format(e))
        return False


def export_result(manifest,bucket,bucket_name,folder_name,directory,input_s3_bucket,input_s3_folder):
    log.info('Fill the file info into manifest')
    try:
        data_matrix = []
        fieldnames =[]
        with open(manifest) as csv_file:
            tsv_reader = csv.DictReader(csv_file, delimiter='\t')
            data_matrix =[]
            for record in tsv_reader:
                del record["case_id"]
                if not fieldnames:
                    for key in record:
                        if key =="case_id":
                            continue
                        fieldnames.append(key)
                    fieldnames+=MANIFEST_FIELDS
                f =join(directory,record["file_name"])
                record["file_size"] = os.stat(f).st_size
                record["file_locations"] =join("s3://",input_s3_bucket ,input_s3_folder,record["file_name"])
                record["file_format"]= (os.path.splitext(f)[1]).split('.')[1].lower()
                record["uuid"] =uuid.uuid4()
                hasher = hashlib.sha256()
                with open(f, 'rb') as afile:
                    buf = afile.read(BLOCKSIZE)
                    while len(buf) > 0:
                        hasher.update(buf)
                        buf = afile.read(BLOCKSIZE)
                record["file_md5"]=hasher.hexdigest()
                record["file_status"] = "uploaded"
                record["acl"] = "open"
                data_matrix.append(record)

        timestr = time.strftime("%Y%m%d-%H%M%S")
        output_file_name =timestr + ".txt"
        output_file = join(directory,output_file_name)

        write_tsv_file(output_file,data_matrix,fieldnames)
        log.info('Upload the manifest info into S3')
        if upload_file(bucket,join(folder_name,output_file_name),output_file):
            return True
        else:
            return False
    except Exception as e:
        print (e)
        return False


def write_tsv_file(f,data,fieldnames):
    with open(f, 'wt') as out_file:

        tsv_writer = csv.DictWriter(out_file, delimiter='\t',fieldnames=fieldnames)
        tsv_writer.writeheader()
        for index in range(len(data)):
            tsv_writer.writerow(data[index])


def check_manifest(args):
    log.info('Validate manifest .')
    pass_check = True
     # check manifest
    if not os.path.isfile(args.manifest):
        log.error('==========>  Manifest: "{}" is not found'.format(args.manifest))
        pass_check=False
    else:
        log.info('Reading manifest .')
        log.info('validating fields in  manifest .')
        #check fields in the manifest, if missing fields stops
        with open(args.manifest) as csv_file:
            tsv_reader =  csv.DictReader(csv_file, delimiter='\t')
            row1 = next(tsv_reader)
            if "file_name" not in row1:
                    log.error('==========> Field in manifest: "{}" is not found'.format("file_name"))
                    pass_check= False
    return pass_check



def check_file_exist(args):
    log.info('Checking the files included in manifest exists in given location .')
    pass_check = True

    if not os.path.exists(args.dir):
        log.error('==========> Folder: "{}" is not found'.format(args.dir))
        pass_check=False

    if not os.path.isdir(args.dir):
        log.error('==========> {} is not a directory!'.format(args.dir))
        pass_check=False

    with open(args.manifest) as csv_file:
        tsv_reader =  csv.DictReader(csv_file, delimiter='\t')
        line_count = 1;
        for record in tsv_reader:
            line_count+=1
            if "file_name" in record and record["file_name"]!="":
                if not isfile(os.path.join(args.dir, record["file_name"])):
                    log.error('==========> File: "{}" is not found'.format(record["file_name"]))
                    pass_check = False
            else:
                log.error('==========> Empty file name in line {}'.format(line_count))
                pass_check = False
            if "case_id" not in record or record["case_id"] == "":
                    log.error('==========> Empty case id name in line {}'.format(line_count))
                    pass_check = False


    return pass_check




def check_file_parent(args):
    log.info('Validate that the parent record of each file.')
    # call data loader function to validate the data
    loader = DataLoader(log, NEO4J_DRIVER, SCHEMA, [args.manifest])
    return  loader.validate_cases_exist_in_file(args.manifest, 100)


def validate_input(args):
    pass_check =[]

    pass_check.append(check_manifest(args))
    if pass_check[0]:
        log.info('Pass validating manifest .')

    pass_check.append(check_file_parent(args))
    if pass_check[1]:
        log.info('Pass validating parents .')
    pass_check.append(check_file_exist(args))

    if pass_check[2]:
        log.info('Pass checking file exists')

    if False in pass_check :
        return False
    else:
        return True

# File loader will try to upload all files from given directory into S3

def main():


    parser = argparse.ArgumentParser(description='Upload files from local to S3')
    # parser.add_argument('-t', '--manifest', help='input manifest',action="store_true")
    # parser.add_argument('-d', '--dir', help='upload files\'s location',action="store_true")
    # parser.add_argument('-isb', '--input-s3-bucket', help='S3 bucket name for files',action="store_true")
    # parser.add_argument('-isf', '--input-s3-folder', help='S3 folder for files',action="store_true")
    # parser.add_argument('-osb', '--output-s3-bucket',help='s3 bucket for manifest',action="store_true")
    # parser.add_argument('-osf', '--output-s3-folder',help='s3 folder for manifest',action="store_true")

    parser.add_argument('-t', '--manifest', help='input manifest' , default="/Users/cheny39/Documents/PythonProject/tmp/input_template.txt")
    parser.add_argument('-d', '--dir', help='upload files\'s location' , default="/Users/cheny39/Documents/PythonProject/tmp/")
    parser.add_argument('-isb', '--input-s3-bucket', help='S3 bucket name for files', default="yizhen-file-loader")
    parser.add_argument('-isf', '--input-s3-folder', help='S3 folder for files',default="input")
    parser.add_argument('-osb', '--output-s3-bucket',help='s3 bucket for manifest',default="yizhen-file-loader")
    parser.add_argument('-osf', '--output-s3-folder',help='s3 folder for manifest',default="output")

    args = parser.parse_args()


    
    # for simplicity sake, we use Optional arguments instead of positional argument(required argument), extra arg check is required.
    # check args
    # flag_are_args_completed = True
    # if(not args.manifest):
    #     log.error('the following arguments are required: -t')
    #     flag_are_args_completed=False
    #
    # if(not args.dir):
    #     log.error('the following arguments are required: -d')
    #     flag_are_args_completed=False
    #
    # if(not args.input_s3_bucket):
    #     log.error('the following arguments are required: -isb')
    #     flag_are_args_completed=False
    #
    # if(not args.input_s3_folder):
    #     log.error('the following arguments are required: -isf')
    #     flag_are_args_completed=False
    #
    # if(not args.output_s3_bucket):
    #     log.error('the following arguments are required: -osb')
    #     flag_are_args_completed=False
    #
    # if(not args.output_s3_folder):
    #     log.error('the following arguments are required: -osf')
    #     flag_are_args_completed=False
    #
    # if(not flag_are_args_completed):
    #     sys.exit(1)


    
    input_bucket = S3Bucket(args.input_s3_bucket)
    output_bucket = S3Bucket(args.output_s3_bucket)
    start = timer()
    if not validate_input(args):
        log.error('validate input fails')
        sys.exit(1)


    if not upload_files_based_on_manifest(input_bucket,args.input_s3_folder, args.dir,args.input_s3_bucket,args.manifest):
        log.error('Upload files to S3 bucket "{}" failed!'.format(args.input_s3_bucket))
        sys.exit(1)


    if not export_result(args.manifest,output_bucket,args.output_s3_bucket,args.output_s3_folder,args.dir,args.input_s3_bucket,args.input_s3_folder):
        log.error('Upload files to S3 bucket "{}" failed!'.format(args.output_s3_bucket))
        sys.exit(1)
    end = timer()
    log.info('Cheers, Job Finished !! Total Execution Time: {:.2f} seconds'.format(end - start))


if __name__ == '__main__':
    main()