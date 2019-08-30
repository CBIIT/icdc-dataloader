#!/usr/bin/env python3
import os, sys
import argparse
from s3 import *
import uuid
import csv
from os.path import isfile, join
import time

def upload_files_in_folder(s3_folder,directory,bucket):
    try:
        for f in os.listdir(directory):
            if isfile(os.path.join(directory, f)):
                bucket.upload_file(join(s3_folder, f),join(directory, f))
        return True
    except Exception as e:
        print (e)
        return False

def export_result(s3_bucket,s3_folder,files_directory,output_directory):
    try:
        if not os.path.exists(output_directory):
                os.makedirs(output_directory)

        data_matrix =[]
        # data header
        data_matrix.append(["f_uuid","f_name","f_size","f_location","f_format"])
        for f in os.listdir(files_directory):
                if isfile(join(files_directory, f)):
                    f_size = os.stat(join(files_directory, f)).st_size
                    f_name = f
                    f_location = s3_bucket + "/" +s3_folder+"/" +f
                    # need to format? upcase?
                    f_format = os.path.splitext(f_name)[1]
                    f_uuid =uuid.uuid1()
                    data_matrix.append([f_uuid,f_name,f_size,f_location,f_format])
        write_tsv_file(output_directory,data_matrix)
        return True
    except Exception as e:
        print (e)
        return False

def write_tsv_file(f,data):
    timestr = time.strftime("%Y%m%d-%H%M%S")
    output_file = f+"/" +timestr + ".tsv"
    with open(output_file, 'wt') as out_file:
        tsv_writer = csv.writer(out_file, delimiter='\t')
        for  index in range(len(data)):
            tsv_writer.writerow(data[index])


# File loader will try to upload all files from given directory into S3

def main():
    parser = argparse.ArgumentParser(description='Upload files from local to S3')
    parser.add_argument('-d', '--dir', help='local' , default="/Users/cheny39/Documents/PythonProject/tmp/")
    parser.add_argument('-s3n', '--bucket', help='S3 bucket name', default="icdcfile")
    parser.add_argument('-s3f', '--s3-folder', help='S3 folder',default="input")
    parser.add_argument('-o', '--output',help='Output directory',default="/Users/cheny39/Documents/PythonProject/tmp/output/")

    args = parser.parse_args()

    log = get_logger('S3 Loader')
 
    if args.s3_folder:
        if not args.bucket:
            log.error('Please specify S3 bucket name with -s3n/--bucket argument!')
            sys.exit(1)
        bucket = S3Bucket(args.bucket)

    if not os.path.exists(args.dir):
        log.error('Folder: "{}" is not found'.format(args.dir))
        sys.exit(1)

    if not os.path.isdir(args.dir):
        log.error('{} is not a directory!'.format(args.dir))
        sys.exit(1)  

    if not upload_files_in_folder(args.s3_folder, args.dir,bucket):
        log.error('Upload files to S3 bucket "{}" failed!'.format(args.bucket))
        sys.exit(1)


    if not export_result(args.bucket,args.s3_folder,args.dir,args.output):
        log.error('Upload files to S3 bucket "{}" failed!'.format(args.bucket))
        sys.exit(1)


if __name__ == '__main__':
    main()