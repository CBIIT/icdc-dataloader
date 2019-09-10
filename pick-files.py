#!/usr/bin/env python3

import glob
import argparse
import csv
import os, sys
import re
from shutil import copyfile

PATIENT_ID = 'patient_id'

def main():
    parser = argparse.ArgumentParser(description='Copy files which names contains case ID')
    parser.add_argument('-c', '--case-file', help='Case file contains all the case IDs', type=argparse.FileType('r'),
                        required=True)
    parser.add_argument('source', help='Source folder to copy files from')
    parser.add_argument('dest', help='Destination folder to copy files to')
    args = parser.parse_args()
    source_folder = args.source
    dest_folder = args.dest
    if not os.path.isdir(source_folder):
        print('{} is not a folder!'.format(source_folder))
        sys.exit(1)
    if not os.path.isdir(dest_folder):
        if not os.path.exists(dest_folder):
            os.makedirs(dest_folder)
            if not os.path.isdir(dest_folder):
                print('Can\'t create folder: {}!'.format(dest_folder))
                sys.exit(1)
            else:
                print('Dest folder "{}" created!'.format(dest_folder))
        else:
            print('{} is not a folder!'.format(dest_folder))
            sys.exit(1)

    patients = {}
    with args.case_file as case_file:
        reader = csv.DictReader(case_file, delimiter='\t')
        for case in reader:
            patient_id = case[PATIENT_ID]
            patients[patient_id] = []

    if len(patient_id) <= 0:
        print('No patient ids found in file: "{}"'.format(case_file.name))
        sys.exit(1)
    exist_files = glob.glob('{}/*'.format(source_folder))
    copied_files = 0
    for file in exist_files:
        base_name = os.path.basename(file)
        for patient_id in patients.keys():
            if re.search(patient_id, base_name):
                patients[patient_id].append(base_name)
                copied_files += 1
                print('Copying file {}'.format(base_name))
                copyfile(file, '{}/{}'.format(dest_folder, base_name))
                break
        # print('{} doesn\'t belong to a patient in the file'.format(base_name))
    patients_with_files = 0
    for patient_id, files in patients.items():
        if files:
            patients_with_files += 1
            print('{}: {}'.format(patient_id, files))
        else:
            print('{}: No files found'.format(patient_id))

    print('{} files picked for {} patients!'.format(copied_files, patients_with_files))


if __name__ == '__main__':
    main()
