#!/bin/sh

CODE_FOLDER=$PWD
cd test
export PYTHONPATH=$PYTHONPATH:$CODE_FOLDER
export ICDC_FILE_LOADER_CONFIG=$CODE_FOLDER/config.ini
python3 -m unittest
