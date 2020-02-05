#!/bin/sh

CODE_FOLDER=$PWD
cd tests
export PYTHONPATH=$PYTHONPATH:$CODE_FOLDER
export ICDC_DATA_LOADER_CONFIG=$CODE_FOLDER/config/config.ini
python3 -m unittest
