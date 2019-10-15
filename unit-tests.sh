#!/bin/sh

CODE_FOLDER=$PWD
cd test
export PYTHONPATH=$PYTHONPATH:$CODE_FOLDER
export ICDC_DATA_LOADER_CONFIG=$CODE_FOLDER/config.ini
export ICDC_DATA_LOADER_PROP=$CODE_FOLDER/props.yml
python3 -m unittest
