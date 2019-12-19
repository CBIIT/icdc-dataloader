#!/bin/sh

CODE_FOLDER=$PWD
cd tests
export PYTHONPATH=$PYTHONPATH:$CODE_FOLDER
export ICDC_DATA_LOADER_CONFIG=$CODE_FOLDER/config/config.ini
export ICDC_DATA_LOADER_PROP=$CODE_FOLDER/config/props.yml
python3 -m unittest
