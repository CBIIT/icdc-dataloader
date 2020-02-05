#!/bin/sh

CODE_FOLDER=$PWD
cd tests
export PYTHONPATH=$PYTHONPATH:$CODE_FOLDER
python3 -m unittest
