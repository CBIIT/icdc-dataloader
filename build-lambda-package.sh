#!/bin/sh
# This script creates a deployable package (zip file) to load into AWS Lambda function

rm -rf dist
mkdir dist
cp utils.py dist/
cp raw_file_processor.py dist/
cd dist
zip -r file_loader_lambda.zip .