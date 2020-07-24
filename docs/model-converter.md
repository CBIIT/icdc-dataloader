---
layout: default
nav_order: 1
title: Model Converter
---
# Model Converter
This is the user documentation for the model converter module contained in the ICDC-Dataloader utility.
## Introduction
The Model Converter is used to generate a GraphQL formatted schema from YAML format input files. 
## Pre-requisites
## Dependencies
## Inputs
* YAML Formatted schema file(s)
* YAML formatted properties file
* GraphQL formatted queries file
* Filepath for the generated outputs
## Outputs
* A GraphQL formatted schema
* A simplified and easier to read version of the GraphQL schema that used for documentation purposes only.
## Command Line Arguments
* Schema File(s)
    * The YAML formatted schema file(s) that contain the model specifications used to generate the GrapQL schema.
    * Command: ````-s/--schema````
    * At least one schema file is required
    * Default Value: ````N/A````
* Properties File
    * The YAML formatted schema properties file for the data model.
    * Command: ````-p/--prop-file````
    * Required
    * Default Value: ````N/A````
* Query File
    * A GraphQL formatted file containing the schema definition block and query type block. This file will be appended to the end of the generated GraphQL schema.
    * Command: ````-q/--query-file````
    * Required
    * Default Value: ````N/A````
* Output Filepath
    * The desired filepath of the output GraphQL schema, the simplified schema will have the same filepath as the GraphQL schema with ````-doc```` appended to the file name.
    * Command: ````-o/--output````
    * Required
    * Default Value: ````N/A````
## Usage Example
Below is an example command to run the model converter:
````
model-converter.py --schema schema-file-1.yml --schema schema-file-2.yml --prop-file schema-properties.yml --query-file queries.graphql --output output_folder\data-model-schema.graphql
````
### Example Inputs
* Schema Files
    * ````schema-file-1.yml````
    * ````schema-file-2.yml````
* Properties File
    * ````schema-properties.yml````
* Query File
    * ````queries.graphql````
* Output Filepath
    * ````output_folder\data-model-schema.graphql````
### Example Outputs
* GraphQL Schema
    * ````output_folder\data-model-schema.graphql````
* Simplified Documentation Schema
    * ````output_folder\data-model-schema-doc.graphql````
