# icdc-dataloader
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/f4d5afb8403642dbab917cb4aa4ef47d)](https://www.codacy.com/manual/FNLCR_2/icdc-dataloader?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=CBIIT/icdc-dataloader&amp;utm_campaign=Badge_Grade)
This is the NCI ICDC/CTDC Data Loader
codacy test
Data Loader requires Python 3.6 or newer.

## Dependencies
Run ```pip3 install -r requirements.txt``` to install dependencies. Or run ```pip install -r requirements.txt``` if you are using virtualenv. 

## Command line arguments
-   ```-i/--uri, Neo4j URI, should look like "bolt://12.34.56.78:7687", default value "bolt://localhost:7687"```
-   ```-u/--user, Neo4j user, default value: "neo4j"```
-   ```-p/--password, Neo4j password, if omit, will read from environment variable "NEO_PASSWORD"```
-   ```-s/--schema, schema file's path, use multiple -s argument if you have schema files. This argument is required```
-   ```--prop-file, property file, for ICDC use config/props-icdc.yml, for CTDC use config/props-ctdc.yml. This argument is required```
-   ```--config-file, configuration file, example is in config/config.example.ini'. This argument is required```
-   ```-c/--cheat-mode, skip validations, aka. Cheat Mode```
-   ```-d/--dry-run, validations only, skip loading```
-   ```--wipe-db, wipe out database before loading```
-   ```--no-backup, kkip backup step```
-   ```-y/--yes, automatically confirm deletion and database wiping```
-   ```-M/--max-violations, max violations to display, default  value is 10```
-   ```-b/--bucket, s3 bucket name, use this argument only to load data from a S3 bucket```
-   ```-f/--s3-folder, s3 folder, use this argument only to load data from a S3 bucket```
-   ```-m/--mode, loading mode, valid values are: "UPSERT_MODE", "NEW_MODE" and "DELETE_MODE", default value is "UPSERT_MODE"```
-   ```<dir>, dataset directory, or local temporary folder when loading from a S3 bucket```

## Usage examples
To load data in /data/Dataset-20191119 to local Neo4j, with password 'secret'.

Run ```python3 loader.py -p secret -s tests/data/icdc-model.yml -s tests/data/icdc-model-props.yml  --config-file config/config.ini --prop-file config/props-icdc.yml /data/Dataset-20191119```
