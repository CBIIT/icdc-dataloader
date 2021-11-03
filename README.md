# NCI ICDC/CTDC Data Loader
This is the documentation index for the NCI ICDC/CTDC Data Loader

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/f4d5afb8403642dbab917cb4aa4ef47d)](https://www.codacy.com/manual/FNLCR_2/icdc-dataloader?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=CBIIT/icdc-dataloader&amp;utm_campaign=Badge_Grade)

## Module List
The NCI ICDC/CTDC Data Loader includes multiple data loading modules:

-   **Data Loader**
    -   The Data Loader module is a versatile Python application used to load data into a Neo4j database.
    -   [Data Loader Documentation](docs/data-loader.md)

-   **File Copier**
    -   The File Copier module copies files from a source URL to a designated AWS S3 Bucket.
    -   [File Copier Documentation](docs/file-copier.md)
    
-   **File Loader**
    -   The File Loader module processes incoming S3 files and then calls the Data Loader module to load the processed file data into a Neo4j database.
    -   [File Loader Documentation](docs/file-loader.md)
    
-   **Model Converter**
    -   The Model Converter uses a combination of YAML format schema files, a YAML formatted properties files, and a GraphQL formatted queries file to generate a GraphQL formatted schema.
    -   [Model Converter Documentation](docs/model-converter.md)
