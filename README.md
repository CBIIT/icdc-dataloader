# NCI ICDC/CTDC Data Loader
This is the documentation index for the NCI ICDC/CTDC Data Loader

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/f4d5afb8403642dbab917cb4aa4ef47d)](https://www.codacy.com/manual/FNLCR_2/icdc-dataloader?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=CBIIT/icdc-dataloader&amp;utm_campaign=Badge_Grade)

## Module List
The NCI ICDC/CTDC Data Loader includes multiple data loading modules:

- **Data Loader**
  - The Data Loader module is a versatile Python application used to load data into a Neo4j database.
  - [Data Loader Documentation](docs/data-loader.md)

- **File Copier**
  - The File Copier module copies files from a source URL to a designated AWS S3 Bucket.
  - [File Copier Documentation](docs/file-copier.md)
  
- **File Loader**
  - The File Loader module processes incoming S3 files and then calls the Data Loader module to load the processed file data into a Neo4j database.
  - [File Loader Documentation](docs/file-loader.md)
  
- **Model Converter**
  - The Model Converter uses a combination of YAML format schema files, a YAML formatted properties files, and a GraphQL formatted queries file to generate a GraphQL formatted schema.
  - [Model Converter Documentation](docs/model-converter.md)

## Configuring Prefect flows

Prefect flow names and GitHub tag discovery can be configured with a
`prefect_options` section in the Prefect deployment YAML. The
[`config/prefect-ins.yaml`](config/prefect-ins.yaml) file is an example:

```yaml
prefect_options:
  flow_names:
    data_loader: "INS icdc-dataloader Neo4j Loading"
    data_hub_loader: "INS icdc-dataloader Neo4j Loading"
    opensearch_loader: "INS icdc-dataloader OpenSearch Indexing"
  include_github_tags: true
```

The supported `flow_names` keys are:

- `data_loader`: Name of the `load_data` Prefect flow.
- `data_hub_loader`: Name of the `data_hub_loader` Prefect flow.
- `opensearch_loader`: Name of the `es_loader_prefect` Prefect flow.

Each flow name is optional. If unspecified, the flow names default to:

- `CRDC Data Loader`
- `CRDC Data Hub Loader`
- `CRDC Data Hub ESloader`

`include_github_tags` is also optional and defaults to `false`. When it is
`false` or omitted, the Prefect parameter dropdowns contain GitHub branches
only. When it is `true`, GitHub tags are added to the available refs for the
model, backend, and frontend repositories. Duplicate branch and tag names
are shown only once.

The flow modules look for these options in `config/prefect-ins.yaml`. If the
file or the `prefect_options` section are absent, then default configuration
values will take effect.
