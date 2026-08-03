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

## Prefect Flows

Before proceeding, if you're the one who will deploy or run these Prefect flows,
then submit a request to be added to the FNL Prefect developers mailing list. Being
on the mailing list will let you log in to Prefect Cloud and deploy/run flows.

### Configuration

No need to edit code, but if you discover that code changes are needed, get them merged upstream.

Three configuration files tailor `icdc-dataloader`'s Prefect flows for INS's needs:

- [`config/prefect-ins.yaml`](../config/prefect-ins.yaml)
  - Name the project in `name`, up top.
  - Specify flow names in the `prefect_options` section. The supported `flow_names` keys are:
    - `data_loader`: Name of the `load_data` Prefect flow.
    - `data_hub_loader`: Name of the `data_hub_loader` Prefect flow.
    - `opensearch_loader`: Name of the `es_loader_prefect` Prefect flow.

    Each flow name is optional. If unspecified, the flow names default to:
    - `CRDC Data Loader`
    - `CRDC Data Hub Loader`
    - `CRDC Data Hub ESloader`
  - Configure `include_github_tags` to read GitHub tags in the
    `prefect_options` section or not.
    This configuration field is optional and defaults to `false`.
    When it is `false` or omitted, the Prefect parameter dropdowns contain
    GitHub branches only. When it is `true`, GitHub tags are added to the
    available refs for the model, backend, and frontend repositories.
    Duplicate branch and tag names are shown only once.
  - Specify the INS branch `3.2.0_ins` of `icdc-dataloader` in the `pull` section.
  - Specify `name`, `parameters`, and `work_pool` for the `ins-metadata-loading-dev` (Loads TSV data into Neo4j)
    and `ins-opensearch-loader` (indexes Neo4j data in OpenSearch) deployments.
  - Ignore other deployments defined in this file.
- [`config/prefect_drop_down_config_dataloader.yaml`](../config/prefect_drop_down_config_dataloader.yaml)
  - Specify parameters for the `ins-metadata-loading-dev` flow.
- [`config/prefect_drop_down_config_esloader.yaml`](../config/prefect_drop_down_config_esloader.yaml)
  - Specify parameters for te `ins-opensearch-loader` flow.

### Prefect Cloud Workspace Variables

Define the following Workspace Variables (Settings -> Variables) in Prefect Cloud:

- `ins_secret_name_dev`
  - The value of this variable should be the key of the key-value pair in AWS Secrets Manager for the INS Dev
    environment secrets.
  - Eg: suppose AWS Secrets Manager is set up like so:

    ```json
    {
      ..., // Other projects' secrets
      "super_secret_ins_stuff": {
        "neo4j_host": "123.456.7.890",
        "neo4j_user": "my_username",
        "opensearch_host": "234.567.8.901",
        ... // Other INS secrets
      },
      ... // Other projects' secrets
    }
    ```

    Then `ins_secret_name_dev` should be set to `"super_secret_ins_stuff"`.

### Deployment

To deploy a flow to Prefect Cloud, install Prefect in your local environment (eg: `pip install prefect`)
and run the command

```bash
prefect deploy --prefect-file config/prefect-ins.yaml
```

Select the flow you want to deploy, and choose "No" for all the options that follow.

### Execution

To run a Prefect flow:

1. Log in to Prefect Cloud, and make sure that you're in the `ccdi-workspace` workspace.
2. Search "Deployments" for `ins-opensearch-loader` or `ins-metadata-loading-dev` - whichever flow you're trying to run -
  and click on the deployment.
3. Click on "Run", and choose "Quick run" or "Custom run" - whichever floats your boat.
4. Select or fill in each parameter for the run.
    - For `ins-metadata-loading-dev`, beware that `s3_bucket` is a string, but `s3_folder` should be an array of one string.
    - For `ins-metadata-loading-dev`, choose `false` for `cheat_mode` and `true` for `wipe_db`, unless you have a particular
      reason not to.
    - For `ins-opensearch-loader`, an empty array `[]` for `indices_list` will instruct the flow to index **all** indices.
