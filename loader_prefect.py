from prefect import flow
from typing import Any, Dict, List, Literal
import os
import yaml
import subprocess
import glob
import prefect.variables as Variable
from github_refs import get_github_refs
from prefect_options import load_prefect_options

ENVIRONMENTS = "environments"
DATABASE_TYPES = "database_type"
MODEL_REPO_URL = "model_repo_url"
NEO4J_USER = "neo4j_user"
NEO4J_IP = "neo4j_ip"
NEO4J_PASSWORD = "neo4j_password"
SUBMISSION_BUCKET = "submission_bucket"
MODEL_DESC = "model-desc"
MEMGRAPH_USER = "memgraph_user"
MEMGRAPH_ENDPOINT = "memgraph_endpoint"
MEMGRAPH_PASSWORD = "memgraph_password"

config_file = "config/prefect_drop_down_config_dataloader.yaml"

def data_model_download(model_repo, model_version):
    from bento.common.utils import get_logger

    log = get_logger('LoaderPrefect')
    subprocess.run(['git', 'clone', model_repo])
    model_folder = os.path.splitext(os.path.basename(model_repo))[0]
    subprocess.run(['git', '-C', model_folder, 'checkout', model_version])
    log.info(f"Finished cloning the data model repository from {model_repo} to {model_folder}")
    model_yaml_files = glob.glob(f'{model_folder}/{MODEL_DESC}/*model*.yaml')
    model_yml_files = glob.glob(f'{model_folder}/{MODEL_DESC}/*model*.yml')
    schemas = model_yaml_files + model_yml_files
    return schemas



with open(config_file, 'r') as file:
    config_drop_list = yaml.safe_load(file)
env = config_drop_list[ENVIRONMENTS].keys()
environment_choices = Literal[tuple(list(env))]
model_repo_url = config_drop_list.get(MODEL_REPO_URL)
flow_names, include_github_tags = load_prefect_options()
model_refs = get_github_refs(model_repo_url, include_github_tags)
branch_choices = Literal[tuple(model_refs)] if model_refs else str
database_choices = Literal[tuple(list(config_drop_list.get(DATABASE_TYPES)))]

@flow(name=flow_names["data_loader"], log_prints=True)
def load_data(
        database_type,
        s3_bucket,
        s3_folder,
        upload_log_dir = None,
        dataset = "data",
        temp_folder = "tmp",
        uri = "bolt://127.0.0.1:7687",
        user = "neo4j",
        password = "password",
        schemas = ["../icdc-model-tool/model-desc/icdc-model.yml", "../icdc-model-tool/model-desc/icdc-model-props.yml"],
        prop_file = "config/props-icdc-pmvp.yml",
        backup_folder = None,
        cheat_mode = False,
        dry_run = False,
        wipe_db = False,
        no_backup = True,
        no_parents = True,
        verbose = False,
        yes = True,
        max_violation = 1000000,
        mode = "upsert",
        split_transaction = False,
        plugins = [],
        empty_cell_null = True,
        skip_permissive_values_validation = False
    ):
    from loader import main

    params = Config(
        database_type,
        dataset,
        uri,
        user,
        password,
        schemas,
        prop_file,
        s3_bucket,
        s3_folder,
        backup_folder,
        cheat_mode,
        dry_run,
        wipe_db,
        no_backup,
        no_parents,
        verbose,
        yes,
        max_violation,
        mode,
        split_transaction,
        upload_log_dir,
        plugins,
        temp_folder,
        empty_cell_null,
        skip_permissive_values_validation
    )
    main(params)

class Config:
    def __init__(
            self,
            database_type,
            dataset,
            uri,
            user,
            password,
            schemas,
            prop_file,
            bucket,
            s3_folder,
            backup_folder,
            cheat_mode,
            dry_run,
            wipe_db,
            no_backup,
            no_parents,
            verbose,
            yes,
            max_violation,
            mode,
            split_transaction,
            upload_log_dir,
            plugins,
            temp_folder,
            empty_cell_null,
            skip_permissive_values_validation

    ):
        from config import PluginConfig

        self.dataset = dataset
        self.uri = uri
        self.user = user
        self.password = password
        self.schema = schemas
        self.prop_file = prop_file
        self.bucket = bucket
        self.s3_folder = s3_folder
        self.backup_folder = backup_folder
        self.cheat_mode = cheat_mode
        self.dry_run = dry_run
        self.wipe_db = wipe_db
        self.no_backup = no_backup
        self.no_parents = no_parents
        self.verbose = verbose
        self.yes = yes
        self.max_violations = max_violation
        self.mode = mode
        self.split_transactions = split_transaction
        self.upload_log_dir = upload_log_dir
        self.plugins = []
        self.temp_folder = temp_folder
        self.database_type = database_type
        self.empty_cell_null = empty_cell_null
        self.skip_permissive_values_validation = skip_permissive_values_validation
        for plugin in plugins:
            self.plugins.append(PluginConfig(plugin))

        self.config_file = None


@flow(name=flow_names["data_hub_loader"], log_prints=True)
def data_hub_loader(
        environment: environment_choices, # type: ignore
        model_branch: branch_choices, # type: ignore
        database_type: database_choices, # type: ignore
        s3_bucket: str,
        s3_folder: List[str],
        cheat_mode: bool,
        dry_run: bool,
        wipe_db: bool,
        mode: str,
        prop_file: str,
        no_parents: bool=True,
        plugins: List[Dict[str, Any]]=[],
        split_transaction: bool=True,
        empty_cell_null: bool=True,
        skip_permissive_values_validation: bool=False,
    ):
    from bento.common.secret_manager import get_secret

    secret_name = Variable.get(config_drop_list[ENVIRONMENTS][environment])
    secret = get_secret(secret_name)
    user = secret[NEO4J_USER]
    uri = "bolt://" + secret[NEO4J_IP] + ":7687"
    password = secret[NEO4J_PASSWORD]
    if database_type == "memgraph":
        user = secret[MEMGRAPH_USER]
        uri = "bolt://" + secret[MEMGRAPH_ENDPOINT] + ":7687"
        password = secret[MEMGRAPH_PASSWORD]
    
    schemas = data_model_download(model_repo_url, model_branch)
    if isinstance(s3_folder, str):
        s3_folder = [s3_folder]
    upload_log_dir = []
    for s3f in s3_folder:
        upload_log_dir.append(f's3://{s3_bucket}/{s3f}/logs')


    load_data(
        database_type = database_type,
        s3_bucket = s3_bucket,
        s3_folder = s3_folder,
        upload_log_dir = upload_log_dir,
        uri = uri,
        user = user,
        password = password,
        schemas = schemas,
        prop_file = prop_file,
        cheat_mode = cheat_mode,
        dry_run = dry_run,
        wipe_db = wipe_db,
        no_parents = no_parents,
        max_violation = 1000000,
        mode = mode,
        plugins = plugins,
        split_transaction = split_transaction,
        empty_cell_null = empty_cell_null,
        skip_permissive_values_validation = skip_permissive_values_validation
    )

if __name__ == "__main__":
    # create your first deployment
    load_data.serve(name="local-data-loader-deployment")
