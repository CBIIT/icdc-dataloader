from prefect import flow, task
from typing import Literal
from loader import main
from config import PluginConfig
from bento.common.secret_manager import get_secret
import os
import yaml
import requests
import subprocess
import glob
import prefect.variables as Variable
from bento.common.utils import get_logger

log = get_logger('LoaderPrefect')
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

def get_github_branches(repo_url):
    # Remove .git if present
    if repo_url.endswith('.git'):
        repo_url = repo_url[:-4]
    # Extract owner and repo name
    parts = repo_url.rstrip('/').split('/')
    owner, repo = parts[-2], parts[-1]
    branches = []
    page = 1
    while True:
        api_url = f'https://api.github.com/repos/{owner}/{repo}/branches?per_page=100&page={page}'
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            branches.extend([branch['name'] for branch in data])
            if len(data) < 100:
                break
            page += 1
        except Exception as e:
            log.error(f"Error fetching branches from GitHub: {e}")
            break
    return branches

def data_model_download(model_repo, model_version):
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
branch_choices = Literal[tuple(get_github_branches(model_repo_url))]
database_choices = Literal[tuple(list(config_drop_list.get(DATABASE_TYPES)))]

@flow(name="CRDC Data Loader", log_prints=True)
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
        plugins = []
    ):

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
        temp_folder
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
            temp_folder
    ):
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
        for plugin in plugins:
            self.plugins.append(PluginConfig(plugin))

        self.config_file = None


@flow(name="CRDC Data Hub Loader", log_prints=True)
def data_hub_loader(
        environment: environment_choices,
        model_branch: branch_choices,
        database_type: database_choices,
        s3_bucket,
        s3_folder,
        cheat_mode,
        dry_run,
        wipe_db,
        mode,
        prop_file,
        no_parents=True,
        plugins=[],
        split_transaction=True
    ):
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

    load_data(
        database_type = database_type,
        s3_bucket = s3_bucket,
        s3_folder = s3_folder,
        upload_log_dir = f's3://{s3_bucket}/{s3_folder}/logs', #
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
        split_transaction = split_transaction
    )

if __name__ == "__main__":
    # create your first deployment
    load_data.serve(name="local-data-loader-deployment")