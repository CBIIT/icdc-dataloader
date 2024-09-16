from prefect import flow, task
from typing import Literal
from loader import main
from config import PluginConfig
from bento.common.secret_manager import get_secret

NEO4J_URI = "neo4j_uri"
NEO4J_PASSWORD = "neo4j_password"
SUBMISSION_BUCKET = "submission_bucket"
database_choices = Literal["neo4j", "memgraph"]

@flow(name="CRDC Data Loader", log_prints=True)
def load_data(
        database_type: database_choices,
        s3_bucket,
        s3_folder,
        upload_log_dir = None,
        dataset = "data",
        temp_folder = "tmp",
        uri = "bolt://127.0.0.1:7687",
        user = "neo4j",
        password = "your-password",
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
        database_type: database_choices,
        organization_id,
        submission_id,
        cheat_mode,
        dry_run,
        wipe_db,
        mode,
        secret_name,
        schemas,
        prop_file,
        no_parents=True,
        plugins=[]
    ):

    secret = get_secret(secret_name)
    uri = secret[NEO4J_URI]
    password = secret[NEO4J_PASSWORD]
    s3_bucket = secret[SUBMISSION_BUCKET]
    s3_folder = f'{organization_id}/{submission_id}/metadata'

    load_data(
        database_type = database_type,
        s3_bucket = s3_bucket,
        s3_folder = s3_folder,
        upload_log_dir = f's3://{s3_bucket}/{s3_folder}/logs', #
        uri = uri,
        password = password,
        schemas = schemas,
        prop_file = prop_file,
        cheat_mode = cheat_mode,
        dry_run = dry_run,
        wipe_db = wipe_db,
        no_parents = no_parents,
        max_violation = 1000000,
        mode = mode,
        plugins = plugins
    )

if __name__ == "__main__":
    # create your first deployment
    load_data.serve(name="local-data-loader-deployment")