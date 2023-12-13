from prefect import flow

from loader import main
from config import PluginConfig


@flow(name="CRDC Data Loader", log_prints=True)
def load_data(
        dataset,
        uri = "bolt://127.0.0.1:7687",
        user = "neo4j",
        password = "your-password",
        schemas = ["/Users/yingm3/work/icdc/code/model-tool/model-desc/icdc-model.yml", "/Users/yingm3/work/icdc/code/model-tool/model-desc/icdc-model-props.yml"],
        prop_file = "config/props-icdc-pmvp.yml",
        bucket = None,
        s3_folder = None,
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
        upload_log_dir = None,
        plugins = []
    ):

    params = Config(
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
        plugins
    )
    main(params)

class Config:
    def __init__(
            self,
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
            plugins
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
        for plugin in plugins:
            self.plugins.append(PluginConfig(plugin))

        self.config_file = None


if __name__ == "__main__":
    # create your first deployment
    load_data.serve(name="local-data-loader-deployment")