from prefect import flow
from jinja2 import Environment, FileSystemLoader
from prefect.blocks.system import Secret
import subprocess

import time
# Hardcoding Vars Temporarily
neo4j_ip='localhost'
neo4j_password=Secret.load("neo4j-password-dev").get()
model_file1='examples/icdc/data-model/icdc-model.yml'
model_file2='examples/icdc/data-model/icdc-model-props.yml'
property_file='config/props-icdc-pmvp.yml'
data_bucket='nci-cbiit-caninedatacommons-dev'
# A Simple Class to hold Args
class ArgsObjects:
    pass

@flow(name="ICDC Data Loader Flow")
def data_loader_wrapper(environment='dev',project_name='icdc',s3_folder='',wipe_db='false',cheat_mode='false',split_transactions='false', flush_redis='true'):
    # Load Jinja Template
    jinja_template_env = Environment(loader=FileSystemLoader("config/"))
    config_template = jinja_template_env.get_template('config.yml.j2')
    # Hardcoding Config Variables

    cheat_mode=cheat_mode
    wipe_db=wipe_db
    split_transactions=split_transactions
    
    s3_folder=s3_folder
    content=config_template.render(neo4j_ip=neo4j_ip,neo4j_password=neo4j_password,
                                 model_file1=model_file1,model_file2=model_file2,
                                 property_file=property_file,cheat_mode=cheat_mode,
                                 wipe_db=wipe_db,split_transactions=split_transactions,
                                 data_bucket=data_bucket,s3_folder=s3_folder)

    # Write to Jinja template
    filename='config/config.yml'
    with open(filename, mode="w", encoding="utf-8") as message:
        message.write(content)
        print(f"... wrote {filename}") 

    subprocess.call('pwd', timeout=60, shell=True)     
    subprocess.call('git submodule update --init --recursive', timeout=60, shell=True)
    subprocess.call('pwd', timeout=60, shell=True) 
    subprocess.call('ls -l', timeout=60, shell=True)
    print("Imported Submodules")
    #time.sleep(10)
    #import loader as neo4j_loader
    import loader as neo4j_loader
    args=populate_args(environment,project_name,s3_folder,wipe_db,cheat_mode,split_transactions,flush_redis)
    neo4j_loader.main(args)
    
    pass

# This function is required to populate the args object to pass to the main method in loader.py
def populate_args(environment='dev',project_name='icdc',s3_folder='',wipe_db='false',cheat_mode='false',split_transactions='false', flush_redis='true'):
    from bento.common.utils import UPSERT_MODE
    args = ArgsObjects
    args.uri='bolt://'+ neo4j_ip+ ':7687'
    args.config_file='config/config.yml'
    args.password = "password"
    args.schema = ['tests/data/icdc-model.yml','tests/data/icdc-model-props.yml']
    args.prop_file ='config/props-icdc.yml'
    args.dataset='examples\icdc\dataset'
    args.no_backup=True
    args.cheat_mode=True
    args.wipe_db=None
    args.split_transactions=False
    args.no_backup=True
    args.backup_folder=None
    args.s3_folder=None
    args.bucket=None
    args.user='neo4j'
    args.dry_run=False
    args.mode=None
    args.max_violations=10
    args.yes=None
    args.loading_mode= UPSERT_MODE
    return args


if __name__ == "main":
    data_loader_wrapper()

#neo4j_loader.main()
if __name__ == "__main__":
    data_loader_wrapper('dev','icdc','',wipe_db='false',cheat_mode='false',split_transactions='false', flush_redis='true')