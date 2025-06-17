from es_loader import ESLoader, _validate_cypher_queries
from prefect import flow
from typing import Literal
from bento.common.secret_manager import get_secret
from bento.common.utils import get_logger, print_config, LOG_PREFIX, APP_NAME
import yaml
import os
import prefect.variables as Variable
from neo4j import GraphDatabase

MEMGRAPH_SECRET = "neo4j_secret"
MEMGRAPH_ENDPOINT = "memgraph_endpoint"
MEMGRAPH_USER = "memgraph_user"
MEMGRAPH_PASSWORD = "memgraph_password"
ES_HOST = "es_host"

config_file = "config/prefect_drop_down_config_esloader.yaml"
with open(config_file, 'r') as file:
    config_drop_list = yaml.safe_load(file)
environment_choices = Literal[tuple(list(config_drop_list.keys()))]

@flow(name="CRDC Dadta Hub ESloader", log_prints=True)
def es_loader_prefect(
    environment: environment_choices, # type: ignore
    about_file,
    model_files,
    prop_file,
    indices_list,
    indices_file,
):  
    with open(indices_file, 'r') as file:
        indices_yaml = yaml.safe_load(file)
    indices = indices_yaml['Indices']
    logger = get_logger('ESLoader')
    config = {}
    config['model_files'] = model_files
    config['about_file'] = about_file
    config['prop_file'] = prop_file
    config['indices_list'] = indices_list
    neo4j_secret = Variable.get(config_drop_list[environment][MEMGRAPH_SECRET])
    secret = get_secret(neo4j_secret)
    config['memgraph_endpoint'] = "bolt://" + secret[MEMGRAPH_ENDPOINT] + ":7687"
    config['memgraph_user'] = secret[MEMGRAPH_USER]
    config['memgraph_password'] = secret[MEMGRAPH_PASSWORD]
    config['es_host'] = secret[ES_HOST]
    print_config(logger, config)

    neo4j_driver = GraphDatabase.driver(
        config['memgraph_endpoint'],
        auth=(config['memgraph_user'],  config['memgraph_password']),
        encrypted=False
    )

    # ...existing code...

    with neo4j_driver.session() as session:
        result = session.run("MATCH (n) RETURN count(n) AS node_count")
        node_count = result.single()["node_count"]
        logger.info(f"Total nodes in database: {node_count}")

    # ...existing code...

    loader = ESLoader(
        es_host=config['es_host'],
        neo4j_driver=neo4j_driver
    )

    load_model = False
    if 'model_files' in config and config['model_files'] and 'prop_file' in config and config['prop_file']:
        loader.read_model(config['model_files'], config['prop_file'])
        load_model = True

    summary = {}
    indices_list = config.get('indices_list')
    if isinstance(indices_list, list):
        if len(indices_list) > 0:
            lower_indices_list = [item.lower() for item in indices_list]
            logger.warning(f"An indices list is provided, only the indices in the indices list {indices_list} will be loaded")
        else:
            logger.warning("Empty indices_list value is provided, all the indices will be loaded")
            indices_list = None
    else:
        logger.warning(f"Invalid indices_list value {indices_list} is provided, all the indices will be loaded")
        indices_list = None

    index_name_list = []
    for index in indices:
        index_name = index.get('index_name')
        index_name_list.append(index_name.lower())
        if indices_list is not None and index_name is not None:
            lower_index_name = index_name.lower()
            if lower_index_name not in lower_indices_list:
                continue
        summary[index_name] = "ERROR!"
        logger.info(f'Begin loading index: "{index_name}"')
        if 'type' not in index or index['type'] == 'neo4j':
            cypher_queries = index.get('cypher_queries')
            cypher_query = index.get('cypher_query')
            if cypher_queries is None and cypher_query is not None:
                cypher_queries = [{'query': cypher_query}]
            try:
                _validate_cypher_queries(cypher_queries)
                summary[index_name] = loader.load(index_name, index['mapping'], cypher_queries)
            except Exception as ex:
                logger.error(f'There is an error in the "{index_name}" index definition, this index will not be loaded')
                logger.error(ex)
        elif index['type'] == 'about_file':
            if 'about_file' in config:
                loader.load_about_page(index_name, index['mapping'], config['about_file'])
                summary[index_name] = "Loaded Successfully"
            else:
                logger.warning(f'"about_file" not set in configuration file, {index_name} will not be loaded!')
        elif index['type'] == 'model':
            if load_model and 'subtype' in index:
                loader.load_model(index_name, index['mapping'], index['subtype'])
                summary[index_name] = "Loaded Successfully"
            else:
                logger.warning(
                    f'"model_files" not set in configuration file, {index_name} will not be loaded!')
        elif index['type'] == 'external':
            logger.info("External data index created - loading will be done via data retriever service")
            loader.create_index(index_name, index["mapping"])
            summary[index_name] = "Index created"
        else:
            logger.error(f'Unknown index type: "{index["type"]}"')
    if indices_list is not None:
        for indices_name in indices_list:
            if indices_name.lower() not in index_name_list:
                logger.warning(f'The index {indices_name} in the indices list does not exist in the definition file')
    logger.info(f'Index loading summary:')
    for index in summary.keys():
        logger.info(f'{index}: {summary[index]}')

if __name__ == "__main__":
    # create your first deployment
   es_loader_prefect.serve(name="es_loader")