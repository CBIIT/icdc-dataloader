import os

import yaml


PREFECT_CONFIG_FILE = "config/prefect-ins.yaml"

DEFAULT_FLOW_NAMES = {
    "data_loader": "CRDC Data Loader",
    "data_hub_loader": "CRDC Data Hub Loader",
    "opensearch_loader": "CRDC Data Hub ESloader",
}


def load_prefect_options(config_file=PREFECT_CONFIG_FILE):
    options = {}
    if os.path.exists(config_file):
        with open(config_file, "r") as file:
            config = yaml.safe_load(file) or {}
        options = config.get("prefect_options") or {}

    configured_names = options.get("flow_names") or {}
    flow_names = {
        key: configured_names.get(key) or default_name
        for key, default_name in DEFAULT_FLOW_NAMES.items()
    }
    include_github_tags = options.get("include_github_tags", False) is True
    return flow_names, include_github_tags
