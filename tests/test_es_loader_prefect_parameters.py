import importlib
import sys
from unittest.mock import patch


def load_es_loader_flow():
    sys.modules.pop("es_loader_prefect", None)
    with patch("github_refs.get_github_refs", return_value=[]):
        module = importlib.import_module("es_loader_prefect")
    return module.es_loader_prefect


def test_indices_list_is_optional_and_defaults_to_all_indices():
    flow = load_es_loader_flow()
    schema = flow.parameters.model_dump()

    assert schema["properties"]["indices_list"]["type"] == "array"
    assert schema["properties"]["indices_list"]["default"] == []
    assert "indices_list" not in schema["required"]
    assert "Leave empty to load every index" in (
        schema["properties"]["indices_list"]["description"]
    )
