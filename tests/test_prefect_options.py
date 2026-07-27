from unittest.mock import Mock, patch

from github_refs import get_github_refs
from prefect_options import DEFAULT_FLOW_NAMES, load_prefect_options


def test_prefect_options_default_when_config_is_missing(tmp_path):
    flow_names, include_tags = load_prefect_options(tmp_path / "missing.yaml")

    assert flow_names == DEFAULT_FLOW_NAMES
    assert include_tags is False


def test_prefect_options_support_partial_flow_names_and_tags(tmp_path):
    config_file = tmp_path / "prefect-ins.yaml"
    config_file.write_text(
        """
prefect_options:
  flow_names:
    opensearch_loader: Custom OpenSearch Loader
  include_github_tags: true
"""
    )

    flow_names, include_tags = load_prefect_options(config_file)

    assert flow_names["data_loader"] == "CRDC Data Loader"
    assert flow_names["data_hub_loader"] == "CRDC Data Hub Loader"
    assert flow_names["opensearch_loader"] == "Custom OpenSearch Loader"
    assert include_tags is True


def _github_response(names):
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = [{"name": name} for name in names]
    return response


@patch("github_refs.requests.get")
def test_github_refs_read_only_branches_by_default(mock_get):
    mock_get.return_value = _github_response(["main", "develop"])

    refs = get_github_refs("https://github.com/CBIIT/example.git")

    assert refs == ["main", "develop"]
    assert "/branches?" in mock_get.call_args.args[0]


@patch("github_refs.requests.get")
def test_github_refs_can_include_tags_and_remove_duplicates(mock_get):
    mock_get.side_effect = [
        _github_response(["main", "release"]),
        _github_response(["v3.2.0", "release"]),
    ]

    refs = get_github_refs(
        "https://github.com/CBIIT/example.git",
        include_tags=True,
    )

    assert refs == ["main", "release", "v3.2.0"]
    assert "/branches?" in mock_get.call_args_list[0].args[0]
    assert "/tags?" in mock_get.call_args_list[1].args[0]
