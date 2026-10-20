import importlib
import sys
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch

import pytest


def load_es_loader_flow():
    sys.modules.pop("es_loader_prefect", None)
    with patch("github_refs.requests.get") as github_request:
        module = importlib.import_module("es_loader_prefect")
    github_request.assert_not_called()
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


def test_all_repository_parameters_accept_future_branches():
    flow = load_es_loader_flow()
    schema = flow.parameters.model_dump()
    for name in ("model_branch", "backend_branch", "frontend_branch"):
        parameter = schema["properties"][name]
        assert parameter["type"] == "string"
        assert "enum" not in parameter
        assert "new branches need no deployment update" in parameter["description"]

    parameters = dict(
        environment="dev", database_type="neo4j", model_branch="2.5.0",
        backend_branch="future/backend", frontend_branch="future/frontend",
        about_file="about.yaml", indices_file="indices.yaml", prop_file="props.yaml",
    )
    validated = flow.validate_parameters(parameters)
    for name in ("model_branch", "backend_branch", "frontend_branch"):
        assert validated[name] == parameters[name]


def git(repo, *args):
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    )


@pytest.fixture
def remote_repo(tmp_path):
    repo = tmp_path / "remote"
    repo.mkdir()
    git(repo, "init", "--initial-branch=main")
    git(repo, "config", "user.email", "test@example.com")
    git(repo, "config", "user.name", "Test")
    (repo / "content.yaml").write_text("version: main\n")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "Initial repository")
    return repo


def test_download_fetches_new_branch_latest_commit_and_tag(remote_repo, tmp_path, monkeypatch):
    load_es_loader_flow()
    module = sys.modules["es_loader_prefect"]
    monkeypatch.setattr(module.tempfile, "tempdir", str(tmp_path))
    # The branch is created after generating the deployed parameter schema.
    git(remote_repo, "checkout", "-b", "2.5.0")
    (remote_repo / "content.yaml").write_text("version: 2.5.0\n")
    git(remote_repo, "commit", "-am", "New branch")
    first = module.repo_download(remote_repo.as_uri(), "2.5.0", Mock())
    assert (Path(first) / "content.yaml").read_text() == "version: 2.5.0\n"

    (remote_repo / "content.yaml").write_text("version: updated\n")
    git(remote_repo, "commit", "-am", "Update branch")
    second = module.repo_download(remote_repo.as_uri(), "2.5.0", Mock())
    assert second != first
    assert (Path(second) / "content.yaml").read_text() == "version: updated\n"

    git(remote_repo, "tag", "release-2.5.0")
    tagged = module.repo_download(remote_repo.as_uri(), "release-2.5.0", Mock())
    assert (Path(tagged) / "content.yaml").read_text() == "version: updated\n"


def test_missing_branch_stops_download(remote_repo, tmp_path, monkeypatch):
    load_es_loader_flow()
    module = sys.modules["es_loader_prefect"]
    monkeypatch.setattr(module.tempfile, "tempdir", str(tmp_path))
    with pytest.raises(subprocess.CalledProcessError):
        module.repo_download(remote_repo.as_uri(), "missing-branch", Mock())
