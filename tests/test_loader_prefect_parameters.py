import importlib
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest


def load_loader_module():
    sys.modules.pop("loader_prefect", None)
    with patch("github_refs.requests.get") as github_request:
        module = importlib.import_module("loader_prefect")
    github_request.assert_not_called()
    return module


def git(repo, *args):
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    )


@pytest.fixture
def model_repo(tmp_path):
    repo = tmp_path / "ins-model"
    repo.mkdir()
    git(repo, "init", "--initial-branch=main")
    git(repo, "config", "user.email", "test@example.com")
    git(repo, "config", "user.name", "Test")
    (repo / "model-desc").mkdir()
    (repo / "model-desc" / "ins-model.yml").write_text("version: main\n")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "Initial model")
    return repo


def test_model_parameter_accepts_future_branches_without_redeployment():
    module = load_loader_module()
    schema = module.data_hub_loader.parameters.model_dump()
    parameter = schema["properties"]["model_branch"]

    assert parameter["type"] == "string"
    assert "enum" not in parameter
    assert "new branches need no deployment update" in parameter["description"]
    parameters = dict(
        environment="dev", database_type="neo4j", s3_bucket="test",
        s3_folder=[], cheat_mode=False, dry_run=True, wipe_db=False,
        mode="upsert", prop_file="config/props-ins.yaml",
        model_branch="future/9.0.0",
    )
    assert module.data_hub_loader.validate_parameters(parameters)["model_branch"] == "future/9.0.0"


def test_model_download_fetches_new_branches_and_latest_commits(model_repo, tmp_path, monkeypatch):
    module = load_loader_module()
    monkeypatch.setattr(module.tempfile, "tempdir", str(tmp_path))
    # Create the branch after the flow schema has already been generated.
    git(model_repo, "checkout", "-b", "2.5.0")
    model = model_repo / "model-desc" / "ins-model.yml"
    model.write_text("version: 2.5.0\n")
    git(model_repo, "commit", "-am", "New branch")

    first_schemas = module.data_model_download(model_repo.as_uri(), "2.5.0")
    assert Path(first_schemas[0]).read_text() == "version: 2.5.0\n"

    model.write_text("version: updated\n")
    git(model_repo, "commit", "-am", "Update remote branch")
    second_schemas = module.data_model_download(model_repo.as_uri(), "2.5.0")
    assert Path(second_schemas[0]).read_text() == "version: updated\n"
    assert first_schemas != second_schemas

    git(model_repo, "tag", "release-2.5.0")
    tag_schemas = module.data_model_download(model_repo.as_uri(), "release-2.5.0")
    assert Path(tag_schemas[0]).read_text() == "version: updated\n"


def test_missing_remote_branch_stops_loading(model_repo, tmp_path, monkeypatch):
    module = load_loader_module()
    monkeypatch.setattr(module.tempfile, "tempdir", str(tmp_path))

    with pytest.raises(subprocess.CalledProcessError):
        module.data_model_download(model_repo.as_uri(), "missing-branch")


def test_branch_without_schemas_stops_loading(model_repo, tmp_path, monkeypatch):
    module = load_loader_module()
    monkeypatch.setattr(module.tempfile, "tempdir", str(tmp_path))
    git(model_repo, "rm", "model-desc/ins-model.yml")
    git(model_repo, "commit", "-m", "Remove schemas")

    with pytest.raises(ValueError, match="No model YAML schemas found"):
        module.data_model_download(model_repo.as_uri(), "main")
