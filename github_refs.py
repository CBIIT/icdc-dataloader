import logging

import requests


log = logging.getLogger(__name__)


def get_github_refs(repo_url, include_tags=False):
    if repo_url.endswith(".git"):
        repo_url = repo_url[:-4]

    parts = repo_url.rstrip("/").split("/")
    owner, repo = parts[-2], parts[-1]
    refs = []
    ref_types = ["branches", "tags"] if include_tags else ["branches"]

    for ref_type in ref_types:
        page = 1
        while True:
            api_url = (
                f"https://api.github.com/repos/{owner}/{repo}/{ref_type}"
                f"?per_page=100&page={page}"
            )
            try:
                response = requests.get(api_url, timeout=30)
                response.raise_for_status()
                data = response.json()
                if not data:
                    break
                refs.extend(item["name"] for item in data)
                if len(data) < 100:
                    break
                page += 1
            except Exception as error:
                log.error("Error fetching %s from GitHub: %s", ref_type, error)
                break

    return list(dict.fromkeys(refs))


def get_github_branches(repo_url):
    return get_github_refs(repo_url)
