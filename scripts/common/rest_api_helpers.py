from pathlib import Path
from typing import Mapping, Optional
import json
import shutil

import requests


class FileAlreadyExists(Exception):
    pass


def query_api(url: str, params: Mapping[str, str], debug_requests=False) -> str:
    if debug_requests:
        print(f'REQUEST: {url}')

    response = requests.get(url, params=params)

    if debug_requests:
        json_response = response.json()
        print('========== RESPONSE ==========')
        if json_response is not None:
            print(json.dumps(json_response, indent=4))
        else:
            print(response.content)
        print('==============================')

    return response.json()


def download_file(url: str, target_path: Path, overwrite=False):
    if not overwrite and target_path.exists():
        raise FileAlreadyExists(f"Refusing to overwrite existing file: '{target_path}'.")

    with requests.get(url, stream=True) as request:
        with open(target_path, 'wb') as target_file:
            shutil.copyfileobj(request.raw, target_file)


class Github:
    BASE_URL = 'https://api.github.com'

    project_slug: str
    debug_requests: bool

    def __init__(self, project_slug: str, debug_requests: bool):
        self.project_slug = project_slug
        self.debug_requests = debug_requests

    def pull_request(self, pr_id: int) -> dict:
        return query_api(
            f'{self.BASE_URL}/repos/{self.project_slug}/pulls/{pr_id}',
            {},
            self.debug_requests
        )


class CircleCI:
    BASE_URL = 'https://circleci.com/api/v2'

    project_slug: str
    debug_requests: bool

    def __init__(self, project_slug: str, debug_requests: bool):
        self.project_slug = project_slug
        self.debug_requests = debug_requests

    def pipelines(self, branch: Optional[str] = None) -> dict:
        return query_api(
            f'{self.BASE_URL}/project/gh/{self.project_slug}/pipeline',
            {'branch': branch} if branch is not None else {},
            self.debug_requests,
        )

    def workflows(self, pipeline_id: str) -> dict:
        return query_api(
            f'{self.BASE_URL}/pipeline/{pipeline_id}/workflow',
            {},
            self.debug_requests,
        )

    def jobs(self, workflow_id: str) -> dict:
        return query_api(
            f'{self.BASE_URL}/workflow/{workflow_id}/job',
            {},
            self.debug_requests,
        )

    def artifacts(self, job_number: int) -> dict:
        return query_api(
            f'{self.BASE_URL}/project/gh/{self.project_slug}/{job_number}/artifacts',
            {},
            self.debug_requests,
        )

    @staticmethod
    def items_to_dict(key: str, paginated_json: dict) -> dict:
        return {
            item[key]: item
            for item in paginated_json['items']
        }

    @staticmethod
    def latest_item(paginated_json: dict) -> dict:
        sorted_items = sorted(paginated_json['items'], key=lambda item: item['created_at'])
        return sorted_items[0] if len(sorted_items) > 0 else None
