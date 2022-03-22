from pathlib import Path
from typing import Mapping, Optional
import functools
import json
import operator
import shutil

import requests


class APIHelperError(Exception):
    pass

class FileAlreadyExists(APIHelperError):
    pass


def query_api(url: str, params: Mapping[str, str], debug_requests=False) -> str:
    if debug_requests:
        print(f'REQUEST: {url}')

    response = requests.get(url, params=params)
    response.raise_for_status()

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
    # None might be a more logical default for max_pages but in most cases we'll actually
    # want some limit to prevent flooding the API with requests in case of a bug.
    DEFAULT_MAX_PAGES = 10
    BASE_URL = 'https://circleci.com/api/v2'

    project_slug: str
    debug_requests: bool

    def __init__(self, project_slug: str, debug_requests: bool):
        self.project_slug = project_slug
        self.debug_requests = debug_requests

    def paginated_query_api_iterator(self, url: str, params: Mapping[str, str], max_pages: int=DEFAULT_MAX_PAGES):

        assert 'page-token' not in params

        page_count = 0
        next_page_token = None
        while max_pages is None or page_count < max_pages:
            if next_page_token is not None:
                params = {**params, 'page-token': next_page_token}

            response = query_api(url, params, self.debug_requests)

            yield response['items']
            next_page_token = response['next_page_token']
            page_count += 1
            if next_page_token is None:
                break

    def paginated_query_api(self, url: str, params: Mapping[str, str], max_pages: int=DEFAULT_MAX_PAGES):
        return functools.reduce(operator.add, self.paginated_query_api_iterator(url, params, max_pages), [])

    def pipelines(self, branch: Optional[str] = None) -> dict:
        return self.paginated_query_api(
            f'{self.BASE_URL}/project/gh/{self.project_slug}/pipeline',
            {'branch': branch} if branch is not None else {},
            max_pages=1,
        )

    def workflows(self, pipeline_id: str) -> dict:
        return self.paginated_query_api(f'{self.BASE_URL}/pipeline/{pipeline_id}/workflow', {})

    def jobs(self, workflow_id: str) -> dict:
        return self.paginated_query_api(f'{self.BASE_URL}/workflow/{workflow_id}/job', {})

    def artifacts(self, job_number: int) -> dict:
        return self.paginated_query_api(f'{self.BASE_URL}/project/gh/{self.project_slug}/{job_number}/artifacts', {})

    @staticmethod
    def latest_item(items: dict) -> dict:
        sorted_items = sorted(items, key=lambda item: item['created_at'])
        return sorted_items[0] if len(sorted_items) > 0 else None
