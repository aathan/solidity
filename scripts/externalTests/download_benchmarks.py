#!/usr/bin/env python3

from argparse import ArgumentParser, Namespace
from pathlib import Path
import sys

# Our scripts/ is not a proper Python package so we need to modify PYTHONPATH to import from it
# pragma pylint: disable=import-error,wrong-import-position
SCRIPTS_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from common.git_helpers import get_current_git_branch
from common.rest_api_helpers import CircleCI, Github, download_file
# pragma pylint: enable=import-error,wrong-import-position


class CommandLineError(Exception):
    pass


def process_commandline() -> Namespace:
    script_description = (
        "Downloads benchmark results attached as artifacts to the c_ext_benchmarks job on CircleCI. "
        "If no options are specified, downloads results for the currently checked out git branch."
    )

    parser = ArgumentParser(description=script_description)

    target_definition = parser.add_mutually_exclusive_group()
    target_definition.add_argument(
        '--branch',
        dest='branch',
        help="Git branch that the job ran on. Will fetch benchmark from the last run on that branch.",
    )
    target_definition.add_argument(
        '--pr',
        dest='pull_request_id',
        type=int,
        help="Github PR ID that the job ran on. Will fetch benchmark from the last run on that PR.",
    )
    target_definition.add_argument(
        '--base-of-pr',
        dest='base_of_pr',
        type=int,
        help=(
            "ID of a Github PR that's based on top of the branch we're interested in. "
            "Will fetch benchmark from the last run on that branch."
        )
    )

    parser.add_argument(
        '--debug-requests',
        dest='debug_requests',
        default=False,
        action='store_true',
        help="Print detailed info about performed API requests and received responses.",
    )
    parser.add_argument(
        '--overwrite',
        dest='overwrite',
        default=False,
        action='store_true',
        help="If artifacts already exist on disk, overwrite them.",
    )

    return parser.parse_args()


def download_benchmark(artifacts, benchmark_name: str, branch: str, commit_hash: str, overwrite: bool):
    print(f"Downloading artifact: {benchmark_name}-{branch}-{commit_hash[:8]}.json.")
    download_file(
        artifacts[f'reports/externalTests/{benchmark_name}.json']['url'],
        Path(f'{benchmark_name}-{branch}-{commit_hash[:8]}.json'),
        overwrite,
    )


def main():
    try:
        options = process_commandline()

        github = Github('ethereum/solidity', options.debug_requests)
        circleci = CircleCI('ethereum/solidity', options.debug_requests)

        branch = options.branch
        if options.branch is None and options.pull_request_id is None and options.base_of_pr is None:
            branch = get_current_git_branch()
        elif options.pull_request_id is not None:
            pr_info = github.pull_request(options.pull_request_id)
            branch = pr_info['head']['ref']
        elif options.base_of_pr is not None:
            pr_info = github.pull_request(options.base_of_pr)
            branch = pr_info['base']['ref']

        pipeline = circleci.latest_item(circleci.pipelines(branch))
        pipeline_id = pipeline['id']
        commit_hash = pipeline['vcs']['revision']
        workflow_id = circleci.latest_item(circleci.workflows(pipeline_id))['id']
        job_number = int(circleci.items_to_dict(
            'name',
            circleci.jobs(workflow_id)
        )['c_ext_benchmarks']['job_number'])
        artifacts = circleci.items_to_dict('path', circleci.artifacts(job_number))

        download_benchmark(artifacts, 'summarized-benchmarks', branch, commit_hash, options.overwrite)
        download_benchmark(artifacts, 'all-benchmarks', branch, commit_hash, options.overwrite)

        return 0
    except CommandLineError as exception:
        print(f"ERROR: {exception}", file=sys.stderr)
        return 1

if __name__ == '__main__':
    sys.exit(main())
