import subprocess


def get_current_git_branch():
    process = subprocess.run(
        ['git', 'symbolic-ref', 'HEAD', '--short'],
        encoding='utf8',
        capture_output=True,
        check=True,
    )
    return process.stdout.strip()
