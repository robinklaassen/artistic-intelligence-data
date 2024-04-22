import os
import subprocess
import sys
from typing import Mapping

from SCons.Script import COMMAND_LINE_TARGETS

_SUBJECT = "artistic_intelligence_data"
_TEST_SUBJECT = "tests"

# Remember if a target has been found, to warn if not.
_target_found: bool = False


def _exec(command: str, env: Mapping | None = None) -> int:
    global _target_found  # noqa: PLW0603 yes, global, I know
    _target_found = True

    print(f">>> {command}")

    exit_code = subprocess.call(command, shell=True, env=env)
    if exit_code != 0:
        print(f"Exiting with {exit_code}")
        sys.exit(exit_code)
    return exit_code


# Generic targets, which will extend the COMMAND_LINE_TARGETS array.
# Makes sure that generic commands run the tests last, as they are the most likely to fail.
if "all" in COMMAND_LINE_TARGETS:
    COMMAND_LINE_TARGETS += ["fix", "format", "quality"]

if "quality" in COMMAND_LINE_TARGETS:
    COMMAND_LINE_TARGETS += ["lint", "lock", "type", "test", "sec"]

# Individual targets
# Fixing
if "fix" in COMMAND_LINE_TARGETS:
    cmd = f"ruff check SConstruct.py {_SUBJECT} {_TEST_SUBJECT} --fix --show-fixes"
    _exec(cmd)

# Formatting
if "format" in COMMAND_LINE_TARGETS:
    cmd = f"ruff format SConstruct.py {_SUBJECT} {_TEST_SUBJECT}"
    _exec(cmd)

# Quality - linting
if "lint" in COMMAND_LINE_TARGETS:
    cmd = f"ruff check SConstruct.py {_SUBJECT} {_TEST_SUBJECT}"
    _exec(cmd)

# Quality - lock file integrity
if "lock" in COMMAND_LINE_TARGETS:
    _exec("poetry check --lock")

# Quality - static type checking
if "type" in COMMAND_LINE_TARGETS:
    _exec(f"mypy {_SUBJECT}", env=os.environ)

# Quality - unit tests
if "test" in COMMAND_LINE_TARGETS:
    _exec(f"python -m pytest {_TEST_SUBJECT}", env=os.environ)

# Quality - security
if "sec" in COMMAND_LINE_TARGETS:
    _exec(f"bandit -r {_SUBJECT} -ll -iii", env=os.environ)

if not _target_found:
    print(f"No valid target in {COMMAND_LINE_TARGETS}. Look at SConstruct.py for what is allowed.")

sys.exit(0)
