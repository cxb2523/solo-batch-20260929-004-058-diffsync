"""Verification that the provided example code works correctly.

Copyright (c) 2020-2021 Network To Code, LLC <info@networktocode.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import os
import subprocess
import sys
from os.path import dirname, join

EXAMPLES = join(dirname(dirname(dirname(__file__))), "examples")
REPO_ROOT = dirname(dirname(dirname(__file__)))


def _run_example(main_path, work_dir):
    """Run an example script with the repo root on PYTHONPATH so diffsync imports."""
    env = dict(os.environ)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = REPO_ROOT + (os.pathsep + existing if existing else "")
    subprocess.run([sys.executable, main_path], cwd=work_dir, check=True, env=env)


def test_example_1():
    """Test that the "example1" script runs successfully."""
    example1_dir = join(EXAMPLES, "01-multiple-data-sources")
    example1_main = join(example1_dir, "main.py")
    # Run it and make sure it doesn't raise an exception or otherwise exit with a non-zero code.
    _run_example(example1_main, example1_dir)


def test_example_2():
    """Test that the "example2" script runs successfully."""
    example2_dir = join(EXAMPLES, "02-callback-function")
    example2_main = join(example2_dir, "main.py")
    # Run it and make sure it doesn't raise an exception or otherwise exit with a non-zero code.
    _run_example(example2_main, example2_dir)


def test_example_4():
    """Test that the "example4" script runs successfully."""
    example4_dir = join(EXAMPLES, "04-get-update-instantiate")
    example4_main = join(example4_dir, "main.py")
    # Run it and make sure it doesn't raise an exception or otherwise exit with a non-zero code.
    _run_example(example4_main, example4_dir)


def test_example_6():
    """Test that the "example6" script runs successfully."""
    example6_dir = join(EXAMPLES, "06-ip-prefixes")
    example6_main = join(example6_dir, "main.py")
    # Run it and make sure it doesn't raise an exception or otherwise exit with a non-zero code.
    _run_example(example6_main, example6_dir)
