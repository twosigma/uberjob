#
# Copyright 2020 Two Sigma Open Source, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import tempfile

import nox

SOURCES = "src", "tests", "noxfile.py", "docs/conf.py"

nox.options.sessions = "lint", "tests", "docs"


def install_with_constraints(session, *args, **kwargs):
    with tempfile.TemporaryDirectory() as temp:
        requirements = os.path.join(temp, "requirements.txt")
        session.run(
            "poetry",
            "export",
            "--with",
            "dev",
            "--without-hashes",
            "--format=requirements.txt",
            f"--output={requirements}",
            external=True,
        )
        session.install(f"--constraint={requirements}", *args, **kwargs)


@nox.session()
def black(session):
    """Run black code formatter."""
    args = session.posargs or SOURCES
    session.run("isort", *args)
    session.run("black", *args)


@nox.session()
def lint(session):
    """Lint using flake8."""
    args = session.posargs or SOURCES
    session.run("poetry", "install", external=True)
    session.run("poetry", "run", "flake8", *args, external=True)


@nox.session()
def tests(session):
    """Run the test suite."""
    args = session.posargs or ["--cov"]
    session.run("poetry", "install", external=True)
    session.run("poetry", "run", "pytest", *args, external=True)


@nox.session()
def coverage(session):
    """Upload coverage data."""
    session.run("coverage", "xml", "--fail-under=0")
    session.run("codecov", *session.posargs)


@nox.session()
def docs(session):
    """Build the documentation."""
    session.run("poetry", "install", external=True)
    session.run("python", "-m", "docs")
    session.run("sphinx-build", "docs", "docs/_build")
