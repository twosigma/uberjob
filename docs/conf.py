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
import inspect

import sphinx_autodoc_typehints

from uberjob import __version__ as version

original_format_annotation = sphinx_autodoc_typehints.format_annotation


def format_annotation(annotation, *args, **kwargs):
    original = original_format_annotation(annotation, *args, **kwargs)
    if inspect.isclass(annotation):
        full_name = f"{annotation.__module__}.{annotation.__qualname__}"
        package_name, *_, class_name = full_name.split(".")
        if package_name in ["networkx", "uberjob"]:
            modified = f":py:class:`~{package_name}.{class_name}`"
            print(f"Rewrite: {original} => {modified}")
            return modified
    return original


sphinx_autodoc_typehints.format_annotation = format_annotation

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx_autodoc_typehints",
    "releases",
]
source_suffix = ".rst"
master_doc = "index"
project = "uberjob"
author = "Daniel Shields, Timothy Shields"
copyright = "2020, Two Sigma Open Source, LLC"
release = version
version = ".".join(version.split(".")[:2])
language = None
exclude_patterns = ["_build"]
pygments_style = "sphinx"
html_theme = "sphinx_rtd_theme"
html_theme_options = {
    "style_nav_header_background": "#8e6ca6",
}
html_context = {"logo": "logo/logo-128.png", "theme_logo_only": False}
html_favicon = "_static/logo/logo-32.png"
html_static_path = ["_static"]
autodoc_member_order = "bysource"
intersphinx_mapping = {
    "python": ("https://docs.python.org/3.6", None),
    "networkx": ("https://networkx.github.io/documentation/stable/", None),
}
releases_github_path = "twosigma/uberjob"
releases_unstable_prehistory = True
