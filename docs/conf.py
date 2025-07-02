"""Sphinx configuration."""

project = "Gridworks Proactor"
author = "Jessica Millar"
copyright = "2023, Jessica Millar"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_click",
    "myst_parser",
    "sphinxcontrib.mermaid",
]
autodoc_typehints = "description"
html_theme = "furo"
myst_heading_anchors = 7
