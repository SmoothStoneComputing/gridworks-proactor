# Contributor Guide

Thank you for your interest in improving this project.
This project is open-source under the [MIT license] and
welcomes contributions in the form of bug reports, feature requests, and pull requests.

Here is a list of important resources for contributors:

- [Source Code]
- [Documentation]
- [Issue Tracker]
- [Code of Conduct]

[mit license]: https://opensource.org/licenses/MIT
[source code]: https://github.com/SmoothStoneComputing/gridworks-proactor
[documentation]: https://gridworks-proactor.readthedocs.io/
[issue tracker]: https://github.com/SmoothStoneComputing/gridworks-proactor/issues

## How to report a bug

Report bugs on the [Issue Tracker].

When filing an issue, make sure to answer these questions:

- Which operating system and Python version are you using?
- Which version of this project are you using?
- What did you do?
- What did you expect to see?
- What did you see instead?

The best way to get your bug fixed is to provide a test case,
and/or steps to reproduce the issue.

## How to request a feature

Request features on the [Issue Tracker].

## How to set up your development environment

You need [uv], which can be installed from [here].

Install [pre-commit] into the repository with:

```console
uv run --all-extras pre-commit install
```

The development environment can be explicitly created or brought up to date
with:

```console
uv sync --all-extras
```

The created environment can be explicitly activated with:
```console
source .venv/bin/activate
```

## How to test the project
Unit tests are located in the _tests_ directory, and are written using the
[pytest] testing framework. You can run the tests with:
```console
uv run pytest
```

The full test suite is run with [nox]. You can run nox without installing it
with:
```console
uv run nox
```

Or you can install nox as a tool with:
```
uv tool install --with nox-uv nox
```

Or you can run nox as an ephemeral tool with:
```console
uvx --with nox-uv nox
```

To run the full test suite:
```console
nox
```

List the available Nox sessions:

```console
nox -l
```

You can also run a specific Nox session.
For example, invoke the unit test suite like this:

```console
nox -s tests
```


[pytest]: https://pytest.readthedocs.io/

## Linting
This projects uses [ruff check] and [ruff format], as configured in the
[pyproject.toml], and runs them on every commit via [pre-commit].

You can run them explicitly with:
```console
uv run ruff check
```
and:
```console
uv run ruff format
```

## Making changes
Make your changes on a branch.

Because this project uses pre-commit hooks installed into the development
environment, pre-commit may fail if `git commit` is not run from within the
environment. To commit changes to your branch run:

```console
uv run git commit ...
```

or run `git commit` from within the activated development environment.


## How to submit changes


Open a [pull request] to submit changes to this project.

Your pull request needs to meet the following guidelines for acceptance:

- Code changes must pass pre-commit.
- The Nox test suite must pass without errors and warnings.
- Include unit tests.
- If your changes add functionality, update the documentation accordingly.

Feel free to submit early, though—we can always iterate on this.

To run linting and code formatting checks before committing your change run:

```console
uv run pre-commit
```

It is recommended to open an issue before starting work on anything. This
will allow a chance to talk it over with the owners and validate your
approach.

[pull request]: https://github.com/thegridelectric/gridworks-proactor/pulls
[uv]: https://docs.astral.sh/uv/
[here]: https://docs.astral.sh/uv/getting-started/installation/#standalone-installer
[nox]: https://nox.thea.codes/
[nox-uv]: https://pypi.org/project/nox-uv/
[ruff check]: https://docs.astral.sh/ruff/linter/#ruff-check
[ruff format]: https://docs.astral.sh/ruff/formatter/
[pyproject.toml]: https://github.com/SmoothStoneComputing/gridworks-proactor/blob/main/pyproject.toml

<!-- github-only -->

[code of conduct]: CODE_OF_CONDUCT.md
