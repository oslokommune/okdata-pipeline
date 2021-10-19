# okdata-pipeline

Collection of pipeline components for [Origo
Dataplattform](https://oslokommune.github.io/dataplattform/).

## Components

- converters
  - [csv](doc/converters/csv.md)
  - [xls](doc/converters/xls.md)
- [lambda_invoker](doc/lambda_invoker.md)
- validators
  - [csv](doc/validators/csv.md)
  - [json](doc/validators/json.md)
- writers
  - [kinesis](doc/writers/kinesis.md)
  - [s3](doc/writers/s3.md)

## Setup

1. Install [Serverless Framework](https://serverless.com/framework/docs/getting-started/)
2. python3.7 -m venv .venv
3. Install Serverless plugins: `make init`
4. Install Python toolchain: `python3 -m pip install (--user) tox black pip-tools`
   - If running with `--user` flag, add `$HOME/.local/bin` to `$PATH`

## Formatting code

Code is formatted using [black](https://pypi.org/project/black/): `make format`.

## Running tests

Tests are run using [tox](https://pypi.org/project/tox/): `make test`.

For tests and linting we use [pytest](https://pypi.org/project/pytest/),
[flake8](https://pypi.org/project/flake8/), and
[black](https://pypi.org/project/black/).

## Deploy

Deploy to both dev and prod is automatic via GitHub Actions on push to
`master`. You can alternatively deploy from local machine (requires `saml2aws`)
with: `make deploy` or `make deploy-prod`.
