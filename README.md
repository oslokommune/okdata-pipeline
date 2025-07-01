# okdata-pipeline

Collection of pipeline components for [Origo
Dataplattform](https://oslokommune.github.io/dataplattform/).

## Components

- converters
  - [csv](doc/converters/csv.md)
  - [json](doc/converters/json.md)
  - [xls](doc/converters/xls.md)
- [lambda_invoker](doc/lambda_invoker.md)
- validators
  - [csv](doc/validators/csv.md)
  - [json](doc/validators/json.md)
- writers
  - [s3](doc/writers/s3.md)

## Setup

```sh
make init
```

## Test

Tests are run using [tox](https://pypi.org/project/tox/):

```sh
make test
```

For tests and linting we use [pytest](https://pypi.org/project/pytest/),
[flake8](https://pypi.org/project/flake8/), and
[black](https://pypi.org/project/black/).

## Deploy

Example GitHub Actions for deploying to dev and prod on push to `main` is
included in `.github/workflows`.

You can also deploy from a local machine to dev with:

```sh
make deploy
```

Or to prod with:

```sh
make deploy-prod
```
