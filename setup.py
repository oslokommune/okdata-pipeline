import os
import setuptools

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

service_name = os.path.basename(os.getcwd())

setuptools.setup(
    name=service_name,
    version="0.1.0",
    author="Origo Dataplattform",
    author_email="dataplattform@oslo.kommune.no",
    description="Origo Dataplattform pipeline components",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/oslokommune/okdata-pipeline",
    packages=setuptools.find_namespace_packages(
        include="okdata.pipeline.*", exclude=["test*"]
    ),
    namespace_packages=["okdata"],
    install_requires=[
        "aws-xray-sdk",
        "boto3",
        "fastparquet",
        "jsonschema",
        "okdata-aws>=0.4.1",
        "okdata-sdk>=0.8.1",
        "pandas",
        "python-dateutil",
        "requests",
        # Newer versions of s3fs cause dependency resolution issues:
        # https://github.com/dask/s3fs/issues/357
        "s3fs<=0.4.2",
        # Newer versions drop support for newer Excel files (.xlsx)
        "xlrd<2.0",
    ],
)
