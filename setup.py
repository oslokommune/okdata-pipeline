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
        # Lock to Lambda runtime version
        "boto3==1.26.90",
        # Transitive dependency, lock to Lambda runtime version
        "botocore==1.29.90",
        "fastparquet",
        # Transitive dependency, lock to Lambda runtime version
        "jmespath==1.0.1",
        "jsonschema",
        "okdata-aws>=0.4.1",
        "okdata-sdk>=0.8.1",
        "pandas",
        # Lock to Lambda runtime version
        "python-dateutil==2.8.2",
        "requests",
        # Newer versions of s3fs cause dependency resolution issues:
        # https://github.com/dask/s3fs/issues/357
        "s3fs<=0.4.2",
        # Transitive dependency, lock to Lambda runtime version
        "s3transfer==0.6.0",
        # Transitive dependency, lock to Lambda runtime version
        "six==1.16.0",
        # Transitive dependency, lock to Lambda runtime version
        "urllib3==1.26.11",
        # Newer versions drop support for newer Excel files (.xlsx)
        "xlrd<2.0",
    ],
)
