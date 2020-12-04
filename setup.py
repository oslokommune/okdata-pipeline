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
    install_requires=[
        "aws-xray-sdk",
        "boto3",
        "jsonschema",
        "okdata-aws>=0.2.0",
        "okdata-sdk>=0.5.1",
        "pandas<0.24",
        "python-dateutil",
        "requests",
        "xlrd",
    ],
)
