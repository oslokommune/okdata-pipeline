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
        "awswrangler[deltalake]",
        "aws-xray-sdk",
        "boto3",
        "jsonschema",
        "okdata-aws>=4.1",
        "okdata-sdk>=2.1.0",
        "openpyxl",
        "pandas>2,<3",
        "python-dateutil",
        "requests",
    ],
    python_requires="==3.13.*",
)
