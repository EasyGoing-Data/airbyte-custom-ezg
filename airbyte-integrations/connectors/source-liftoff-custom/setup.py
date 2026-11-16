#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk==0.51.1",
    "pydantic~=1.9.2",
    "jsonschema~=3.2.0",
    "jsonref~=0.2",
    "dpath~=2.0.1",
    "pendulum==3.2.0",
    "requests==2.32.5",
]

TEST_REQUIREMENTS = []

setup(
    name="source_liftoff",
    description="Source implementation for Liftoff Reporting API.",
    author="EasyGoing Data",
    author_email="data@easygoing.vn",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json", "*.yaml", "schemas/*.json", "schemas/shared/*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
