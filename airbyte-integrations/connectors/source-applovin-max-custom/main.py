#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import sys

from airbyte_cdk.entrypoint import launch
from source_applovin_max import SourceApplovinMax

if __name__ == "__main__":
    source = SourceApplovinMax()
    launch(source, sys.argv[1:])
