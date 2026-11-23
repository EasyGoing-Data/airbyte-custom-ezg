#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import sys

from airbyte_cdk.entrypoint import launch
from source_liftoff import SourceLiftoff

if __name__ == "__main__":
    source = SourceLiftoff()
    launch(source, sys.argv[1:])
