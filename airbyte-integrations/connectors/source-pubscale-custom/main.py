#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import sys

from airbyte_cdk.entrypoint import launch
from source_pubscale import SourcePubscale

if __name__ == "__main__":
    source = SourcePubscale()
    launch(source, sys.argv[1:])
