import sys

from airbyte_cdk.entrypoint import launch

from .source import SourceExchangeRateCustom


def run():
    launch(SourceExchangeRateCustom(), sys.argv[1:])
