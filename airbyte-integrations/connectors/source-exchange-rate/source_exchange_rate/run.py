import sys
from airbyte_cdk.entrypoint import launch
from .source import SourceExchangeRate

def run():
    launch(SourceExchangeRate(), sys.argv[1:])
