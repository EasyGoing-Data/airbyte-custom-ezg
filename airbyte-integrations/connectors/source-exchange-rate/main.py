import sys
from source_exchange_rate import SourceExchangeRate
from airbyte_cdk.entrypoint import launch

if __name__ == "__main__":
    launch(SourceExchangeRate(), sys.argv[1:])
