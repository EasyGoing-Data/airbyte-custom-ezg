import sys
from airbyte_cdk.entrypoint import launch
from .source import SourceGoogleAnalytics


def run():
    launch(SourceGoogleAnalytics(), sys.argv[1:])
