import sys

from airbyte_cdk.entrypoint import launch

from .source import SourceAppleAppStore


def run():
    source = SourceAppleAppStore()
    launch(source, sys.argv[1:])
