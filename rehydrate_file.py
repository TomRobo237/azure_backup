#!/usr/bin/env python3
'''Command line tool to list container and files within'''
import argparse
import os

from azure.storage.blob import StandardBlobTier, RehydratePriority
from dotenv import load_dotenv, find_dotenv
import prettytable

from azure_client import azure_client
from azure_client.logger import log, formatter

PARSER = argparse.ArgumentParser(description='Rehydrate/dehydrate an archive blob')
PARSER.add_argument('--container', '-c', required=True)
PARSER.add_argument('--tier', '-t', default='Cool')
PARSER.add_argument('--filename', '-f', required=True)
PARSER.add_argument('--priority', '-p', default='Standard')
ARGS = PARSER.parse_args()

load_dotenv(find_dotenv())

AZURE_URL, AZURE_KEY = os.getenv("AZURE_URL"), os.getenv("AZURE_KEY")
SERVICE = azure_client.connect_service(AZURE_URL, AZURE_KEY)
CONTAINER = azure_client.connect_container(SERVICE, ARGS.container, create=False)
BLOB = CONTAINER.get_blob_client(ARGS.filename)

if not BLOB.exists():
    log.error(f'File {ARGS.filename} does not exist in {ARGS.container}')
    exit(1)

BLOB_INFO = BLOB.get_blob_properties()
if BLOB_INFO.blob_tier == ARGS.tier:
    log.info(f'File {ARGS.filename} is already at tier {ARGS.tier}')
    exit(0)
if BLOB_INFO.archive_status == f'rehydrate-pending-to-{ARGS.tier.lower()}':
    log.info(f'File {ARGS.filename} is already pending rehydration to {ARGS.tier}')
    exit(0)

azure_client.set_blob_tier(BLOB, StandardBlobTier(ARGS.tier), RehydratePriority(ARGS.priority))
