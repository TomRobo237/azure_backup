#!/usr/bin/env python3
'''Simple command line tool to upload a single file to a azure container'''
import argparse
import os

from azure.storage.blob import StandardBlobTier
from dotenv import load_dotenv, find_dotenv

from azure_client import azure_client

load_dotenv(find_dotenv())
AZURE_URL, AZURE_KEY = os.getenv("AZURE_URL"), os.getenv("AZURE_KEY")

PARSER = argparse.ArgumentParser(description='Upload a file to your Azure storage')
PARSER.add_argument('--container', '-c')
PARSER.add_argument('--filename', '-f')
PARSER.add_argument('--overwrite', '-o', action='store_true')
PARSER.add_argument('--tier', '-t')
ARGS = PARSER.parse_args()

SERVICE = azure_client.connect_service(AZURE_URL, AZURE_KEY)
CONTAINER = azure_client.connect_container(SERVICE, ARGS.container)

BLOB_FILENAMES = azure_client.get_blob_manifest(CONTAINER)

if ARGS.filename in BLOB_FILENAMES:
    op = azure_client.upload_blob(
        CONTAINER,
        ARGS.filename,
        ARGS.filename,
        StandardBlobTier(ARGS.tier),
        update=True,
        overwrite=ARGS.overwrite
    )
else:
    op = azure_client.upload_blob(CONTAINER, ARGS.filename, ARGS.filename, StandardBlobTier(ARGS.tier))
print(op)

# TODO: this may not work with absoulte filepaths correctly!
