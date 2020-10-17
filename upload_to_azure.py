#!/usr/bin/env python3
import os
import hashlib

from azure_client import azure_client
from azure.storage.blob import BlobServiceClient, StandardBlobTier
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
AZURE_URL, AZURE_KEY = os.getenv("AZURE_URL"), os.getenv("AZURE_KEY")

class args: # TODO: Create parser! This is a placeholder to ensure it works.
    container = 'test'
    filename = 'testing/myfile2.txt'
    folder = ''
    overwrite = True
    tier = StandardBlobTier('Archive')

service = azure_client.connect_service(AZURE_URL, AZURE_KEY)
container = azure_client.connect_container(service ,args.container)

blob_filenames = azure_client.get_blob_manifest(container)

if args.filename in blob_filenames:
    azure_client.upload_blob(container, args.filename, args.tier, update=True, overwrite=args.overwrite)
else:
    azure_client.upload_blob(container, args.filename, args.tier)

# TODO: Catch errors from service.
# TODO: Create options parser.
# TODO: Create folder uploader. Probably in a different script.
