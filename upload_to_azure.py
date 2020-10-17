#!/usr/bin/env python3
import os
import hashlib

from azure.storage.blob import BlobServiceClient, StandardBlobTier
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
AZURE_URL, AZURE_KEY = os.getenv("AZURE_URL"), os.getenv("AZURE_KEY")

class args: # TODO: Create parser! This is a placeholder to ensure it works.
    container = 'test'
    filename = 'testing/myfile1.txt'
    folder = ''
    overwrite = True
    tier = StandardBlobTier('Archive')

service = BlobServiceClient(account_url=AZURE_URL, credential=AZURE_KEY)

# Parse options for container name to upload, compare to list to see if container needs to be created
container_list = [x for x in service.list_containers()]
container_names = [x['name'] for x in container_list]

container_client = service.get_container_client(args.container)

if args.container not in container_names: # Meaning no container setup yet.
    operation = container_client.create_container()
    if operation['error_code'] is not None:
        raise Exception(operation['error_code'])
    else:
        print(f"{operation['date']}: Created container {args.container}, request_id: {operation['request_id']}.")

blob_list = [x for x in container_client.list_blobs()]
blob_filenames = [x['name'] for x in blob_list]

file_md5 = hashlib.md5(open(args.filename, 'rb').read()).hexdigest()

blob_client = container_client.get_blob_client(args.filename)

if args.filename in blob_filenames:
    blob_properties = blob_client.get_blob_properties()
    try:
        blob_md5 = blob_properties['metadata']['md5']
    except KeyError:
        blob_md5 = ''

    print(f"{args.filename} already in container. cloud md5: {blob_md5}, local md5: {file_md5}")
    if file_md5 != blob_md5:
        print(f'MD5sum Mismatch: Sending local copy of {args.filename}')
        if args.overwrite:
            blob_client.delete_blob()
            blob_client.upload_blob(
                open(args.filename, 'rb').read(),
                standard_blob_tier=args.tier,
                metadata={'md5': file_md5}
            )
        else:
            print(f'Set not to overwrite. Will not send {args.filename}')

else:
    print(f'Not found in container, sending local file {args.filename}')
    blob_client.upload_blob(
        open(args.filename, 'rb').read(),
        standard_blob_tier=args.tier,
        metadata={'md5': file_md5}
    )

# TODO: Catch errors from service.
# TODO: Create options parser.
# TODO: Create folder uploader. Probably in a different script.
