#!/usr/bin/env python3
'''Simple command line tool to download a single file from a container'''
import argparse
import os
import threading
import queue

from azure.storage.blob import StandardBlobTier
from dotenv import load_dotenv, find_dotenv

from azure_client import azure_client
from azure_client.logger import log, formatter

load_dotenv(find_dotenv())
AZURE_URL, AZURE_KEY = os.getenv("AZURE_URL"), os.getenv("AZURE_KEY")

PARSER = argparse.ArgumentParser(description='Download a file from your Azure storage')
PARSER.add_argument('--container', '-c', required=True)
PARSER.add_argument('--destination', '-d', required=True)
PARSER.add_argument('--overwrite', '-o', action='store_false')
PARSER.add_argument('--workers', '-w', default=1, type=int)
ARGS = PARSER.parse_args()

SERVICE = azure_client.connect_service(AZURE_URL, AZURE_KEY)
CONTAINER = azure_client.connect_container(SERVICE, ARGS.container)

BLOB_FILENAMES = azure_client.get_blob_manifest(CONTAINER)

q = queue.Queue()

def worker():
    while True:
        try:
            file = q.get()
            BLOB = CONTAINER.get_blob_client(file)
            BLOB_INFO = BLOB.get_blob_properties()
            if BLOB_INFO.blob_tier not in ['Hot', 'Cool']:
                log.error(f'{ARGS.filename} is not a tier that can be downloaded. Currently {BLOB_INFO.blob_tier}')
                q.task_done()
            else:
                azure_client.download_blob(BLOB, BLOB_INFO, ARGS.destination, ARGS.overwrite)
                q.task_done()
        except Exception as e:
            log.error('File: %s', file)
            log.error('Exception!', exc_info=e)
            q.task_done()
            continue

for _ in range(ARGS.workers):
    threading.Thread(target=worker, daemon=True).start()

for file in BLOB_FILENAMES:
    q.put(file)

q.join()
