#!/usr/bin/env python3
'''Command line tool to rehydrate or dehydrate an entire container'''
import argparse
import os
import threading
import queue

from azure.storage.blob import StandardBlobTier, RehydratePriority
from dotenv import load_dotenv, find_dotenv
import prettytable

from azure_client import azure_client
from azure_client.logger import log, formatter

PARSER = argparse.ArgumentParser(description='Rehydrate/dehydrate an archive blob')
PARSER.add_argument('--container', '-c', required=True)
PARSER.add_argument('--tier', '-t', default='Cool')
PARSER.add_argument('--priority', '-p', default='Standard')
PARSER.add_argument('--workers', '-w', default=1, type=int)
ARGS = PARSER.parse_args()

load_dotenv(find_dotenv())

AZURE_URL, AZURE_KEY = os.getenv("AZURE_URL"), os.getenv("AZURE_KEY")
SERVICE = azure_client.connect_service(AZURE_URL, AZURE_KEY)
CONTAINER = azure_client.connect_container(SERVICE, ARGS.container, create=False)
BLOB_FILENAMES = azure_client.get_blob_manifest(CONTAINER)

q = queue.Queue()

def worker():
    while True:
        try:
            file = q.get()
            BLOB = CONTAINER.get_blob_client(file)
            BLOB_INFO = BLOB.get_blob_properties()
            if BLOB_INFO.blob_tier == ARGS.tier:
                log.info(f'File {file} is already at tier {ARGS.tier}')
                q.task_done()
            elif BLOB_INFO.archive_status == f'rehydrate-pending-to-{ARGS.tier.lower()}':
                log.info(f'File {file} is already pending rehydration to {ARGS.tier}')
                q.task_done()
            else:
                azure_client.set_blob_tier(BLOB, StandardBlobTier(ARGS.tier), RehydratePriority(ARGS.priority))
                q.task_done()
        except Exception as e:
            if e.error_code == 'BlobBeingRehydrated':
                log.info(f'File {file} is already in the middle of rehydration')
                q.task_done()
                continue
            log.error('File: %s', file)
            log.error('Exception!', exc_info=e)
            # log.error('Replacing file in queue.')
            # q.put(file)

for _ in range(ARGS.workers):
    threading.Thread(target=worker, daemon=True).start()

for azure_filename in BLOB_FILENAMES:
    q.put(azure_filename)

q.join()
