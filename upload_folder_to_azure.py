#!/usr/bin/env python3
'''Simple command line tool to upload a single file to a azure container'''
import argparse
from glob import glob, iglob
import os
import pathlib
import threading
import queue
import logging

from azure.storage.blob import StandardBlobTier
from dotenv import load_dotenv, find_dotenv

from azure_client import azure_client
from azure_client.logger import log, formatter

load_dotenv(find_dotenv())
AZURE_URL, AZURE_KEY = os.getenv("AZURE_URL"), os.getenv("AZURE_KEY")

PARSER = argparse.ArgumentParser(description='Upload a file to your Azure storage')
PARSER.add_argument('--container', '-c', required=True)
PARSER.add_argument('--folder', '-f', required=True )
PARSER.add_argument('--overwrite', '-o', action='store_true')
PARSER.add_argument('--tier', '-t', default='Archive')
PARSER.add_argument('--strip-base-folder', '-s', action='store_true')
PARSER.add_argument('--workers', '-w', default=0, type=int)
PARSER.add_argument('--debug', '-d', action='store_true')
PARSER.add_argument('--logfile', '-l')
ARGS = PARSER.parse_args()

if ARGS.logfile:
    handler = logging.FileHandler(ARGS.logfile)
    handler.setFormatter(formatter)
    log.addHandler(handler)

SERVICE = azure_client.connect_service(AZURE_URL, AZURE_KEY)
CONTAINER = azure_client.connect_container(SERVICE, ARGS.container)

BLOB_FILENAMES = azure_client.get_blob_manifest(CONTAINER)

# Getting the folders filenames and stripping absoulte paths for upload to azure, 
# cutting of first folder name if flagged. Added this flag since the container might be named the same as the folder
# and then it would be silly to have a container named like movies with the only folder being movies in it.
ABS_FOLDER = pathlib.Path(ARGS.folder)
BASE_ABS_FOLDER = pathlib.Path(ARGS.folder).parents[0]

file_list = [z for z in [ y for x in os.walk(ARGS.folder) for y in glob(os.path.join(x[0], '*'))] if not os.path.isdir(z)]

if ARGS.folder.startswith('/'):
    FINAL_FOLDER = str(pathlib.Path(ARGS.folder)).split(str(BASE_ABS_FOLDER) + os.sep)[1]
    azure_filename_list = [ x.split(str(BASE_ABS_FOLDER) + os.sep)[-1] for x in file_list]
    if ARGS.strip_base_folder:
        azure_filename_list = [ x.split(str(FINAL_FOLDER) + os.sep)[-1] for x in azure_filename_list ]
else:
    FINAL_FOLDER = ARGS.folder
    if ARGS.strip_base_folder:
        azure_filename_list = [ x.split(str(FINAL_FOLDER) + os.sep)[-1] for x in file_list]
    else:
        azure_filename_list = [ x.split(str(BASE_ABS_FOLDER) + os.sep)[-1] for x in file_list]


# Actually doing the upload
if ARGS.workers <= 1: # FIXME: make 1 thread if not set.
    for filename, azure_filename in zip(file_list, azure_filename_list):
        if azure_filename in BLOB_FILENAMES:
            azure_client.upload_blob(
                CONTAINER,
                filename,
                azure_filename,
                StandardBlobTier(ARGS.tier),
                update=True,
                overwrite=ARGS.overwrite,
                debug=ARGS.debug
            )
        else:
            azure_client.upload_blob(
                CONTAINER,
                filename,
                azure_filename,
                StandardBlobTier(ARGS.tier),
                debug=ARGS.debug
            )
elif ARGS.workers > 1:
    q = queue.Queue()

    def worker():
        while True:
            files = q.get()
            filename, azure_filename = files
            if azure_filename in BLOB_FILENAMES:
                args = [
                    CONTAINER,
                    filename,
                    azure_filename,
                    StandardBlobTier(ARGS.tier),
                    True,
                    ARGS.overwrite,
                    ARGS.debug
                ]
                kwargs = {}
            else:
                args = [CONTAINER, filename, azure_filename, StandardBlobTier(ARGS.tier)]
                kwargs = {'debug': ARGS.debug}
            azure_client.upload_blob(*args, **kwargs)
            q.task_done()

    for i in range(ARGS.workers):
        threading.Thread(target=worker, daemon=True).start()

    for filename, azure_filename in zip(file_list, azure_filename_list):
        files = (filename, azure_filename)
        q.put(files)

    q.join()
