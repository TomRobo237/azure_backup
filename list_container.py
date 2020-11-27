#!/usr/bin/env python3
'''Command line tool to list container and files within'''
import os
import sys
import textwrap

from dotenv import load_dotenv, find_dotenv
import prettytable

from azure_client import azure_client

def human_readable(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

if len(sys.argv) != 2:
    print(f'Usage: {__file__} container_name\n\nWill list all files in container "container_name" with tier and size')
    exit(1)

load_dotenv(find_dotenv())

AZURE_URL, AZURE_KEY = os.getenv("AZURE_URL"), os.getenv("AZURE_KEY")
SERVICE = azure_client.connect_service(AZURE_URL, AZURE_KEY)
CONTAINER = azure_client.connect_container(SERVICE, sys.argv[1], create=False)
BLOB_INFO = azure_client.get_blob_list_information(CONTAINER)

TABLE = prettytable.PrettyTable()
TABLE.field_names = ['Filename', 'Tier', 'Size']
TABLE.align = 'l'

total_size = 0

for fn, bt, sz in BLOB_INFO:
    total_size += sz
    fn = '.../' + fn[-75:] if len(fn) > 75 else fn
    TABLE.add_row([fn, bt, human_readable(sz)])

TABLE.add_row(['TOTALS', 'NA', human_readable(total_size)])

print(TABLE)
