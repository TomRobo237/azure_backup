'''set of functions to connect to azure and handle interactions to make scripting easier'''
import hashlib
from datetime import datetime
from time import sleep
from sys import stderr
import threading

from azure.storage.blob import BlobServiceClient, ContainerClient, StandardBlobTier, BlobClient, RehydratePriority

from .logger import log

def get_md5sum(filename: str) -> str:
    with open(filename, 'rb') as fp:
        file_hash = hashlib.md5()
        while chunk := fp.read(8192):
            file_hash.update(chunk)
    log.debug(f'Calculated md5 {file_hash.hexdigest()}')
    return file_hash.hexdigest()

def connect_service(url: str, creds: str) -> BlobServiceClient:
    '''Connect to the main service, maybe write new ways to connect later'''
    return BlobServiceClient(account_url=url, credential=creds)

def connect_container(service: BlobServiceClient, container: str, create=True) -> ContainerClient:
    '''
    Parse options for container name to upload,
    compare to list to see if container needs to be created
    '''
    container_list = [x for x in service.list_containers()]
    container_names = [x['name'] for x in container_list]

    container_client = service.get_container_client(container)

    if container not in container_names and create: # Meaning no container setup yet.
        operation = container_client.create_container()
        if operation['error_code'] is not None:
            raise Exception(operation['error_code'])
        else:
            log.info(f"Created container {container}, request_id: {operation['request_id']}.")
    elif container not in container_names and not create:
        log.error(f'Container {container} not found.')
        exit(1)


    return container_client

def get_blob_manifest(container_client: ContainerClient) -> (list):
    '''Returns list of filenames.'''
    return [y.name for y in [x for x in container_client.list_blobs()]]

def get_blob_list_information(container_client: ContainerClient) -> (list):
    '''Returns list of tuples with information about blobs..'''
    return [(y.name, y.blob_tier, y.size) for y in [x for x in container_client.list_blobs()]]

def set_blob_tier(blob: BlobClient, tier: StandardBlobTier, priority: RehydratePriority) -> None:
    '''Set/change blob tier'''
    log.info(f'Setting blob tier for {blob.blob_name} to {tier.value} with priority {priority.value}')
    blob.set_standard_blob_tier(tier, rehydrate_priority=priority)
    INFO = blob.get_blob_properties()
    log.info(f'blob {blob.blob_name} tier is currently {INFO.blob_tier} and archive_status is now at '
             f'{INFO.archive_status}')

def upload_blob(container_client: ContainerClient,
                filename: str,
                azure_filename: str,
                tier: StandardBlobTier,
                update=False,
                overwrite=False,
                retries=0,
                debug=False
               ) -> dict:
    '''
    Upload a file as a blob to the cloud, there is checking to see if the md5sum matches if its
    already uploaded, by tagging the md5 in the metadata.
    '''
    #TODO: Make this log better, more readable, kinda a mess rn
    file_md5 = get_md5sum(filename)
    operation = {'operation': 'no-op'} # Default return

    blob_client = container_client.get_blob_client(azure_filename)

    if update:
        blob_properties = blob_client.get_blob_properties()
        try:
            blob_md5 = blob_properties['metadata']['md5']
        except KeyError:
            blob_md5 = ''
        log.info(f"Already in container. {azure_filename} cloud md5: {blob_md5}, {filename} local md5: {file_md5}")
        if file_md5 != blob_md5: # TODO: Reorder to be cleaner
            if overwrite:
                log.info(f'MD5sum Mismatch - Sending local copy of {filename}')
                blob_client.delete_blob()
                with open(filename, 'rb') as data:
                    operation = blob_client.upload_blob(
                        data,
                        standard_blob_tier=tier,
                        metadata={'md5': file_md5}
                    )
            else:
                log.info(f'MD5Sum Mismatch - Set not to overwrite. Will not send {filename}')
        else:
            log.info(f'MD5Sums Match - no-op')
    else:
        log.info(f'{filename} not found in container, sending local file.')
        with open(filename, 'rb') as data:
            operation = blob_client.upload_blob(
                data,
                standard_blob_tier=tier,
                metadata={'md5': file_md5}
            )
        log.info(f"Uploaded: {filename}, request_id: {operation['request_id']}"
        )
    return operation
