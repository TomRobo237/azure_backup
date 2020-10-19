'''set of functions to connect to azure and handle interactions to make scripting easier'''
import hashlib
from datetime import datetime
from time import sleep
from sys import stderr

from azure.storage.blob import BlobServiceClient, ContainerClient, StandardBlobTier

DATE_FORMAT = '%Y-%m-%d %X - '
def timestamp():
    return datetime.utcnow().strftime(DATE_FORMAT)

def print_debug(message):
    print(message, file=stderr)

def get_md5sum(filename: str) -> str:
    with open(filename, 'rb') as fp:
        file_hash = hashlib.md5()
        while chunk := fp.read(8192):
            file_hash.update(chunk)
    print(f'Calculated md5 {file_hash.hexdigest()}')
    return file_hash.hexdigest()

def connect_service(url: str, creds: str) -> BlobServiceClient:
    '''Connect to the main service, maybe write new ways to connect later'''
    return BlobServiceClient(account_url=url, credential=creds)

def connect_container(service: BlobServiceClient, container: str) -> ContainerClient:
    '''
    Parse options for container name to upload,
    compare to list to see if container needs to be created
    '''
    container_list = [x for x in service.list_containers()]
    container_names = [x['name'] for x in container_list]

    container_client = service.get_container_client(container)

    if container not in container_names: # Meaning no container setup yet.
        operation = container_client.create_container()
        if operation['error_code'] is not None:
            raise Exception(operation['error_code'])
        else:
            print(f"{operation['date'].strftime(DATE_FORMAT)}"
                  f"Created container {container}, request_id: {operation['request_id']}."
                 )

    return container_client

def get_blob_manifest(container_client: ContainerClient) -> (list):
    '''Returns list of filenames.'''
    return [y['name'] for y in [x for x in container_client.list_blobs()]]

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
    file_md5 = get_md5sum(filename)
    log = []

    blob_client = container_client.get_blob_client(azure_filename)

    if update:
        blob_properties = blob_client.get_blob_properties()
        try:
            blob_md5 = blob_properties['metadata']['md5']
        except KeyError:
            blob_md5 = ''
        log.append(f"{timestamp()} Already in container. {azure_filename} cloud md5: {blob_md5}, " +
              f"{filename} local md5: {file_md5}"
             )
        if debug:
            print_debug(log[-1])
        if file_md5 != blob_md5:
            log.append(f'{timestamp()} MD5sum Mismatch - Sending local copy of {filename}')
            if debug:
                print_debug(log[-1])
            if overwrite:
                try:
                    blob_client.delete_blob()
                    with open(filename, 'rb') as data:
                        operation = blob_client.upload_blob(
                            data,
                            standard_blob_tier=tier,
                            metadata={'md5': file_md5}
                        )
                except Exception as e: # Should be more specific... but.
                    print(f"{timestamp()} EXCEPTION!!! {e}")
                    if retries < 1:
                        kwargs['retries'] = kwargs['retries'] + 1
                        sleep(2)
                        upload_blob(*args, **kwargs)
            else:
                log.append(f'{timestamp()} Set not to overwrite. Will not send {filename}')
                if debug:
                    print_debug(log[-1])
                operation = {'operation': 'no-op'}
        else:
            log.append(f'{timestamp()} MD5Sums Match - no-op')
            if debug:
                print_debug(log[-1])
            operation = {'operation': 'no-op'}
    else:
        log.append(f'{timestamp()} {filename} not found in container, sending local file.')
        if debug:
            print_debug(log[-1])
        try:
            with open(filename, 'rb') as data:
                operation = blob_client.upload_blob(
                    data,
                    standard_blob_tier=tier,
                    metadata={'md5': file_md5}
                )
            log.append(f"{operation['date'].strftime(DATE_FORMAT)} Uploaded: {filename}, " +
              f"request_id: {operation['request_id']}"
            )
            if debug:
                print_debug(log[-1])
        except Exception as e: # Should be more specific... but.
            print(f"{timestamp()} EXCEPTION!!! {e}")
            if retries < 1:
                kwargs['retries'] = kwargs['retries'] + 1
                sleep(2)
                upload_blob(*args, **kwargs)
    if debug:
        log = [f'{timestamp()} Debug log, already printed.']
    return operation, log
