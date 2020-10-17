import hashlib

from azure.storage.blob import BlobServiceClient, ContainerClient, StandardBlobTier

def connect_service(url: str, creds: str) -> BlobServiceClient:
    return BlobServiceClient(account_url=url, credential=creds)

def connect_container(service: BlobServiceClient, container: str) -> ContainerClient:
    # Parse options for container name to upload, compare to list to see if container needs to be created
    container_list = [x for x in service.list_containers()]
    container_names = [x['name'] for x in container_list]

    container_client = service.get_container_client(container)

    if container not in container_names: # Meaning no container setup yet.
        operation = container_client.create_container()
        if operation['error_code'] is not None:
            raise Exception(operation['error_code'])
        else:
            print(f"{operation['date']}: Created container {container}, request_id: {operation['request_id']}.")

    return container_client

def get_blob_manifest(container_client: ContainerClient) -> (list): # Returns list of filenames.
    return [y['name'] for y in [x for x in container_client.list_blobs()]]

def upload_blob(container_client: ContainerClient,
                filename: str,
                tier: StandardBlobTier,
                update=False,
                overwrite=False
               ) -> dict:
    file_md5 = hashlib.md5(open(filename, 'rb').read()).hexdigest()

    blob_client = container_client.get_blob_client(filename)

    if update:
        blob_properties = blob_client.get_blob_properties()
        try:
            blob_md5 = blob_properties['metadata']['md5']
        except KeyError:
            blob_md5 = ''

        print(f"{filename} already in container. cloud md5: {blob_md5}, local md5: {file_md5}")
        if file_md5 != blob_md5:
            print(f'MD5sum Mismatch: Sending local copy of {filename}')
            if overwrite:
                blob_client.delete_blob()
                blob_client.upload_blob(
                    open(filename, 'rb').read(),
                    standard_blob_tier=tier,
                    metadata={'md5': file_md5}
                )
            else:
                print(f'Set not to overwrite. Will not send {filename}')

    else:
        print(f'{filename} not found in container, sending local file.')
        blob_client.upload_blob(
            open(filename, 'rb').read(),
            standard_blob_tier=tier,
            metadata={'md5': file_md5}
        )

# TODO: Catch errors from service.
