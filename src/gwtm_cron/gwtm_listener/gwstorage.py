import fsspec  # type: ignore
from swiftclient import Connection as SwiftConnection  # type: ignore
from swiftclient.exceptions import ClientException as SwiftClientException  # type: ignore
from keystoneauth1.identity import v3  # type: ignore
from keystoneauth1 import session  # type: ignore

def _get_fs(source, config):
    """Get filesystem for S3 or Azure. Swift uses _get_swift_conn instead."""
    try:
        if source == 's3':
            return fsspec.filesystem("s3", key=config.AWS_ACCESS_KEY_ID, secret=config.AWS_SECRET_ACCESS_KEY)
        if source == 'abfs':
            return fsspec.filesystem("abfs", account_name=config.AZURE_ACCOUNT_NAME, account_key=config.AZURE_ACCOUNT_KEY)
        if source == 'swift':
            # Swift uses python-swiftclient directly, not fsspec
            raise NotImplementedError("Swift should use _get_swift_conn, not _get_fs")
    except Exception as e:
        raise Exception(f"Error in creating {source} filesystem: {str(e)}")


def _get_swift_conn(config):
    """Get Swift connection using python-swiftclient.

    Supports both application credentials and username/password authentication.
    If OS_USERNAME looks like a UUID (application credential ID), uses
    application credential authentication via keystoneauth1. Otherwise uses
    username/password authentication.
    """
    try:
        # Check if we're using application credentials (UUID format)
        # Application credential IDs are 32 character hex strings
        import re
        is_app_cred = bool(re.match(r'^[a-f0-9]{32}$', config.OS_USERNAME or ''))

        if is_app_cred:
            # Use application credential authentication with keystoneauth1
            auth = v3.ApplicationCredential(
                auth_url=config.OS_AUTH_URL,
                application_credential_id=config.OS_USERNAME,
                application_credential_secret=config.OS_PASSWORD
            )
            sess = session.Session(auth=auth)

            return SwiftConnection(
                session=sess,
                os_options={
                    'object_storage_url': config.OS_STORAGE_URL
                }
            )
        else:
            # Use username/password authentication
            return SwiftConnection(
                authurl=config.OS_AUTH_URL,
                user=config.OS_USERNAME,
                key=config.OS_PASSWORD,
                os_options={
                    'user_domain_name': config.OS_USER_DOMAIN_NAME,
                    'project_domain_name': config.OS_PROJECT_DOMAIN_NAME,
                    'project_name': config.OS_PROJECT_NAME,
                },
                auth_version='3'
            )
    except Exception as e:
        raise Exception(f"Error creating Swift connection: {str(e)}")


def download_gwtm_file(filename, source='s3', config=None, decode=True):

    try:
        if source == 'swift':
            # Use python-swiftclient directly for Swift
            conn = _get_swift_conn(config)
            headers, content = conn.get_object(config.OS_CONTAINER_NAME, filename)
            if decode:
                return content.decode('utf-8')
            else:
                return content

        # Use fsspec for S3 and Azure
        if source == 's3':
            s3file = fsspec.open(f"s3://{config.AWS_BUCKET}/{filename}")

        if source == 'abfs':
            s3file = fsspec.open(f"abfs://{filename}", "rb", account_name=config.AZURE_ACCOUNT_NAME, account_key=config.AZURE_ACCOUNT_KEY)

        with s3file as _file:
            if decode:
                return _file.read().decode('utf-8')
            else:
                return _file.read()

    except Exception:
        raise Exception(f"Error reading {source} file: {filename}")
            


def upload_gwtm_file(content, filename, source="s3", config=None):
    if source == 'swift':
        # Use python-swiftclient directly for Swift
        conn = _get_swift_conn(config)
        conn.put_object(config.OS_CONTAINER_NAME, filename, content)
        return True

    # Use fsspec for S3 and Azure
    fs = _get_fs(source=source, config=config)

    if source=="s3" and f"{config.AWS_BUCKET}/" not in filename:
        filename = f"{config.AWS_BUCKET}/{filename}"

    open_file = fs.open(filename, "wb")
    with open_file as of:
        of.write(content)
    of.close()
    return True


def list_gwtm_bucket(container, source="s3", config=None):
    if source == 'swift':
        # Use python-swiftclient directly for Swift
        conn = _get_swift_conn(config)
        headers, objects = conn.get_container(config.OS_CONTAINER_NAME, prefix=f"{container}/")
        ret = []
        for obj in objects:
            name = obj['name']
            # Skip the directory marker itself
            if name != f"{container}/":
                ret.append(name)
        return sorted(ret)

    # Use fsspec for S3 and Azure
    fs = _get_fs(source=source, config=config)

    if source == 's3':
        bucket_content = fs.ls(f"{config.AWS_BUCKET}/{container}")
        ret = []
        for b in bucket_content:
            split_b = b.split(f"{config.AWS_BUCKET}/")[1]
            if split_b != f"{container}/":
                ret.append(split_b)
        return sorted(ret)

    ret = fs.ls(container)
    return sorted(ret)


def delete_gwtm_files(keys, source="s3", config=None):
    if source == 'swift':
        # Use python-swiftclient directly for Swift
        conn = _get_swift_conn(config)
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            conn.delete_object(config.OS_CONTAINER_NAME, key)
        return True

    # Use fsspec for S3 and Azure
    if source=="s3":
        if isinstance(keys, list):
            for i,k in enumerate(keys):
                if f"{config.AWS_BUCKET}/" not in k:
                    keys[i] = f"{config.AWS_BUCKET}/{k}"
        if isinstance(keys, str) and f"{config.AWS_BUCKET}/" not in keys:
            keys = f"{config.AWS_BUCKET}/{keys}"

    fs = _get_fs(source=source, config=config)
    fs.rm(keys)
    return True


def get_fname(fullname):
    split = fullname.split('/')
    fname = split[len(split)-1]
    return fname

class test_config(object):
    pass

if __name__ == '__main__':
    import os
    config = test_config()

    config.AZURE_ACCOUNT_NAME = os.environ.get('AZURE_ACCOUNT_NAME', None) # type: ignore
    config.AZURE_ACCOUNT_KEY = os.environ.get("AZURE_ACCOUNT_KEY", None) # type: ignore
    config.AWS_BUCKET = os.environ.get("AWS_BUCKET", None) # type: ignore
    config.AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', None) # type: ignore
    config.AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', None) # type: ignore

    bucket_sources = ["s3", "abfs"]
    for source in bucket_sources:

        content = "line1\\nline2"
        filename = "test/vv.text"
        test1 = upload_gwtm_file(content, filename, source=source, config=config)
        assert test1, "error upload" 

        test2 = download_gwtm_file(filename, source=source, config=config)
        assert test2==content, "error download"

        test3 = delete_gwtm_files([filename], source=source, config=config)
        assert test3, "error delete"

    s3_content = list_gwtm_bucket("fit", "s3", config)[0:10]
    abfs_content = list_gwtm_bucket("fit", "abfs", config)[0:10]
    assert s3_content==abfs_content
