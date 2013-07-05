import swiftclient

import boto.s3.connection

from nose.tools import eq_ as eq

# url
url = "objects.dreamhost.com"

# S3 keys
access_key = 'fs46-5HPyNCbQ_HIYUwO'
secret_key = 'pz71sbAR4OK_OffU4BfZIpfFq5P0OFm68RgDRVg0'

# Swift keys
username = 'kevinchoi:kevchoi'
api_key = 'YGzYAnKmAeG6I9XatSVkGf0lAeUHH09NZiGMM30u'
swifturl = 'http://objects.dreamhost.com/auth/v1.0'

# For more parameters:
# https://github.com/boto/boto/blob/develop/boto/s3/connection.py
s3conn = boto.s3.S3Connection(
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    is_secure=True,
    port=None,
    proxy=None,
    proxy_port=None,
    host = url,
    https_connection_factory=None,
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )

# For more parameters:
# https://github.com/openstack/python-swiftclient/blob/master/swiftclient/client.py
swiftconn = swiftclient.Connection(
    authurl = swifturl,
    user = username,
    key = api_key,
    preauthurl=None
    # NOTE TO SELF: Port, HTTPS/HTTP, etc. all contained in authurl/preauthurl
    )