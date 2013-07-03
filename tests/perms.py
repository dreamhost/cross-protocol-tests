import boto
import boto.s3.connection
import boto.s3
from boto.s3.key import Key

import swiftclient

import os.path
import sys

# url
url = "api.alexandria.dhobjects.com"

# S3 keys
access_key = 'KYN7aKpS7FEVW5e_tgzA'
secret_key = '_tzs-lDwMf-zv3fQmUqR-OjB8G1efsgHnKXm9gpX'

# Swift keys
username = 'permissions:swiftuser'
api_key = 'MJUrGGD8ZYCtdVXmOkr0YrlhAh-BwmJ8I3GrQSKS'
swifturl = 'http://api.alexandria.dhobjects.com/auth/v1.0'

s3conn = boto.connect_s3(
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    host = url,
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )

swiftconn = swiftclient.Connection(
    authurl = swifturl,
    user = username,
    key = api_key
    )

s3user = boto.connect_s3(
    aws_access_key_id = '6yE3REPZhNXemXmv7Rto',
    aws_secret_access_key = 'J2ZYmZzFRwYrAgc9-hZdVsRa8gqqJC1ZA0_gJI_U',
    host = url,
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )

swiftuser = swiftclient.Connection(
    authurl = swifturl,
    user = 'otheruser:swiftuser',
    key = 'lT6Avey7jvCldJzrmiaU-bmBTCUHe_gkIrQjl-ba'
    )

def create_swift_containers(swiftconn):
    containers=[
    'swift-default',
    'swift-publicread',
    'swift-publicwrite',
    'swift-publicreadwrite',
    'swift-privateread',
    'swift-privatewrite',
    'swift-privatereadwrite',
    'swift-publicreadprivatewrite',
    'swift-privatereadpublicwrite'
    ]
    headers=[
    {},
    {'x-container-read':'.r:*'},
    {'x-container-write':'.r:*'},
    {'x-container-read':'.r:*', 'x-container-write':'.r:*'},
    {'x-container-read':'otheruser'},
    {'x-container-write':'otheruser'},
    {'x-container-read':'otheruser', 'x-container-write':'otheruser'},
    {'x-container-read':'.r:*', 'x-container-write':'otheruser'},
    {'x-container-read':'otheruser', 'x-container-write':'.r:*'}
    ]
    assert(len(containers)==len(headers))
    for i in range(len(containers)):
        swiftconn.put_container(containers[i])
        swiftconn.post_container(containers[i], headers[i])
        swiftconn.put_object(containers[i],'test.txt','test text here')

def create_s3_buckets(s3conn):
    containers=[
    'as3-private',
    'as3-public-read',
    'as3-public-read-write',
    'as3-authenticated-read'
    ]
    headers=[
    {'x-amz-acl':'private'},
    {'x-amz-acl':'public-read'},
    {'x-amz-acl':'public-read-write'},
    {'x-amz-acl':'authenticated-read'}
    ]
    assert(len(containers)==len(headers))
    for i in range(len(containers)):
        s3conn.make_request('PUT', containers[i], headers=headers[i])
        s3conn.make_request('PUT', containers[i], 'test.txt', headers[i], data='test text here')

def list_swift_acls(swiftconn):
    containers=[
    's3-private',
    's3-public-read',
    's3-public-read-write',
    's3-authenticated-read'
    ]
    for i in range(len(containers)):
        print swiftconn.head_container(containers[i])

def list_s3_acls(s3conn):
    containers=[
    's3-private',
    's3-public-read',
    's3-public-read-write',
    's3-authenticated-read'
    ]
    for i in range(len(containers)):
        resp = s3conn.make_request('GET', containers[i], query_args='acl')
        print resp.read()

def add(s3conn):
    basepath = os.path.dirname(sys.argv[0])
    filepath = os.path.abspath(os.path.join(basepath, "..", "..", "sloth.gif"))
    containers=[
    's3-private',
    's3-public-read',
    's3-public-read-write',
    's3-authenticated-read'
    ]

    for i in range(len(containers)):
        bucket = s3conn.get_bucket(containers[i])
        key = Key(bucket)
        key.key = 'sloth.gif'
        key.set_contents_from_filename(filepath)