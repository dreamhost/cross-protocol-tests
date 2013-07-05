import boto
import boto.s3.connection
import boto.s3
from boto.s3.key import Key

import swiftclient

import random
from nose.tools import eq_ as eq

from configobj import ConfigObj


## IMPORT FROM CONFIG.INI

conf = ConfigObj('config.ini')
s3keys = conf['s3']
swiftkeys = conf['swift']

s3conn = boto.connect_s3(
    aws_access_key_id = s3keys['username'],
    aws_secret_access_key = s3keys['key'],
    host = s3keys['url'],
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )

swiftconn = swiftclient.Connection(
    authurl = swiftkeys['url'],
    user = swiftkeys['username'],
    key = swiftkeys['key']
    )

## HELPER FUNCTIONS

def assert_raises(excClass, callableObj, *args, **kwargs):
    """
    Like unittest.TestCase.assertRaises, but returns the exception.
    """
    try:
        callableObj(*args, **kwargs)
    except excClass as e:
        return e
    else:
        if hasattr(excClass, '__name__'):
            excName = excClass.__name__
        else:
            excName = str(excClass)
        raise AssertionError("%s not raised" % excName)

def swift_list_containers(swiftconn):
    buckets = swiftconn.get_account()[1]
    list_of_buckets = [bucket_dictionary[u'name'] \
    for bucket_dictionary in buckets]
    return list_of_buckets

def swift_list_objects(swiftconn, name):
    objects = swiftconn.get_container(name)[1]
    list_of_objects = [object_dictionary[u'name'] \
    for object_dictionary in objects]
    return list_of_objects

def swift_delete_all_containers(swiftconn):
    for container in swift_list_containers(swiftconn):
        objects = swift_list_objects(swiftconn, container)
        for name in objects:
            swiftconn.delete_object(container, name)
        swiftconn.delete_container(container)

def swift_md5(swiftconn, container, object_name):
    return swiftconn.head_object(container, object_name)['etag']

def swift_size(swiftconn, container, object_name):
    return int(swiftconn.head_object(container, object_name)['content-length'])

def s3_list_buckets(s3conn):
    buckets = s3conn.get_all_buckets()
    list_of_buckets = [bucket.name for bucket in buckets]
    return list_of_buckets

def s3_list_objects(s3conn, name):
    bucket = s3conn.get_bucket(name)
    list_of_objects = [obj.key for obj in bucket.list()]
    return list_of_objects

def s3_delete_all_buckets(s3conn):
    for name in s3_list_buckets(s3conn):
        bucket = s3conn.get_bucket(name)
        keys = bucket.list()
        for key in keys:
            key.delete()
        s3conn.delete_bucket(bucket)

def s3_md5(s3conn, bucket, object_name):
    resp = s3conn.make_request('HEAD', bucket, object_name)
    for x in resp.getheaders():
        if x[0]=='etag':
            return x[1][1:-1]

def s3_size(s3conn, bucket, object_name):
    resp = s3conn.make_request('HEAD', bucket, object_name)
    for x in resp.getheaders():
        if x[0]=='content-length':
            return x[1][1:-1]

# 3-63 chars, must start/end with lowercase letter or number
# can contain lowercase letters, numbers, and dashes (no periods)
# cannot start/end with dash or period
# no consecutive period/dashes
def create_valid_name():
    name_length = random.randint(3,63)
    name = ''
    for x in range(name_length):
        if x == 0 or x == name_length-1:
            name+=chr(random.randint(97,122))
        elif x!=0 and name[x-1] in '-.':
            name+=chr(random.randint(97,122))
        else:
            name+=random.choice(chr(random.randint(97,122))+'-')
    return name


## Note: to reestablish connection, set up tests as classes
## and use instance variables

## BUCKET TESTS

def test_list_buckets():
    # Create bucket
    name = create_valid_name()
    bucket = s3conn.create_bucket(name)
    # Check buckets list
    # name = name.encode('utf-8')
    eq(swift_list_containers(swiftconn), [name])
    eq(s3_list_buckets(s3conn), [name])
    # FIXME: remove bucket or teardown?

def test_list_container():
    # Create container
    name = create_valid_name()
    swiftconn.put_container(name)
    # Check containers list
    eq(s3_list_buckets(s3conn), [name])
    eq(swift_list_containers(swiftconn), [name])
    # FIXME: remove bucket or teardown?

def test_list_empty_bucket():
    # Create bucket
    name = create_valid_name()
    bucket = s3conn.create_bucket(name)
    # Get list of objects using Swift
    eq(swift_list_objects(swiftconn, name), [])
    eq(s3_list_objects(s3conn, name), [])

def test_list_empty_container():
    # Create container
    name = create_valid_name()
    swiftconn.put_container(name)
    # Get list of objects
    eq(s3_list_objects(s3conn, name), [])
    eq(swift_list_objects(swiftconn, name), [])

def test_list_nonempty_bucket():
    # Create bucket
    name = create_valid_name()
    bucket = s3conn.create_bucket(name)
    # Add test object using S3
    k = Key(bucket)
    k.key = 'test'
    k.set_contents_from_string('this is a test file')
    # Get list of objects using Swift
    eq(swift_list_objects(swiftconn, name), ['test'])

def test_list_nonempty_container():
    # Create container
    name = create_valid_name()
    swiftconn.put_container(name)
    # Add test object using Swift
    swiftconn.put_object(name, 'test', 'this is a test file')
    # Get list of objects using S3
    eq(s3_list_objects(s3conn, name), ['test'])

#test_head_bucket FIXME

#test_head_container FIXME

def test_delete_bucket():
    # Create bucket
    name = create_valid_name()
    bucket = s3conn.create_bucket(name)
    # Delete using Swift
    swiftconn.delete_container(name)
    # List buckets
    eq(swift_list_containers(swiftconn), [])
    eq(s3_list_buckets(s3conn), [])
    # Try deleting again with S3/Swift
    s3_err = assert_raises(boto.exception.S3ResponseError, s3conn.delete_bucket, name)
    swift_err = assert_raises(swiftclient.ClientException, swiftconn.delete_container, name)
    eq(s3_err.status,404)
    eq(swift_err.http_status, 404)
    eq(swift_err.http_reason, 'Not Found')
    eq(s3_err.reason, 'Not Found')
    eq(swift_err.http_response_content, 'NoSuchBucket')
    eq(s3_err.error_code, 'NoSuchBucket')

def test_delete_container():
    # Create bucket using Swift
    name = create_valid_name()
    swiftconn.put_container(name)
    # Delete using S3
    s3conn.delete_bucket(name)
    # List buckets
    eq(swift_list_containers(swiftconn), [])
    eq(s3_list_buckets(s3conn), [])
    # Try deleting again with S3/Swift
    s3_err = assert_raises(boto.exception.S3ResponseError, s3conn.delete_bucket, name)
    swift_err = assert_raises(swiftclient.ClientException, swiftconn.delete_container, name)
    eq(s3_err.status,404)
    eq(swift_err.http_status, 404)
    eq(swift_err.http_reason, 'Not Found')
    eq(s3_err.reason, 'Not Found')
    eq(swift_err.http_response_content, 'NoSuchBucket')
    eq(s3_err.error_code, 'NoSuchBucket')

def test_delete_non_empty_bucket():
    # Create bucket
    name = create_valid_name()
    bucket = s3conn.create_bucket(name)
    # Add test object using S3
    k = Key(bucket)
    k.key = 'test'
    k.set_contents_from_string('this is a test file')
    # Try to delete bucket using Swift
    swift_err = assert_raises(swiftclient.ClientException, swiftconn.delete_container, name)
    eq(swift_err.http_status, 409)
    eq(swift_err.http_reason, 'Conflict')
    eq(swift_err.http_response_content, 'BucketNotEmpty')

def test_delete_non_empty_container():
    # Create container
    name = create_valid_name()
    swiftconn.put_container(name)
    # Add test object using Swift
    swiftconn.put_object(name, 'test', 'this is a test file')
    # Try to delete container using S3
    s3_err = assert_raises(boto.exception.S3ResponseError, s3conn.delete_bucket, name)
    eq(s3_err.status, 409)
    eq(s3_err.reason, 'Conflict')
    eq(s3_err.error_code, 'BucketNotEmpty')

## OBJECT TESTS

def test_list_object_in_bucket():
    swift_delete_all_containers(swiftconn)
    # Create container using Swift
    name = create_valid_name()
    swiftconn.put_container(name)
    # Create object using S3
    bucket = s3conn.get_bucket(name)
    k = Key(bucket)
    k.key = 'test'
    k.set_contents_from_string('Create object using S3')
    # Check list of objects
    eq(swift_list_objects(swiftconn, name), ['test'])
    eq(s3_list_objects(s3conn, name), ['test'])

def test_list_object_in_container():
    swift_delete_all_containers(swiftconn)
    # Create bucket using S3
    name = create_valid_name()
    bucket = s3conn.create_bucket(name)
    # Create object using Swift
    swiftconn.put_object(name, 'test', 'Create object using Swift')
    # Check list of objects
    eq(s3_list_objects(s3conn, name), ['test'])
    eq(swift_list_objects(swiftconn, name), ['test'])

def test_size_object_in_container():
    # Create container
    name = create_valid_name()
    swiftconn.put_container(name)
    # Create object using S3
    bucket = s3conn.get_bucket(name)
    k = Key(bucket)
    k.key = 'test'
    f = open('/dev/urandom', 'r')
    data = f.read(random.randint(1,30000))
    f.close()
    k.set_contents_from_string(data)
    # Check size
    eq(k.size, swift_size(swiftconn, name, 'test'))

def test_size_object_in_bucket():
    # Create bucket
    name = create_valid_name()
    bucket = s3conn.create_bucket(name)
    # Create object using Swift
    f = open('/dev/urandom', 'r')
    data = f.read(random.randint(1,30000))
    f.close()
    swiftconn.put_object(name, 'test', data)
    k = Key(bucket, 'test')
    k.open_read()
    # Check size
    eq(k.size, swift_size(swiftconn, name, 'test'))

def test_checksum_object_in_container():
    # Create container
    name = create_valid_name()
    swiftconn.put_container(name)
    # Create object using S3
    bucket = s3conn.get_bucket(name)
    k = Key(bucket)
    k.key = 'test'
    f = open('/dev/urandom', 'r')
    data = f.read(random.randint(1,30000))
    f.close()
    k.set_contents_from_string(data)
    # Checksum
    eq(s3_md5(s3conn, name, 'test'), swift_md5(swiftconn, name, 'test'))

def test_checksum_object_in_bucket():
    # Create bucket
    name = create_valid_name()
    bucket = s3conn.create_bucket(name)
    # Create object using Swift
    f = open('/dev/urandom', 'r')
    data = f.read(random.randint(1,30000))
    f.close()
    swiftconn.put_object(name, 'test', data)
    k = Key(bucket, 'test')
    # Checksum
    eq(s3_md5(s3conn, name, 'test'), swift_md5(swiftconn, name, 'test'))

def test_delete_object_swift():
    # Create bucket
    name = create_valid_name()
    bucket = s3conn.create_bucket(name)
    # Create object using S3
    bucket = s3conn.get_bucket(name)
    k = Key(bucket)
    k.key = 'test'
    f = open('/dev/urandom', 'r')
    data = f.read(random.randint(1,30000))
    f.close()
    k.set_contents_from_string(data)
    # Delete object with Swift
    swiftconn.delete_object(name, 'test')
    # Check list of objects
    eq(s3_list_objects(s3conn, name), [])
    eq(swift_list_objects(swiftconn, name), [])

def test_delete_object_s3():
    # Create container
    name = create_valid_name()
    swiftconn.put_container(name)
    # Create object using Swift
    f = open('/dev/urandom', 'r')
    data = f.read(random.randint(1,30000))
    f.close()
    swiftconn.put_object(name, 'test', data)
    # Delete object with S3
    bucket = s3conn.get_bucket(name)
    k = Key(bucket, 'test')
    k.delete()
    # Check list of objects
    eq(s3_list_objects(s3conn, name), [])
    eq(swift_list_objects(swiftconn, name), [])