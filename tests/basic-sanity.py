import boto.s3.connection

import swiftclient

import random

from nose.tools import with_setup

from tools import assert_raises
from tools import create_valid_name
from tools import get_s3conn
from tools import get_swiftconn

from nose.tools import eq_ as eq

## BUCKET TESTS


def setup_delete_buckets():
    # Deletes all buckets, both empty and nonempty
    s3conn = get_s3conn()
    for bucket in s3conn.list_buckets():
        b = s3conn.get_bucket(bucket)
        keys = b.list()
        for key in keys:
            key.delete()
        s3conn.delete_bucket(bucket)


@with_setup(setup_delete_buckets, teardown=None)
def test_list_buckets():
    # Create bucket
    s3conn = get_s3conn()
    bucket = create_valid_name()
    s3conn.put_bucket(bucket)

    # Check list buckets
    eq(s3conn.list_buckets(), [bucket])
    swiftconn = get_swiftconn()
    eq(swiftconn.list_containers(), [bucket])


@with_setup(setup_delete_buckets, teardown=None)
def test_list_container():
    # Create container
    swiftconn = get_swiftconn()
    bucket = create_valid_name()
    swiftconn.put_container(bucket)

    # Check list containers
    eq(swiftconn.list_containers(), [bucket])
    s3conn = get_s3conn()
    eq(s3conn.list_buckets(), [bucket])


def test_list_empty_bucket():
    # Create bucket
    s3conn = get_s3conn()
    bucket = create_valid_name()
    s3conn.put_bucket(bucket)
    # Get list of objects
    swiftconn = get_swiftconn()
    eq(swiftconn.list_objects(bucket), [])
    eq(s3conn.list_objects(bucket), [])


def test_list_empty_container():
    # Create container
    bucket = create_valid_name()
    swiftconn = get_swiftconn()
    swiftconn.put_container(bucket)
    # Get list of objects
    s3conn = get_s3conn()
    eq(s3conn.list_objects(bucket), [])
    eq(swiftconn.list_objects(bucket), [])


def test_list_nonempty_bucket():
    # Create bucket
    s3conn = get_s3conn()
    bucket = create_valid_name()
    s3conn.put_bucket(bucket)
    # Add test object using S3
    objectname = 'test-object'
    text = 'test-object text'
    s3conn.put_object(bucket, objectname, text)
    # Get list of objects using Swift
    swiftconn = get_swiftconn()
    eq(swiftconn.list_objects(bucket), [objectname])


def test_list_nonempty_container():
    # Create container
    bucket = create_valid_name()
    swiftconn = get_swiftconn()
    swiftconn.put_container(bucket)
    # Add test object using Swift
    objectname = 'test-object'
    text = 'test-object text'
    swiftconn.put_object(bucket, objectname, text)
    # Get list of objects using S3
    s3conn = get_s3conn()
    eq(s3conn.list_objects(bucket), [objectname])


@with_setup(setup_delete_buckets, teardown=None)
def test_delete_bucket():
    # Create bucket
    s3conn = get_s3conn()
    bucket = create_valid_name()
    s3conn.put_bucket(bucket)
    # Delete using Swift
    swiftconn = get_swiftconn()
    swiftconn.delete_container(bucket)
    # List buckets
    eq(swiftconn.list_containers(), [])
    eq(s3conn.list_buckets(), [])
    # Try deleting again with S3/Swift
    s3_err = assert_raises(boto.exception.S3ResponseError,
                           s3conn.delete_bucket, bucket)
    swift_err = assert_raises(swiftclient.ClientException,
                              swiftconn.delete_container, bucket)
    eq(s3_err.status, 404)
    eq(swift_err.http_status, 404)
    eq(swift_err.http_reason, 'Not Found')
    eq(s3_err.reason, 'Not Found')
    eq(swift_err.http_response_content, 'NoSuchBucket')
    eq(s3_err.error_code, 'NoSuchBucket')


@with_setup(setup_delete_buckets, teardown=None)
def test_delete_container():
    # Create bucket using Swift
    swiftconn = get_swiftconn()
    bucket = create_valid_name()
    swiftconn.put_container(bucket)
    # Delete using S3
    s3conn = get_s3conn()
    s3conn.delete_bucket(bucket)
    # List buckets
    eq(swiftconn.list_containers(), [])
    eq(s3conn.list_buckets(), [])
    # Try deleting again with S3/Swift
    s3_err = assert_raises(boto.exception.S3ResponseError,
                           s3conn.delete_bucket, bucket)
    swift_err = assert_raises(swiftclient.ClientException,
                              swiftconn.delete_container, bucket)
    eq(s3_err.status, 404)
    eq(swift_err.http_status, 404)
    eq(swift_err.http_reason, 'Not Found')
    eq(s3_err.reason, 'Not Found')
    eq(swift_err.http_response_content, 'NoSuchBucket')
    eq(s3_err.error_code, 'NoSuchBucket')


def test_delete_non_empty_bucket():
    # Create bucket
    s3conn = get_s3conn()
    bucket = create_valid_name()
    s3conn.put_bucket(bucket)
    # Add test object using S3
    objectname = 'test-object'
    text = 'test-object text'
    s3conn.put_object(bucket, objectname, text)
    # Try to delete bucket using Swift
    swiftconn = get_swiftconn()
    swift_err = assert_raises(swiftclient.ClientException,
                              swiftconn.delete_container, bucket)
    eq(swift_err.http_status, 409)
    eq(swift_err.http_reason, 'Conflict')
    eq(swift_err.http_response_content, 'BucketNotEmpty')


def test_delete_non_empty_container():
    # Create container
    bucket = create_valid_name()
    swiftconn = get_swiftconn()
    swiftconn.put_container(bucket)
    # Add test object using Swift
    objectname = 'test-object'
    text = 'test-object text'
    swiftconn.put_object(bucket, objectname, text)
    # Try to delete container using S3
    s3conn = get_s3conn()
    s3_err = assert_raises(boto.exception.S3ResponseError,
                           s3conn.delete_bucket, bucket)
    eq(s3_err.status, 409)
    eq(s3_err.reason, 'Conflict')
    eq(s3_err.error_code, 'BucketNotEmpty')


## OBJECT TESTS
def test_list_object_in_bucket():
    # Create container using Swift
    swiftconn = get_swiftconn()
    bucket = create_valid_name()
    swiftconn.put_container(bucket)
    # Create object using S3
    s3conn = get_s3conn()
    objectname = 'test-object'
    text = 'test-object text'
    s3conn.put_object(bucket, objectname, text)
    # Check list of objects
    eq(swiftconn.list_objects(bucket), [objectname])
    eq(s3conn.list_objects(bucket), [objectname])


def test_list_object_in_container():
    # Create bucket using S3
    s3conn = get_s3conn()
    bucket = create_valid_name()
    s3conn.put_bucket(bucket)
    # Create object using Swift
    swiftconn = get_swiftconn()
    objectname = 'test-object'
    text = 'test-object text'
    swiftconn.put_object(bucket, objectname, text)
    # Check list of objects
    eq(s3conn.list_objects(bucket), [objectname])
    eq(swiftconn.list_objects(bucket), [objectname])


def test_size_object_in_container():
    # Create container
    swiftconn = get_swiftconn()
    bucket = create_valid_name()
    swiftconn.put_container(bucket)
    # Create object using S3
    s3conn = get_s3conn()
    objectname = 'test-object'
    bytes = s3conn.put_random_object(bucket, objectname)
    # Check size
    eq(bytes, swiftconn.get_size(bucket, objectname))


def test_size_object_in_bucket():
    # Create bucket
    s3conn = get_s3conn()
    bucket = create_valid_name()
    s3conn.put_bucket(bucket)
    # Create object using Swift
    swiftconn = get_swiftconn()
    objectname = 'test-object'
    bytes = swiftconn.put_random_object(bucket, objectname)
    # Check size
    eq(bytes, s3conn.get_size(bucket, objectname))


def test_checksum_object_in_container():
    # Create container
    swiftconn = get_swiftconn()
    bucket = create_valid_name()
    swiftconn.put_container(bucket)
    # Create object using S3
    s3conn = get_s3conn()
    objectname = 'test-object'
    s3conn.put_random_object(bucket, objectname)
    # Check checksum
    eq(s3conn.get_md5(bucket, objectname),
       swiftconn.get_md5(bucket, objectname))


def test_checksum_object_in_bucket():
    # Create bucket
    s3conn = get_s3conn()
    bucket = create_valid_name()
    s3conn.put_bucket(bucket)
    # Create object using Swift
    swiftconn = get_s3conn()
    objectname = 'test-object'
    swiftconn.put_random_object(bucket, objectname)
    # Check checksum
    eq(s3conn.get_md5(bucket, objectname),
       swiftconn.get_md5(bucket, objectname))


def test_delete_object_swift():
    # Create bucket
    s3conn = get_s3conn()
    bucket = create_valid_name()
    s3conn.put_bucket(bucket)
    # Create object using S3
    s3conn = get_s3conn()
    objectname = 'test-object'
    s3conn.put_random_object(bucket, objectname)
    # Delete object with Swift
    swiftconn = get_swiftconn()
    swiftconn.delete_object(bucket, objectname)
    # Check list of objects
    eq(s3conn.list_objects(bucket), [])
    eq(swiftconn.list_objects(bucket), [])


def test_delete_object_s3():
    # Create container
    swiftconn = get_swiftconn()
    bucket = create_valid_name()
    swiftconn.put_container(bucket)
    # Create object using Swift
    swiftconn = get_s3conn()
    objectname = 'test-object'
    swiftconn.put_random_object(bucket, objectname)
    # Delete object with S3
    s3conn = get_s3conn()
    s3conn.delete_object(bucket, objectname)
    # Check list of objects
    eq(s3conn.list_objects(bucket), [])
    eq(swiftconn.list_objects(bucket), [])


def test_copy_swift_object():
    # Create buckets
    s3conn = get_s3conn()
    bucket = create_valid_name()
    s3conn.put_bucket(bucket)
    destination_bucket = create_valid_name()
    s3conn.put_bucket(destination_bucket)
    # Create object using S3
    s3conn = get_s3conn()
    objectname = 'test-object'
    destination_objectname = 'test-object'
    s3conn.put_random_object(bucket, objectname)
    # Copy object using Swift
    swiftconn = get_swiftconn()
    swiftconn.copy_object(bucket, objectname, destination_bucket,
                          destination_objectname)
    # Check list of objects
    eq(s3conn.list_objects(destination_bucket), [destination_objectname])
    eq(swiftconn.list_objects(destination_bucket), [objectname])
    # Check checksum
    eq(s3conn.get_md5(bucket, objectname),
       s3conn.get_md5(destination_bucket, destination_objectname))
    eq(swiftconn.get_md5(bucket, objectname),
       swiftconn.get_md5(destination_bucket, destination_objectname))
    # Check size
    eq(s3conn.get_size(bucket, objectname),
       s3conn.get_size(destination_bucket, destination_objectname))
    eq(swiftconn.get_size(bucket, objectname),
       swiftconn.get_size(destination_bucket, destination_objectname))


def test_copy_s3_object():
    # Create containers
    swiftconn = get_swiftconn()
    bucket = create_valid_name()
    swiftconn.put_container(bucket)
    destination_bucket = create_valid_name()
    swiftconn.put_container(destination_bucket)
    # Create object using Swift
    swiftconn = get_s3conn()
    objectname = 'test-object'
    destination_objectname = 'test-object'
    swiftconn.put_random_object(bucket, objectname)
    # Copy object using S3
    s3conn = get_s3conn()
    s3conn.copy_object(bucket, objectname, destination_bucket,
                       destination_objectname)
    # Check list of objects
    eq(s3conn.list_objects(destination_bucket), [destination_objectname])
    eq(swiftconn.list_objects(destination_bucket), [objectname])
    # Check checksum
    eq(s3conn.get_md5(bucket, objectname),
       s3conn.get_md5(destination_bucket, destination_objectname))
    eq(swiftconn.get_md5(bucket, objectname),
       swiftconn.get_md5(destination_bucket, destination_objectname))
    # Check size
    eq(s3conn.get_size(bucket, objectname),
       s3conn.get_size(destination_bucket, destination_objectname))
    eq(swiftconn.get_size(bucket, objectname),
       swiftconn.get_size(destination_bucket, destination_objectname))


def test_size_accounting_s3_objects():
    # Create bucket
    s3conn = get_s3conn()
    bucket = create_valid_name()
    s3conn.put_bucket(bucket)
    # Create a random number of objects using S3
    num_files = random.randint(1, 10)
    for i in range(num_files):
        objectname = 'test-object' + str(i)
        s3conn.put_random_object(bucket, objectname)
    # Sizes must be equal
    swiftconn = get_swiftconn()
    eq(s3conn.get_size(bucket), swiftconn.get_size(bucket))


def test_size_accounting_swift_objects():
    # Create bucket
    swiftconn = get_swiftconn()
    bucket = create_valid_name()
    swiftconn.put_container(bucket)
    # Create a random number of objects using Swift
    num_files = random.randint(1, 10)
    for i in range(num_files):
        objectname = 'test-object' + str(i)
        swiftconn.put_random_object(bucket, objectname)
    # Sizes must be equal
    s3conn = get_swiftconn()
    eq(s3conn.get_size(bucket), swiftconn.get_size(bucket))


def test_size_accounting_mixed_objects():
    # Create bucket
    s3conn = get_s3conn()
    swiftconn = get_swiftconn()
    bucket = create_valid_name()
    s3conn.put_bucket(bucket)
    # Create a random number of objects using S3/Swift
    num_files = random.randint(1, 10)
    for i in range(num_files):
        objectname = 'test-object' + str(i)
        random.choice([swiftconn.put_random_object,
                      s3conn.put_random_object])(bucket, objectname)
    # Sizes must be equal
    eq(s3conn.get_size(bucket), swiftconn.get_size(bucket))


def test_size_accounting_remove_mixed_objects():
    # Create bucket
    s3conn = get_s3conn()
    swiftconn = get_swiftconn()
    bucket = create_valid_name()
    s3conn.put_bucket(bucket)
    # Create a random number of objects using S3/Swift
    num_files = random.randint(1, 10)
    for i in range(num_files):
        objectname = 'test-object' + str(i)
        random.choice([swiftconn.put_random_object,
                      s3conn.put_random_object])(bucket, objectname)
    # Remove a random number of objects using S3/Swift
    remove_files = random.randint(1, num_files)
    for i in range(remove_files):
        objectname = 'test-object' + str(i)
        random.choice([swiftconn.delete_object,
                      s3conn.delete_object])(bucket, objectname)
    # Sizes must be equal
    eq(s3conn.get_size(bucket), swiftconn.get_size(bucket))


def generate_random_string(length=8):
    return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz1234567890') for x in range(length))

def test_object_custom_swift_metadata():
    bucket = create_valid_name()
    objectname = 'metadata-object'
    text = 'object with user metadata'
    # Create bucket and object
    swiftconn = get_swiftconn()
    swiftconn.put_container(bucket)
    swiftconn.put_object(bucket, objectname, text)
    # Create a random number of metadata tags using S3
    num_files = random.randint(1, 10)
    metadata = []
    for i in range(num_files):
        key = generate_random_string()
        value = generate_random_string()
        metadata += [(key, value)]
    swiftconn.add_metadata(bucket, objectname, metadata)
    s3conn = get_s3conn()
    eq(sorted(metadata), sorted(swiftconn.list_metadata(bucket, objectname)))
    eq(sorted(metadata), sorted(s3conn.list_metadata(bucket, objectname)))

def test_object_custom_s3_metadata():
    bucket = create_valid_name()
    objectname = 'metadata-object'
    text = 'object with user metadata'
    # Create bucket and object
    s3conn = get_s3conn()
    s3conn.put_bucket(bucket)
    s3conn.put_object(bucket, objectname, text)
    # Create a random number of metadata tags using S3
    num_files = random.randint(1, 10)
    metadata = []
    for i in range(num_files):
        key = generate_random_string()
        value = generate_random_string()
        metadata += [(key, value)]
    s3conn.add_metadata(bucket, objectname, metadata)
    swiftconn = get_swiftconn()
    eq(sorted(metadata), sorted(swiftconn.list_metadata(bucket, objectname)))
    eq(sorted(metadata), sorted(s3conn.list_metadata(bucket, objectname)))

# NOTE: Adding S3/Swift metadata overwrites already existing custom metadata