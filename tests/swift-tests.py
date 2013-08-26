import random
import StringIO

from nose.tools import eq_ as eq
import swiftclient

from tools import assert_raises
from tools import create_valid_utf8_name
from tools import create_valid_name
from tools import delete_buckets
from tools import generate_random_string
from tools import get_s3conn
from tools import get_swiftconn

class TestContainer():

    def create_bucket_name(self):
        return create_valid_name()

    def create_name(self):
        return create_valid_name()

    def test_metadata(self):
        # Create container
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
        swiftconn.put_container(bucket)
        # Create object with a custom metadata tag using Swift
        objectname = self.create_name()
        swiftconn.put_random_object(bucket, objectname)
        key = generate_random_string()
        value = generate_random_string()
        metadata = [(key, value)]
        # Add metadata using a POST request
        swiftconn.add_custom_metadata(bucket, objectname, metadata)
        eq(metadata, swiftconn.list_custom_metadata(bucket,
                                    objectname))

    def test_multi_metadata(self):
        # Create container
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
        swiftconn.put_container(bucket)
        # Create object
        objectname = self.create_name()
        swiftconn.put_random_object(bucket, objectname)
        # Add a custom metadata tag using Swift
        key = generate_random_string()
        value = generate_random_string()
        metadata = [(key, value)]
        swiftconn.add_custom_metadata(bucket, objectname, metadata)
        # Check response with HEAD
        eq(sorted(metadata), sorted(swiftconn.list_custom_metadata(bucket,
                                    objectname)))
        # Add another custom metadata tag using Swift
        key = generate_random_string()
        value = generate_random_string()
        metadata += [(key, value)]
        swiftconn.add_custom_metadata(bucket, objectname, metadata)
        # Check response with HEAD
        eq(sorted(metadata), sorted(swiftconn.list_custom_metadata(bucket,
                                    objectname)))

    def test_PUT_metadata(self):
        # Create container
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
        swiftconn.put_container(bucket)
        # Create object with a custom metadata tag using Swift
        objectname = self.create_name()
        data = 'test object'
        key = generate_random_string()
        value = generate_random_string()
        headers = {key: value}
        swiftconn.put_object(bucket, objectname, contents=data, headers=headers)
        eq([(key, value)], swiftconn.list_custom_metadata(bucket,
                                    objectname))

class TestObject():

    def create_bucket_name(self):
        return create_valid_name()

    def create_name(self):
        return create_valid_name()

    def test_copy_object(self):
        # Create containers
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
        swiftconn.put_container(bucket)
        destination_bucket = self.create_bucket_name()
        swiftconn.put_container(destination_bucket)
        # Create object using Swift
        swiftconn = get_s3conn()
        objectname = self.create_name()
        destination_objectname = self.create_name()
        swiftconn.put_random_object(bucket, objectname)
        # Copy object
        swiftconn.copy_object(bucket, objectname, destination_bucket,
                           destination_objectname)
        # Check list of objects
        eq(swiftconn.list_objects(destination_bucket), [destination_objectname])
        eq(swiftconn.list_objects(destination_bucket),
           [destination_objectname])
        # Check checksum
        eq(swiftconn.get_md5(bucket, objectname),
           swiftconn.get_md5(destination_bucket, destination_objectname))
        # Check size
        eq(swiftconn.get_size(bucket, objectname),
           swiftconn.get_size(destination_bucket, destination_objectname))
        # Check contents
        eq(swiftconn.get_contents(bucket, objectname),
           swiftconn.get_contents(destination_bucket, destination_objectname))


# Account custom metadata not allowed
"""
class TestAccount():
    def test_account_metadata(self):
        swiftconn = get_swiftconn()
        # Add a custom metadata tag using Swift
        key = generate_random_string()
        value = generate_random_string()
        headers = {'x-account-meta-'+key: value}
        # POST custom metadata headers account
        swiftconn.post_account(headers=headers)
        # Check response with HEAD
        response = swiftconn.head_account()
        returned_metadata = []
        for header in response:
            if header.startswith('x-account-meta-'):
                returned_metadata += [(header[len('x-account-meta-'):], response[header])]
        eq(sorted(metadata), sorted(returned_metadata))
        # Check response with GET
        response = swiftconn.get_account()
        returned_metadata = []
        for header in response[0]:
            if header.startswith('x-account-meta-'):
                returned_metadata += [(header[len('x-account-meta-'):], response[header])]
        eq(sorted(metadata), sorted(returned_metadata))
"""