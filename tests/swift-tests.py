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
        key = generate_random_string()
        value = generate_random_string()
        headers = {key: value}
        data = 'test object metadata'
        swiftconn.put_object(container=bucket, name=objectname, contents=data, headers=headers)

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




    # TEST BUCKET OPERATIONS:

    # Account custom metadata not allowed
    """
    def test_metadata(self):
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