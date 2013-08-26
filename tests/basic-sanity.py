import random
import StringIO

import boto
import boto.s3.connection
from nose.tools import eq_ as eq
import swiftclient

from tools import assert_raises
from tools import create_valid_utf8_name
from tools import create_valid_name
from tools import delete_buckets
from tools import generate_random_string
from tools import get_s3conn
from tools import get_swiftconn


class TestBasicCrossProtocolOperations():

    def create_bucket_name(self):
        return create_valid_name()

    def create_name(self):
        return create_valid_name()

    # TEST BUCKET OPERATIONS:

    def test_list_buckets(self):
        # Delete all buckets
        delete_buckets()
        # Create bucket
        s3conn = get_s3conn()
        bucket = self.create_bucket_name()
        s3conn.put_bucket(bucket)
        # Check list buckets
        eq(s3conn.list_buckets(), [bucket])
        swiftconn = get_swiftconn()
        eq(swiftconn.list_containers(), [bucket])

    def test_list_container(self):
        # Delete all buckets
        delete_buckets()
        # Create container
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
        swiftconn.put_container(bucket)
        # Check list containers
        eq(swiftconn.list_containers(), [bucket])
        s3conn = get_s3conn()
        eq(s3conn.list_buckets(), [bucket])

    def test_list_empty_bucket(self):
        # Create bucket
        s3conn = get_s3conn()
        bucket = self.create_bucket_name()
        s3conn.put_bucket(bucket)
        # Get list of objects
        swiftconn = get_swiftconn()
        eq(swiftconn.list_objects(bucket), [])
        eq(s3conn.list_objects(bucket), [])

    def test_list_empty_container(self):
        # Create container
        bucket = self.create_bucket_name()
        swiftconn = get_swiftconn()
        swiftconn.put_container(bucket)
        # Get list of objects
        s3conn = get_s3conn()
        eq(s3conn.list_objects(bucket), [])
        eq(swiftconn.list_objects(bucket), [])

    def test_list_nonempty_bucket(self):
        # Create bucket
        s3conn = get_s3conn()
        bucket = self.create_bucket_name()
        s3conn.put_bucket(bucket)
        # Add test object using S3
        objectname = self.create_name()
        text = 'test object text'
        s3conn.put_object(bucket, objectname, text)
        # Get list of objects using Swift
        swiftconn = get_swiftconn()
        eq(swiftconn.list_objects(bucket), [objectname])

    def test_list_nonempty_container(self):
        # Create container
        bucket = self.create_bucket_name()
        swiftconn = get_swiftconn()
        swiftconn.put_container(bucket)
        # Add test object using Swift
        objectname = self.create_name()
        text = 'test object text'
        swiftconn.put_object(bucket, objectname, text)
        # Get list of objects using S3
        s3conn = get_s3conn()
        eq(s3conn.list_objects(bucket), [objectname])

    def test_delete_bucket(self):
        # Delete all buckets
        delete_buckets()
        # Create bucket using S3
        s3conn = get_s3conn()
        bucket = self.create_bucket_name()
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

    def test_delete_container(self):
        # Delete all buckets
        delete_buckets()
        # Create bucket using Swift
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
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

    def test_delete_non_empty_bucket(self):
        # Create bucket
        s3conn = get_s3conn()
        bucket = self.create_bucket_name()
        s3conn.put_bucket(bucket)
        # Add test object using S3
        objectname = self.create_name()
        text = 'test object text'
        s3conn.put_object(bucket, objectname, text)
        # Try to delete bucket using Swift
        swiftconn = get_swiftconn()
        swift_err = assert_raises(swiftclient.ClientException,
                                  swiftconn.delete_container, bucket)
        eq(swift_err.http_status, 409)
        eq(swift_err.http_reason, 'Conflict')
        eq(swift_err.http_response_content, 'BucketNotEmpty')

    def test_delete_non_empty_container(self):
        # Create container
        bucket = self.create_bucket_name()
        swiftconn = get_swiftconn()
        swiftconn.put_container(bucket)
        # Add test object using Swift
        objectname = self.create_name()
        text = 'test object text'
        swiftconn.put_object(bucket, objectname, text)
        # Try to delete container using S3
        s3conn = get_s3conn()
        s3_err = assert_raises(boto.exception.S3ResponseError,
                               s3conn.delete_bucket, bucket)
        eq(s3_err.status, 409)
        eq(s3_err.reason, 'Conflict')
        eq(s3_err.error_code, 'BucketNotEmpty')

    # TEST OBJECT OPERATIONS:

    def test_list_object_in_container(self):
        # Create container using Swift
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
        swiftconn.put_container(bucket)
        # Create object using S3
        s3conn = get_s3conn()
        objectname = self.create_name()
        text = 'test object text'
        s3conn.put_object(bucket, objectname, text)
        # Check list of objects
        eq(swiftconn.list_objects(bucket), [objectname])
        eq(s3conn.list_objects(bucket), [objectname])

    def test_list_object_in_bucket(self):
        # Create bucket using S3
        s3conn = get_s3conn()
        bucket = self.create_bucket_name()
        s3conn.put_bucket(bucket)
        # Create object using Swift
        swiftconn = get_swiftconn()
        objectname = self.create_name()
        text = 'test object text'
        swiftconn.put_object(bucket, objectname, text)
        # Check list of objects
        eq(s3conn.list_objects(bucket), [objectname])
        eq(swiftconn.list_objects(bucket), [objectname])

    def test_size_object_in_container(self):
        # Create container
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
        swiftconn.put_container(bucket)
        # Create object using S3
        s3conn = get_s3conn()
        objectname = self.create_name()
        bytes = s3conn.put_random_object(bucket, objectname)
        # Check size
        eq(bytes, swiftconn.get_size(bucket, objectname))

    def test_size_object_in_bucket(self):
        # Create bucket
        s3conn = get_s3conn()
        bucket = self.create_bucket_name()
        s3conn.put_bucket(bucket)
        # Create object using Swift
        swiftconn = get_swiftconn()
        objectname = self.create_name()
        bytes = swiftconn.put_random_object(bucket, objectname)
        # Check size
        eq(bytes, s3conn.get_size(bucket, objectname))

    def test_checksum_object_in_container(self):
        # Create container
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
        swiftconn.put_container(bucket)
        # Create object using S3
        s3conn = get_s3conn()
        objectname = self.create_name()
        s3conn.put_random_object(bucket, objectname)
        # Check checksum
        eq(s3conn.get_md5(bucket, objectname),
           swiftconn.get_md5(bucket, objectname))

    def test_checksum_object_in_bucket(self):
        # Create bucket
        s3conn = get_s3conn()
        bucket = self.create_bucket_name()
        s3conn.put_bucket(bucket)
        # Create object using Swift
        swiftconn = get_s3conn()
        objectname = self.create_name()
        swiftconn.put_random_object(bucket, objectname)
        # Check checksum
        eq(s3conn.get_md5(bucket, objectname),
           swiftconn.get_md5(bucket, objectname))

    def test_delete_object_in_bucket(self):
        # Create bucket
        s3conn = get_s3conn()
        bucket = self.create_bucket_name()
        s3conn.put_bucket(bucket)
        # Create object using S3
        s3conn = get_s3conn()
        objectname = self.create_name()
        s3conn.put_random_object(bucket, objectname)
        # Delete object with Swift
        swiftconn = get_swiftconn()
        swiftconn.delete_object(bucket, objectname)
        # Check list of objects
        eq(s3conn.list_objects(bucket), [])
        eq(swiftconn.list_objects(bucket), [])

    def test_delete_object_in_container(self):
        # Create container
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
        swiftconn.put_container(bucket)
        # Create object using Swift
        swiftconn = get_s3conn()
        objectname = self.create_name()
        swiftconn.put_random_object(bucket, objectname)
        # Delete object with S3
        s3conn = get_s3conn()
        s3conn.delete_object(bucket, objectname)
        # Check list of objects
        eq(s3conn.list_objects(bucket), [])
        eq(swiftconn.list_objects(bucket), [])

    def test_copy_s3_object(self):
        # Create buckets
        s3conn = get_s3conn()
        bucket = self.create_bucket_name()
        s3conn.put_bucket(bucket)
        destination_bucket = self.create_bucket_name()
        s3conn.put_bucket(destination_bucket)
        # Create object using S3
        s3conn = get_s3conn()
        objectname = self.create_name()
        destination_objectname = self.create_name()
        s3conn.put_random_object(bucket, objectname)
        # Copy object using Swift
        swiftconn = get_swiftconn()
        swiftconn.copy_object(bucket, objectname, destination_bucket,
                              destination_objectname)
        # Check list of objects
        eq(s3conn.list_objects(destination_bucket), [destination_objectname])
        eq(swiftconn.list_objects(destination_bucket),
           [destination_objectname])
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
        # Check contents
        eq(s3conn.get_contents(bucket, objectname),
           s3conn.get_contents(destination_bucket, destination_objectname))

    def test_copy_swift_object(self):
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
        # Copy object using S3
        s3conn = get_s3conn()
        s3conn.copy_object(bucket, objectname, destination_bucket,
                           destination_objectname)
        # Check list of objects
        eq(s3conn.list_objects(destination_bucket), [destination_objectname])
        eq(swiftconn.list_objects(destination_bucket),
           [destination_objectname])
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
        # Check contents
        eq(swiftconn.get_contents(bucket, objectname),
           swiftconn.get_contents(destination_bucket, destination_objectname))

    # TEST SIZE ACCOUNT IN S3/SWIFT BUCKETS
    def test_size_accounting_s3_objects(self):
        # Create bucket
        s3conn = get_s3conn()
        bucket = self.create_bucket_name()
        s3conn.put_bucket(bucket)
        # Create a random number of objects using S3
        num_files = random.randint(1, 10)
        total_size = 0
        for i in range(num_files):
            objectname = self.create_name() + str(i)
            total_size += s3conn.put_random_object(bucket, objectname)
        # Sizes must be equal
        swiftconn = get_swiftconn()
        eq(s3conn.get_size(bucket), swiftconn.get_size(bucket))
        eq(total_size, s3conn.get_size(bucket))
        eq(total_size, swiftconn.get_size(bucket))

    def test_size_accounting_swift_objects(self):
        # Create bucket
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
        swiftconn.put_container(bucket)
        # Create a random number of objects using Swift
        num_files = random.randint(1, 10)
        total_size = 0
        for i in range(num_files):
            objectname = self.create_name() + str(i)
            total_size += swiftconn.put_random_object(bucket, objectname)
        # Sizes must be equal
        s3conn = get_swiftconn()
        eq(s3conn.get_size(bucket), swiftconn.get_size(bucket))
        eq(total_size, s3conn.get_size(bucket))
        eq(total_size, swiftconn.get_size(bucket))

    def test_size_accounting_mixed_objects(self):
        # Create bucket
        s3conn = get_s3conn()
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
        s3conn.put_bucket(bucket)
        # Create a random number of objects using S3/Swift
        num_files = random.randint(1, 10)
        total_size = 0
        for i in range(num_files):
            objectname = self.create_name() + str(i)
            total_size += random.choice([swiftconn.put_random_object,
                          s3conn.put_random_object])(bucket, objectname)
        # Sizes must be equal
        eq(s3conn.get_size(bucket), swiftconn.get_size(bucket))
        eq(total_size, s3conn.get_size(bucket))
        eq(total_size, swiftconn.get_size(bucket))

    def test_size_accounting_remove_mixed_objects(self):
        # Create bucket
        s3conn = get_s3conn()
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
        s3conn.put_bucket(bucket)
        # Create a random number of objects using S3/Swift
        num_files = random.randint(1, 10)
        total_size = 0
        objectnames = []
        for i in range(num_files):
            objectnames += [self.create_name() + str(i)]
            total_size += random.choice([swiftconn.put_random_object,
                          s3conn.put_random_object])(bucket, objectnames[i])
        # Remove a random number of objects using S3/Swift
        remove_files = random.randint(1, num_files)
        for i in range(remove_files):
            objectname = objectnames[i]
            total_size -= random.choice([swiftconn.get_size,
                          s3conn.get_size])(bucket, objectname)
            random.choice([swiftconn.delete_object,
                          s3conn.delete_object])(bucket, objectname)
        # Sizes must be equal
        eq(s3conn.get_size(bucket), swiftconn.get_size(bucket))
        eq(total_size, s3conn.get_size(bucket))
        eq(total_size, swiftconn.get_size(bucket))

    def test_swift_object_custom_metadata(self):
        bucket = self.create_bucket_name()
        objectname = self.create_name()
        text = 'test object with user metadata'
        # Create bucket and object
        swiftconn = get_swiftconn()
        swiftconn.put_container(bucket)
        swiftconn.put_object(bucket, objectname, text)
        # Create a random number of metadata tags using Swift
        num_metadata = random.randint(1, 10)
        metadata = []
        for i in range(num_metadata):
            key = generate_random_string()
            value = generate_random_string()
            metadata += [(key, value)]
        swiftconn.add_custom_metadata(bucket, objectname, metadata)
        s3conn = get_s3conn()
        eq(sorted(metadata),
           sorted(swiftconn.list_custom_metadata(bucket, objectname)))
        eq(sorted(metadata),
           sorted(s3conn.list_custom_metadata(bucket, objectname)))

    def test_s3_object_custom_metadata(self):
        bucket = self.create_bucket_name()
        objectname = self.create_name()
        text = 'test object with user metadata'
        # Create bucket and object
        s3conn = get_s3conn()
        s3conn.put_bucket(bucket)
        s3conn.put_object(bucket, objectname, text)
        # Create a random number of metadata tags using S3
        num_metadata = random.randint(1, 10)
        metadata = []
        for i in range(num_metadata):
            key = generate_random_string()
            value = generate_random_string()
            metadata += [(key, value)]
        s3conn.add_custom_metadata(bucket, objectname, metadata)
        swiftconn = get_swiftconn()
        eq(sorted(metadata),
           sorted(swiftconn.list_custom_metadata(bucket, objectname)))
        eq(sorted(metadata), sorted(s3conn.list_custom_metadata(bucket, objectname)))

    # CHECK IF METADATA IS COPIED CORRECTLY
    def test_copy_swift_object_custom_metadata(self):
        # Create buckets
        s3conn = get_s3conn()
        bucket = self.create_bucket_name()
        s3conn.put_bucket(bucket)
        destination_bucket = self.create_bucket_name()
        s3conn.put_bucket(destination_bucket)
        # Create object using S3
        s3conn = get_s3conn()
        objectname = self.create_name()
        destination_objectname = self.create_name()
        s3conn.put_random_object(bucket, objectname)
        # Create a random number of metadata tags using S3
        num_metadata = random.randint(1, 10)
        metadata = []
        for i in range(num_metadata):
            key = generate_random_string()
            value = generate_random_string()
            metadata += [(key, value)]
        s3conn.add_custom_metadata(bucket, objectname, metadata)
        # Copy object using Swift
        swiftconn = get_swiftconn()
        swiftconn.copy_object(bucket, objectname, destination_bucket,
                              destination_objectname)
        # Check metadata
        eq(sorted(metadata), sorted(swiftconn.list_custom_metadata(destination_bucket,
                                    destination_objectname)))
        eq(sorted(metadata), sorted(s3conn.list_custom_metadata(destination_bucket,
                                    destination_objectname)))

    def test_copy_s3_object_custom_metadata(self):
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
        # Create a random number of metadata tags using Swift
        num_metadata = random.randint(1, 10)
        metadata = []
        for i in range(num_metadata):
            key = generate_random_string()
            value = generate_random_string()
            metadata += [(key, value)]
        swiftconn.add_custom_metadata(bucket, objectname, metadata)
        # Copy object using S3
        s3conn = get_s3conn()
        s3conn.copy_object(bucket, objectname, destination_bucket,
                           destination_objectname)
        # Check metadata
        eq(sorted(metadata), sorted(swiftconn.list_custom_metadata(destination_bucket,
                                    destination_objectname)))
        eq(sorted(metadata), sorted(s3conn.list_custom_metadata(destination_bucket,
                                    destination_objectname)))

    def test_s3_multipart_upload(self):
        # Create bucket
        s3conn = get_s3conn()
        bucket = self.create_bucket_name()
        objectname = 'multipart upload test object'
        s3conn.put_bucket(bucket)
        b = s3conn.get_bucket(bucket)
        mp = b.initiate_multipart_upload(objectname)
        # Create a random number of objects using S3/Swift
        num_files = random.randint(1, 10)
        total_size = 0
        for i in range(num_files):
            d = open('/dev/urandom', 'rb')
            bytes = random.randint(1, 300)
            data = d.read(bytes)
            total_size += bytes
            f = StringIO.StringIO()
            f.write(data)
            f.seek(0)
            mp.upload_part_from_file(f, i + 1)
            f.close()
            d.close()

        mp.complete_upload()

        swiftconn = get_swiftconn()
        eq(swiftconn.get_contents(bucket, objectname), s3conn.get_contents(bucket, objectname))
        eq(swiftconn.list_objects(bucket), s3conn.list_objects(bucket))
        eq(s3conn.get_md5(bucket, objectname), swiftconn.get_md5(bucket, objectname))
        eq(s3conn.get_size(bucket, objectname), swiftconn.get_size(bucket, objectname))

    def test_s3_folders(self):
        # Create bucket and object using S3
        s3conn = get_s3conn()
        bucket = self.create_bucket_name()
        s3conn.put_bucket(bucket)
        folder = self.create_name() + '/'
        objectname = folder + self.create_name()
        text = 'test object text'
        s3conn.put_object(bucket, objectname, text)
        # Check list of objects
        swiftconn = get_swiftconn()
        eq(swiftconn.list_objects(bucket), [objectname])
        eq(s3conn.list_objects(bucket), [objectname])
        # Content should be the same
        eq(s3conn.get_contents(bucket, objectname), swiftconn.get_contents(bucket, objectname))

    def test_swift_folders(self):
        # Create bucket and object using Swift
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
        swiftconn.put_container(bucket)
        folder = self.create_name() + '/'
        objectname = folder + self.create_name()
        text = 'test object text'
        swiftconn.put_object(bucket, objectname, text)
        # Check list of objects
        s3conn = get_s3conn()
        eq(swiftconn.list_objects(bucket), [objectname])
        eq(s3conn.list_objects(bucket), [objectname])
        # Content should be the same
        eq(s3conn.get_contents(bucket, objectname), swiftconn.get_contents(bucket, objectname))


class TestUTF8Objects(TestBasicCrossProtocolOperations):
    def create_name(self):
        return create_valid_utf8_name()


class TestUTF8Buckets():
    # Note: Swift can create UTF-8 encoded buckets

    def create_bucket_name(self):
        return create_valid_utf8_name()

    def create_name(self):
        return create_valid_utf8_name()
        
    def test_list_container(self):
        # Delete all buckets
        delete_buckets()
        # Create container
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
        swiftconn.put_container(bucket)
        # Check list containers
        eq(swiftconn.list_containers(), [bucket])
        s3conn = get_s3conn()
        eq(s3conn.list_buckets(), [bucket])

    # Does not pass
    """
    def test_list_empty_container(self):
        # Create container
        bucket = self.create_bucket_name()
        swiftconn = get_swiftconn()
        swiftconn.put_container(bucket)
        # Get list of objects
        s3conn = get_s3conn()
        eq(s3conn.list_objects(bucket), [])
        eq(swiftconn.list_objects(bucket), [])
        ## DOES NOT WORK

    def test_list_nonempty_container(self):
        # Create container
        bucket = self.create_bucket_name()
        swiftconn = get_swiftconn()
        swiftconn.put_container(bucket)
        # Add test object using Swift
        objectname = self.create_name()
        text = 'test object text'
        swiftconn.put_object(bucket, objectname, text)
        # Get list of objects using S3
        s3conn = get_s3conn()
        eq(s3conn.list_objects(bucket), [objectname])
        ## DOES NOT WORK

    def test_list_object_in_container(self):
        # Create container using Swift
        swiftconn = get_swiftconn()
        bucket = self.create_bucket_name()
        swiftconn.put_container(bucket)
        # Create object using S3
        s3conn = get_s3conn()
        objectname = self.create_name()
        text = 'test object text'
        s3conn.put_object(bucket, objectname, text)
        # Check list of objects
        eq(swiftconn.list_objects(bucket), [objectname])
        eq(s3conn.list_objects(bucket), [objectname])
        ## DOES NOT WORK
    """