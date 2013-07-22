import tools
from tools import get_config
from tools import assert_raises
from tools import create_valid_name
from tools import get_s3conn
from tools import get_swiftconn
from tools import get_s3user
from tools import get_swiftuser
from tools import get_unauthuser

import httplib

import unittest
from nose.tools import eq_ as eq


conf = get_config()
username = conf['user']

def create_swift_container_with_acl(acl_headers):
    # Create Swift container
    swiftconn = get_swiftconn()
    container = create_valid_name()
    ### Note: Bobtail does not support updating container ACLs using PUT
    swiftconn.put_container(container)
    swiftconn.post_container(container,acl_headers)
    return container
def create_s3_bucket_with_acl(permission, username=None):
    # Create S3 container
    s3conn = get_s3conn()
    bucket = create_valid_name()
    s3conn.put_bucket(bucket)
    if username:
        s3conn.add_private_acl(permission, username, bucket)
        return bucket
    s3conn.add_public_acl(permission, bucket)
    return bucket
def delete_bucket(name):
    # Delete bucket (and objects inside it)
    s3conn = get_s3conn()
    bucket = s3conn.get_bucket(name)
    keys = bucket.list()
    for key in keys:
        key.delete()
    s3conn.delete_bucket(bucket)


class SwiftContainerReadPermissions(object):
    # Basic test cases for Swift container read permissions:
    # Use the second user to read files with different permissions

    def test_read_default_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket, filename, text)
        # Read object using S3 (second user)
        s3user = get_s3user()
        eq(s3user.get_contents(bucket, filename), text)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.get_contents(bucket, filename), text)
    def test_read_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # Read object using S3 (second user)
        s3user = get_s3user()
        eq(s3user.get_contents(bucket, filename), text)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.get_contents(bucket, filename), text)
    def test_read_public_read_s3_object(self):
        bucket = self.bucket
        filename = 'public-read-s3-object'
        text = 'public read s3 object'
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Read object using S3 (second user)
        s3user = get_s3user()
        eq(s3user.get_contents(bucket, filename), text)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.get_contents(bucket, filename), text)
    def test_read_private_read_s3_object(self):
        bucket = self.bucket
        filename = 'private-read-s3-object'
        text = 'private read s3 object'
        # Create private read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Read object using S3 (second user)
        s3user = get_s3user()
        eq(s3user.get_contents(bucket, filename), text)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.get_contents(bucket, filename), text)
    def test_read_public_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'public-full-control-s3-object'
        text = 'public full control s3 object'
        # Create public full control S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Read object using S3 (second user)
        s3user = get_s3user()
        eq(s3user.get_contents(bucket, filename), text)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.get_contents(bucket, filename), text)
    def test_read_private_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'private-full-control-s3-object'
        text = 'private full control s3 object'
        # Create private full control S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket,filename,text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Read object using S3 (second user)
        s3user = get_s3user()
        eq(s3user.get_contents(bucket, filename), text)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.get_contents(bucket, filename), text)

class TestPublicReadSwiftContainer(unittest.TestCase, SwiftContainerReadPermissions):
    # NOTE: A swift public read container means ANY user should be able to
    # read the objects in the bucket

    # This does not mean that any user can list the objects in a container -
    # list object permissions can be set by using '.rlistings: username'
    # which is currently not supported in the RadosGW

    def setUp(self):
        # Create a Swift public read container
        self.bucket = \
        create_swift_container_with_acl({'x-container-read':'.r:*'})
    def tearDown(self):
        delete_bucket(self.bucket)

    # Use the unauth user to read files with different permissions

    def test_unauthuser_read_default_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket, filename, text)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)

    def test_unauthuser_read_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)

    def test_unauthuser_read_public_read_s3_object(self):
        bucket = self.bucket
        filename = 'public-read-s3-object'
        text = 'public read s3 object'
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)

    def test_unauthuser_read_private_read_s3_object(self):
        bucket = self.bucket
        filename = 'private-read-s3-object'
        text = 'private read s3 object'
        # Create private read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)

    def test_unauthuser_read_public_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'public-full-control-s3-object'
        text = 'public full control s3 object'
        # Create public full control S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)

    def test_unauthuser_read_private_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'private-full-control-s3-object'
        text = 'private full control s3 object'
        # Create private full control S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket,filename,text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)

class TestPrivateReadSwiftContainer(unittest.TestCase, SwiftContainerReadPermissions):
    # NOTE: A swift private read container means only the specified user should
    # be able to read the objects in the bucket

    def setUp(self):
        # Create a Swift public read container
        self.bucket = \
        create_swift_container_with_acl({'x-container-read':username})
    def tearDown(self):
        delete_bucket(self.bucket)

    # Use the unauth user to read files with different permissions

    def test_unauthuser_read_default_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket, filename, text)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        assert_raises(httplib.HTTPException, unauthuser.get_contents,
            bucket, filename)

    def test_unauthuser_read_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        assert_raises(httplib.HTTPException, unauthuser.get_contents,
            bucket, filename)

    def test_unauthuser_read_public_read_s3_object(self):
        bucket = self.bucket
        filename = 'public-read-s3-object'
        text = 'public read s3 object'
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)


    def test_unauthuser_read_private_read_s3_object(self):
        bucket = self.bucket
        filename = 'private-read-s3-object'
        text = 'private read s3 object'
        # Create private read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        assert_raises(httplib.HTTPException, unauthuser.get_contents,
            bucket, filename)

    def test_unauthuser_read_public_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'public-full-control-s3-object'
        text = 'public full control s3 object'
        # Create public full control S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)

    def test_unauthuser_read_private_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'private-full-control-s3-object'
        text = 'private full control s3 object'
        # Create private full control S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket,filename,text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        assert_raises(httplib.HTTPException, unauthuser.get_contents,
            bucket, filename)

class SwiftContainerWritePermissions(object):
    # Basic test cases for Swift container write permissions
    # Use the second user to create and delete files
    def test_create_default_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        # Create Swift object with second user
        swiftuser = get_swiftuser()
        swiftuser.put_object(bucket, filename, text)
        # Check that it was created
        eq(swiftuser.get_contents(bucket, filename), text)
        # Delete with Swift
        swiftconn = get_swiftconn()
        swiftconn.delete_object(bucket, filename)
        # Check that it was deleted
        assert_raises(swiftclient.ClientException, swiftconn.delete_object, bucket, filename)

        # Create Swift object with second user
        swiftuser.put_object(bucket, filename, text)
        # Check that it was created
        eq(swiftuser.get_contents(bucket, filename), text)
        # Delete with S3
        s3conn = get_s3conn()
        s3conn.delete_object(bucket, filename)
        # Check that it was deleted
        assert_raises(boto.exception.S3ResponseError, s3conn.delete_object, bucket, filename)
    def test_create_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object with second user
        s3user = get_s3user()
        s3user.put_object(bucket, filename, text)
        # Check that it was created
        eq(s3user.get_contents(bucket, filename), text)
        # Delete with Swift
        swiftconn = get_swiftconn()
        swiftconn.delete_object(bucket, filename)
        # Check that it was deleted
        assert_raises(swiftclient.ClientException, swiftconn.delete_object, bucket, filename)

        # Create S3 object with second user
        s3user.put_object(bucket, filename, text)
        # Check that it was created
        eq(s3user.get_contents(bucket, filename), text)
        # Delete with S3
        s3conn = get_s3conn()
        s3conn.delete_object(bucket, filename)
        # Check that it was deleted
        assert_raises(boto.exception.S3ResponseError, s3conn.delete_object, bucket, filename)
    def test_delete_default_swift_object(self):
        # Create Swift object (main user)
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket, filename, text)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(swiftconn.list_objects(bucket), [])

        # Create Swift object (main user)
        swiftconn.put_object(bucket, filename, text)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
    def test_delete_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(s3conn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
        eq(s3conn.list_objects(bucket), [])
    def test_delete_public_read_s3_object(self):
        bucket = self.bucket
        filename = 'public-read-s3-object'
        text = 'public read s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(s3conn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_delete_private_read_s3_object(self):
        bucket = self.bucket
        filename = 'private-read-s3-object'
        text = 'private read s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(s3conn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_delete_public_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'public-full-control-s3-object'
        text = 'public full control s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(s3conn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_delete_private_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'private-full-control-s3-object'
        text = 'private full control s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(s3conn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])

class TestPublicWriteSwiftContainer(unittest.TestCase, SwiftContainerWritePermissions):
    # NOTE: A swift public write container means ANY user should be able to
    # create and delete the objects in the bucket

    # Create a Swift container (public write) then create/delete various
    # objects with different permissions using the second/unauthenticated user
    def setUp(self):
        # Create a Swift public write container
        self.bucket = \
        create_swift_container_with_acl({'x-container-write':'.r:*'})
    def tearDown(self):
        delete_bucket(self.bucket)

    def test_unauthuser_create(self):
        bucket = self.bucket
        filename = 'default-object'
        text = 'default object'
        # Create object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.put_object(bucket, filename, text)
        # Check that it was created
        swiftconn = get_swiftconn()
        eq(swiftconn.list_objects(bucket), [filename])
        # Delete with Swift
        swiftconn.delete_object(bucket, filename)

        # Create object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.put_object(bucket,filename,text)
        # Check that it was created
        s3conn = get_s3conn()
        eq(s3conn.list_objects(bucket), [filename])
        # Delete with S3
        s3conn.delete_object(bucket, filename)
    def test_unauthuser_delete_default_swift_object(self):
        # Create Swift object (main user)
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
    def test_unauthuser_delete_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket,filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_unauthuser_delete_public_read_s3_object(self):
        bucket = self.bucket
        filename = 'public-read-s3-object'
        text = 'public read s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_unauthuser_delete_private_read_s3_object(self):
        bucket = self.bucket
        filename = 'private-read-s3-object'
        text = 'private read s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_unauthuser_delete_public_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'public-full-control-s3-object'
        text = 'public full control s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_unauthuser_delete_private_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'private-full-control-s3-object'
        text = 'private full control s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])

class TestPrivateWriteSwiftContainer(unittest.TestCase, SwiftContainerWritePermissions):
    def setUp(self):
        # Create a Swift public write container
        self.bucket = \
        create_swift_container_with_acl({'x-container-write':username})
    def tearDown(self):
        delete_bucket(self.bucket)

    def test_unauthuser_create(self):
        bucket = self.bucket
        filename = 'default-object'
        text = 'default object'
        # Create object with unauthenticated user
        unauthuser = get_unauthuser()
        assert_raises(httplib.HTTPException, unauthuser.put_object,
            bucket, filename, text)
    def test_unauthuser_delete_default_swift_object(self):
        # Create Swift object (main user)
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        # Delete should fail
        assert_raises(httplib.HTTPException, unauthuser.delete_object,
            bucket, filename)
    def test_unauthuser_delete_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object (main user)
        s3conn = get_swiftconn()
        s3conn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        # Delete should fail
        assert_raises(httplib.HTTPException, unauthuser.delete_object,
            bucket, filename)
    def test_unauthuser_delete_public_read_s3_object(self):
        bucket = self.bucket
        filename = 'public-read-s3-object'
        text = 'public read s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        # Delete should fail
        assert_raises(httplib.HTTPException, unauthuser.delete_object,
            bucket, filename)
    def test_unauthuser_delete_private_read_s3_object(self):
        bucket = self.bucket
        filename = 'private-read-s3-object'
        text = 'private read s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        # Delete should fail
        assert_raises(httplib.HTTPException, unauthuser.delete_object,
            bucket, filename)
    def test_unauthuser_delete_public_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'public-full-control-s3-object'
        text = 'public full control s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        # Delete should fail
        assert_raises(httplib.HTTPException, unauthuser.delete_object,
            bucket, filename)
    def test_unauthuser_delete_private_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'private-full-control-s3-object'
        text = 'private full control s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        # Delete should fail
        assert_raises(httplib.HTTPException, unauthuser.delete_object,
            bucket, filename)

class S3BucketReadPermissions(object):
    def test_default_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket, filename, text)
        # List objects using S3 (second user)
        s3user = get_s3user()
        eq(s3user.list_objects(bucket, filename), [filename])
        # List objects using Swift (second user)
        swiftuser = get_swiftuser()
        eq(swiftuser.list_objects(bucket, filename), [filename])
    def test_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # List objects using S3 (second user)
        s3user = get_s3user()
        eq(s3user.list_objects(bucket, filename), [filename])
        # List objects using Swift (second user)
        swiftuser = get_swiftuser()
        eq(swiftuser.list_objects(bucket, filename), [filename])
    def test_public_read_s3_object(self):
        bucket = self.bucket
        filename = 'public-read-s3-object'
        text = 'public read s3 object'
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # List objects using S3 (second user)
        s3user = get_s3user()
        eq(s3user.list_objects(bucket, filename), [filename])
        # List objects using Swift (second user)
        swiftuser = get_swiftuser()
        eq(swiftuser.list_objects(bucket, filename), [filename])
    def test_private_read_s3_object(self):
        bucket = self.bucket
        filename = 'private-read-s3-object'
        text = 'private read s3 object'
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # List objects using S3 (second user)
        s3user = get_s3user()
        eq(s3user.list_objects(bucket, filename), [filename])
        # List objects using Swift (second user)
        swiftuser = get_swiftuser()
        eq(swiftuser.list_objects(bucket, filename), [filename])
    def test_public_full_control_s3_object(self):
        # Create public read S3 object (main user)
        bucket = self.bucket
        filename = 'public-full-control-s3-object'
        text = 'public full control s3 object'
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # List objects using S3 (second user)
        s3user = get_s3user()
        eq(s3user.list_objects(bucket, filename), [filename])
        # List objects using Swift (second user)
        swiftuser = get_swiftuser()
        eq(swiftuser.list_objects(bucket, filename), [filename])
    def test_private_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'private-full-control-s3-object'
        text = 'private full control s3 object'
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket,filename,text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # List objects using S3 (second user)
        s3user = get_s3user()
        eq(s3user.list_objects(bucket, filename), [filename])
        # List objects using Swift (second user)
        swiftuser = get_swiftuser()
        eq(swiftuser.list_objects(bucket, filename), [filename])

class TestPublicReadS3Bucket(unittest.TestCase, S3BucketReadPermissions):

    def setUp(self):
        # Create an S3 public read bucket
        self.bucket = create_s3_bucket_with_acl('READ')
    def tearDown(self):
        delete_bucket(self.bucket)

    def test_default_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket, filename, text)
        # List objects using unauthenticated user
        unauthuser = get_unauthuser()
        s3conn = get_s3conn()
        eq(unauthuser.list_objects(bucket), s3conn.compare_list_objects(bucket))
    def test_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # List objects using unauthenticated user
        unauthuser = get_unauthuser()
        s3conn = get_s3conn()
        eq(unauthuser.list_objects(bucket), s3conn.compare_list_objects(bucket))
    def test_public_read_s3_object(self):
        bucket = self.bucket
        filename = 'public-read-s3-object'
        text = 'public read s3 object'
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # List objects using unauthenticated user
        unauthuser = get_unauthuser()
        s3conn = get_s3conn()
        eq(unauthuser.list_objects(bucket), s3conn.compare_list_objects(bucket))
    def test_private_read_s3_object(self):
        bucket = self.bucket
        filename = 'private-read-s3-object'
        text = 'private read s3 object'
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # List objects using unauthenticated user
        unauthuser = get_unauthuser()
        s3conn = get_s3conn()
        eq(unauthuser.list_objects(bucket), s3conn.compare_list_objects(bucket))
    def test_public_full_control_s3_object(self):
        # Create public read S3 object (main user)
        bucket = self.bucket
        filename = 'public-full-control-s3-object'
        text = 'public full control s3 object'
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # List objects using unauthenticated user
        unauthuser = get_unauthuser()
        s3conn = get_s3conn()
        eq(unauthuser.list_objects(bucket), s3conn.compare_list_objects(bucket))
    def test_private_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'private-full-control-s3-object'
        text = 'private full control s3 object'
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket,filename,text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # List objects using unauthenticated user
        unauthuser = get_unauthuser()
        s3conn = get_s3conn()
        eq(unauthuser.list_objects(bucket), s3conn.compare_list_objects(bucket))

class TestPrivateReadS3Bucket(unittest.TestCase, S3BucketReadPermissions):

    def setUp(self):
        # Create an S3 public read bucket
        self.bucket = create_s3_bucket_with_acl('READ', username)
    def tearDown(self):
        delete_bucket(self.bucket)

    def test_default_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket, filename, text)
        # List objects using unauthenticated user (should fail)
        unauthuser = get_unauthuser()
        assert_raises(httplib.HTTPException, unauthuser.list_objects,
            bucket)
    def test_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # List objects using unauthenticated user (should fail)
        unauthuser = get_unauthuser()
        assert_raises(httplib.HTTPException, unauthuser.list_objects,
            bucket)
    def test_public_read_s3_object(self):
        bucket = self.bucket
        filename = 'public-read-s3-object'
        text = 'public read s3 object'
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # List objects using unauthenticated user (should fail)
        unauthuser = get_unauthuser()
        assert_raises(httplib.HTTPException, unauthuser.list_objects,
            bucket)
    def test_private_read_s3_object(self):
        bucket = self.bucket
        filename = 'private-read-s3-object'
        text = 'private read s3 object'
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # List objects using unauthenticated user (should fail)
        unauthuser = get_unauthuser()
        assert_raises(httplib.HTTPException, unauthuser.list_objects,
            bucket)
    def test_public_full_control_s3_object(self):
        # Create public read S3 object (main user)
        bucket = self.bucket
        filename = 'public-full-control-s3-object'
        text = 'public full control s3 object'
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # List objects using unauthenticated user (should fail)
        unauthuser = get_unauthuser()
        assert_raises(httplib.HTTPException, unauthuser.list_objects,
            bucket)
    def test_private_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'private-full-control-s3-object'
        text = 'private full control s3 object'
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket,filename,text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # List objects using unauthenticated user (should fail)
        unauthuser = get_unauthuser()
        assert_raises(httplib.HTTPException, unauthuser.list_objects,
            bucket)

class S3BucketWritePermissions(object):
    def test_create_default_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        # Create Swift object with second user
        swiftuser = get_swiftuser()
        swiftuser.put_object(bucket, filename, text)
        # Check that it was created
        eq(swiftuser.get_contents(bucket, filename), text)
        # Delete with Swift
        swiftconn = get_swiftconn()
        swiftconn.delete_object(bucket, filename)

        # Create Swift object with second user
        swiftuser.put_object(bucket, filename, text)
        # Check that it was created
        eq(swiftuser.get_contents(bucket, filename), text)
        # Delete with S3
        s3conn = get_s3conn()
        s3conn.delete_object(bucket, filename)
    def test_create_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object with second user
        s3user = get_s3user()
        s3user.put_object(bucket, filename, text)
        # Check that it was created
        eq(s3user.get_contents(bucket, filename), text)
        # Delete with Swift
        swiftconn = get_swiftconn()
        swiftconn.delete_object(bucket, filename)

        # Create S3 object with second user
        s3user.put_object(bucket, filename, text)
        # Check that it was created
        eq(s3user.get_contents(bucket, filename), text)
        # Delete with S3
        s3conn = get_s3conn()
        s3conn.delete_object(bucket, filename)
    def test_delete_default_swift_object(self):
        # Create Swift object (main user)
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket, filename, text)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(swiftconn.list_objects(bucket), [])

        # Create Swift object (main user)
        swiftconn.put_object(bucket, filename, text)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
    def test_delete_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(s3conn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_delete_public_read_s3_object(self):
        bucket = self.bucket
        filename = 'public-read-s3-object'
        text = 'public read s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(s3conn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_delete_private_read_s3_object(self):
        bucket = self.bucket
        filename = 'private-read-s3-object'
        text = 'private read s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(s3conn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_delete_public_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'public-full-control-s3-object'
        text = 'public full control s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(s3conn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_delete_private_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'private-full-control-s3-object'
        text = 'private full control s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(s3conn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])

class TestPublicWriteS3Bucket(unittest.TestCase, S3BucketWritePermissions):

    def setUp(self):
        # Create a Swift public write container
        self.bucket = \
        create_swift_container_with_acl({'x-container-write':'.r:*'})
    def tearDown(self):
        delete_bucket(self.bucket)

    def test_unauthuser_create(self):
        bucket = self.bucket
        filename = 'default-object'
        text = 'default object'
        # Create object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.put_object(bucket, filename, text)
        # Check that it was created
        swiftconn = get_swiftconn()
        eq(swiftconn.list_objects(bucket), [filename])
        # Delete with Swift
        swiftconn.delete_object(bucket, filename)

        # Create object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.put_object(bucket,filename,text)
        # Check that it was created
        s3conn = get_s3conn()
        eq(s3conn.list_objects(bucket), [filename])
        # Delete with S3
        s3conn.delete_object(bucket, filename)
    def test_unauthuser_delete_default_swift_object(self):
        # Create Swift object (main user)
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
    def test_unauthuser_delete_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket,filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_unauthuser_delete_public_read_s3_object(self):
        bucket = self.bucket
        filename = 'public-read-s3-object'
        text = 'public read s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_unauthuser_delete_private_read_s3_object(self):
        bucket = self.bucket
        filename = 'private-read-s3-object'
        text = 'private read s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_unauthuser_delete_public_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'public-full-control-s3-object'
        text = 'public full control s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_unauthuser_delete_private_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'private-full-control-s3-object'
        text = 'private full control s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])

class TestPrivateWriteS3Bucket(unittest.TestCase, S3BucketWritePermissions):

    def setUp(self):
        # Create a Swift public write container
        self.bucket = \
        create_swift_container_with_acl({'x-container-write':username})
    def tearDown(self):
        delete_bucket(self.bucket)

    def test_unauthuser_create(self):
        bucket = self.bucket
        filename = 'default-object'
        text = 'default object'
        # Create object with unauthenticated user
        unauthuser = get_unauthuser()
        assert_raises(httplib.HTTPException, unauthuser.put_object,
            bucket, filename, text)
    def test_unauthuser_delete_default_swift_object(self):
        # Create Swift object (main user)
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        # Delete should fail
        assert_raises(httplib.HTTPException, unauthuser.delete_object,
            bucket, filename)
    def test_unauthuser_delete_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        # Delete should fail
        assert_raises(httplib.HTTPException, unauthuser.delete_object,
            bucket, filename)
    def test_unauthuser_delete_public_read_s3_object(self):
        bucket = self.bucket
        filename = 'public-read-s3-object'
        text = 'public read s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        # Delete should fail
        assert_raises(httplib.HTTPException, unauthuser.delete_object,
            bucket, filename)
    def test_unauthuser_delete_private_read_s3_object(self):
        bucket = self.bucket
        filename = 'private-read-s3-object'
        text = 'private read s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        # Delete should fail
        assert_raises(httplib.HTTPException, unauthuser.delete_object,
            bucket, filename)
    def test_unauthuser_delete_public_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'public-full-control-s3-object'
        text = 'public full control s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        # Delete should fail
        assert_raises(httplib.HTTPException, unauthuser.delete_object,
            bucket, filename)
    def test_unauthuser_delete_private_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'private-full-control-s3-object'
        text = 'private full control s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        # Delete should fail
        assert_raises(httplib.HTTPException, unauthuser.delete_object,
            bucket, filename)

class CrossBucketWritePermissions(object):
    def test_create_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        # Create Swift object with second user
        swiftuser = get_swiftuser()
        swiftuser.put_object(bucket, filename, text)
        # Check that it was created
        eq(swiftuser.get_contents(bucket, filename), text)
    def test_create_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object with second user
        s3user = get_s3user()
        s3user.put_object(bucket, filename, text)
        # Check that it was created
        eq(s3user.get_contents(bucket, filename), text)
    def test_delete_swift_object(self):
        # Create Swift object (main user)
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket, filename, text)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])

        # Create Swift object (main user)
        swiftconn.put_object(bucket, filename, text)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
    def test_delete_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])

class TestPrivateWriteSwiftContainerPublicWriteS3Bucket(unittest.TestCase, CrossBucketWritePermissions):

    def setUp(self):
        # Create a Swift public write container
        self.bucket = \
        create_swift_container_with_acl({'x-container-write':username})
        s3conn = get_s3conn()
        s3conn.add_public_acl('WRITE', self.bucket)
    def tearDown(self):
        delete_bucket(self.bucket)
    def test_unauthuser_create(self):
        bucket = self.bucket
        filename = 'default-object'
        text = 'default object'
        # Create object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.put_object(bucket, filename, text)
        # Check that it was created
        swiftconn = get_swiftconn()
        eq(swiftconn.list_objects(bucket), [filename])
        # Delete with Swift
        swiftconn.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])

        # Create object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.put_object(bucket,filename,text)
        # Check that it was created
        s3conn = get_s3conn()
        eq(s3conn.list_objects(bucket), [filename])
        # Delete with S3
        s3conn.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])
    def test_unauthuser_delete_default_swift_object(self):
        # Create Swift object (main user)
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
    def test_unauthuser_delete_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket,filename)
        # Check that bucket is empty
        eq(s3conn.list_objects(bucket), [])

class TestPrivateWriteS3BucketPublicWriteSwiftContainer(unittest.TestCase, CrossBucketWritePermissions):

    def setUp(self):
        # Create a Swift public write container
        self.bucket = \
        create_swift_container_with_acl({'x-container-write':'.r:*'})
        s3conn = get_s3conn()
        s3conn.add_private_acl('WRITE', username, self.bucket)
    def tearDown(self):
        delete_bucket(self.bucket)
    def test_unauthuser_create(self):
        bucket = self.bucket
        filename = 'default-object'
        text = 'default object'
        
        # Create object with unauthenticated user
        unauthuser = get_unauthuser()
        # Should fail
        assert_raises(httplib.HTTPException, unauthuser.put_object,
            bucket, filename, text)
    def test_unauthuser_delete_default_swift_object(self):
        # Create Swift object (main user)
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'default swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        # Should fail
        assert_raises(httplib.HTTPException, unauthuser.delete_object,
            bucket, filename)
    def test_unauthuser_delete_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'default s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        # Should fail
        assert_raises(httplib.HTTPException, unauthuser.delete_object,
            bucket, filename)