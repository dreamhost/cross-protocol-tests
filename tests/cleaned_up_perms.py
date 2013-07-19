### FIX LATER
from tools import *

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
def create_s3_container_with_acl(acl_headers):
    # Create S3 container
    ### FIXME
    s3conn = get_s3conn()
    bucket = create_valid_name()
    s3conn.put_bucket(bucket)
    return bucket
def delete_all():
    # Deletes all containers, both empty and nonempty
    swiftconn = get_swiftconn()
    for container in swiftconn.list_containers():
        objects = swiftconn.list_objects(container)
        for name in objects:
            swiftconn.delete_object(container, name)
        swiftconn.delete_container(container)

class SwiftContainerReadPermissions(object):
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

    # Create a Swift container (public read) then create various objects with
    # different permissions and try to read the object using a second user
    # as well as an unauthenticated user

    def setUp(self):
        # Create a Swift public read container
        self.bucket = \
        create_swift_container_with_acl({'x-container-read':'.r:*'})
    def tearDown(self):
        delete_all()

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
    def setUp(self):
        # Create a Swift private read container
        self.bucket = create_swift_container_with_acl({'x-container-read':username})
    def tearDown(self):
        delete_all()

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
        assert_raises(httplib.HTTPException, unauthuser.get_contents,
            bucket, filename)

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
        assert_raises(httplib.HTTPException, unauthuser.get_contents,
            bucket, filename)

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