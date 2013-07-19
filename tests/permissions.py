### FIX LATER
from wrapper import *
import unittest
from nose.tools import eq_ as eq


conf = get_config()
username = conf['user']

# listing objects should fail

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
    # Delete using S3
    s3conn = get_s3conn()
    for name in s3_list_buckets(s3conn):
        bucket = s3conn.get_bucket(name)
        keys = bucket.list()
        for key in keys:
            key.delete()
        s3conn.delete_bucket(bucket)


class TestPublicReadSwiftContainer(unittest.TestCase):
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

    def test_default_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'defualt swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket, filename, text)
        # Read object using S3 (second user)
        s3user = get_s3user()
        eq(s3user.get_contents(bucket, filename), text)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.get_contents(bucket, filename), text)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)
    def test_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'defualt s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # Read object using S3 (second user)
        s3user = get_s3user()
        eq(s3user.get_contents(bucket, filename), text)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.get_contents(bucket, filename), text)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)
    def test_public_read_s3_object(self):
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
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)
    def test_private_read_s3_object(self):
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
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)
    def test_public_full_control_s3_object(self):
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
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)
    def test_private_full_control_s3_object(self):
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
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)

class TestPublicWriteSwiftContainer(unittest.TestCase):
    # NOTE: A swift public write container means ANY user should be able to
    # create and delete the objects in the bucket

    # Create a Swift container (public write) then create/delete various
    # objects with different permissions using the second/unauthenticated user

    def setUp(self):
        # Create a Swift public write container
        self.bucket = \
        create_swift_container_with_acl({'x-container-write':'.r:*'})

    def test_create_default_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'defualt swift object'
        # Create Swift object with second user
        swiftuser = get_swiftuser()
        swiftuser.put_object(bucket, filename, text)
        # Delete with Swift
        swiftconn = get_swiftconn()
        swiftconn.delete_object(bucket, filename, text)

        # Create Swift object with second user
        swiftuser.put_object(bucket, filename, text)
        # Delete with S3
        s3conn = get_s3conn()
        s3conn.delete_object(bucket, filename, text)

        # Create object with unauthenticated user
        unauthuser.put_object(bucket, filename, text)
        # Delete with Swift
        swiftconn.delete_object(bucket, filename, text)

        # Create object with unauthenticated user
        unauthuser.put_object(bucket,filename,text)
        # Delete with S3
        s3conn.delete_object(bucket, filename, text)

    def test_create_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'defualt s3 object'

        # Create S3 object with second user
        s3user = get_s3user()
        s3user.put_object(bucket, filename, text)
        # Delete with Swift
        swiftconn = get_swiftconn()
        swiftconn.delete_object(bucket, filename, text)

        # Create S3 object with second user
        s3user.put_object(bucket, filename, text)
        # Delete with S3
        s3conn = get_s3conn()
        s3conn.delete_object(bucket, filename, text)
        
        # Create object with unauthenticated user
        unauthuser.put_object(bucket, filename, text)
        # Delete with Swift
        swiftconn.delete_object(bucket, filename, text)

        # Create object with unauthenticated user
        unauthuser.put_object(bucket,filename,text)
        # Delete with S3
        s3conn.delete_object(bucket, filename, text)

    def test_delete_default_swift_object(self):
        # Create Swift object (main user)
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'defualt swift object'
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
        
        # Create Swift object (main user)
        swiftconn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket,filename)
        # Check that bucket is empty
        swiftconn.get_container(bucket)
        eq(swiftconn.list_objects(bucket), [])
    def test_delete_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'defualt s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
        
        # Create S3 object (main user)
        s3conn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket,filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
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
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
        
        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
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
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
        
        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
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
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
        
        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
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
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
        
        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
class TestPrivateReadSwiftContainer:
    def setUp(self):
        # Create a Swift private read container
        self.bucket = create_swift_container_with_acl({'x-container-read':username})
    def test_default_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'defualt swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket, filename, text)
        # Read object using S3 (second user)
        s3user = get_s3user()
        eq(s3user.get_contents(bucket, filename), text)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.get_contents(bucket, filename), text)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        # SHOULD FAIL - use assert_raises
        eq(unauthuser.get_contents(bucket, filename), text)
    def test_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'defualt s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # Read object using S3 (second user)
        s3user = get_s3user()
        eq(s3user.get_contents(bucket, filename), text)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.get_contents(bucket, filename), text)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        # SHOULD FAIL - use assert_raises
        eq(unauthuser.get_contents(bucket, filename), text)
    def test_public_read_s3_object(self):
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
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)
    def test_private_read_s3_object(self):
        bucket = self.bucket
        filename = 'private-read-s3-object'
        text = 'private read s3 object'
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Read object using S3 (second user)
        s3user = get_s3user()
        eq(s3user.get_contents(bucket, filename), text)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.get_contents(bucket, filename), text)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        # SHOULD FAIL - use assert_raises
        eq(unauthuser.get_contents(bucket, filename), text)
    def test_public_full_control_s3_object(self):
        # Create public read S3 object (main user)
        bucket = self.bucket
        filename = 'public-full-control-s3-object'
        text = 'public full control s3 object'
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Read object using S3 (second user)
        s3user = get_s3user()
        eq(s3user.get_contents(bucket, filename), text)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.get_contents(bucket, filename), text)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), text)
    def test_private_full_control_s3_object(self):
        bucket = self.bucket
        filename = 'private-full-control-s3-object'
        text = 'private full control s3 object'
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket,filename,text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Read object using S3 (second user)
        s3user = get_s3user()
        eq(s3user.get_contents(bucket, filename), text)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.get_contents(bucket, filename), text)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        # SHOULD FAIL - use assert_raises
        eq(unauthuser.get_contents(bucket, filename), text)

# class TestPrivateWriteSwiftContainer:

class TestPublicReadS3Bucket(unittest.TestCase):
    def setUp():
        # Create an S3 public read bucket
        bucket = create_s3_container_with_acl()
    def test_default_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'defualt swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket, filename, text)
        # List objects using S3 (second user)
        s3user = get_s3user()
        eq(s3user.list_objects(bucket, filename), [filename])
        # List objects using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), [filename])
    def test_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'defualt s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # List objects using S3 (second user)
        s3user = get_s3user()
        eq(s3user.list_objects(bucket, filename), [filename])
        # List objects using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), [filename])
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
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), [filename])
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
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), [filename])
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
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), [filename])
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
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), [filename])

class TestPrivateReadS3Bucket(unittest.TestCase):
    def setUp():
        # Create an S3 private read bucket
        bucket = create_s3_container_with_acl()
    def test_default_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'defualt swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket, filename, text)
        # List objects using S3 (second user)
        s3user = get_s3user()
        eq(s3user.list_objects(bucket, filename), [filename])
        # List objects using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        # SHOULD FAIL - use assert_raises
        eq(unauthuser.get_contents(bucket, filename), [filename])
    def test_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'defualt s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # List objects using S3 (second user)
        s3user = get_s3user()
        eq(s3user.list_objects(bucket, filename), [filename])
        # List objects using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        # SHOULD FAIL - use assert_raises
        eq(unauthuser.get_contents(bucket, filename), [filename])
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
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), [filename])
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
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        # SHOULD FAIL - use assert_raises
        eq(unauthuser.get_contents(bucket, filename), [filename])
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
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), [filename])
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
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        # SHOULD FAIL - use assert_raises
        eq(unauthuser.get_contents(bucket, filename), [filename])


### EXTREMELY SIMILAR TO TEST PUBLIC WRITE SWIFT BUCKET
class TestPublicWriteS3Bucket(unittest.TestCase):
    def test_create_object():
        ### FIXME
        bucket = self.bucket
        filename = 'object'
        text = 'defualt object'
        # Create Swift object with second user
        swiftuser = get_swiftuser()
        swiftuser.put_object(bucket, filename, text)

        # Create S3 object with second user
        s3user = get_s3user()
        s3user.put_object(bucket, filename, text)
        # Create object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.put_object(bucket, filename, text)
        # Read with Swift
        # Read with S3
        # Delete with Swift
        # Delete with S3

    def test_create_default_s3_object(self):
        # Create S3 object with second user

    def test_delete_default_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'defualt swift object'
        # Create Swift object (main user)
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
        
        # Create Swift object (main user)
        swiftconn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket,filename)
        # Check that bucket is empty
        swiftconn.get_container(bucket)
        eq(swiftconn.list_objects(bucket), [])
    def test_delete_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'defualt s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
        
        # Create S3 object (main user)
        s3conn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket,filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
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
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
        
        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
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
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
        
        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
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
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
        
        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
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
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
        
        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])

class TestPrivateReadS3Bucket(unittest.TestCase):
    def setUp():
        # Create an S3 public read bucket
        bucket = create_s3_container_with_acl()
    def test_default_swift_object(self):
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'defualt swift object'
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket, filename, text)
        # List objects using S3 (second user)
        s3user = get_s3user()
        eq(s3user.list_objects(bucket, filename), [filename])
        # List objects using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), [filename])
    def test_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'defualt s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # List objects using S3 (second user)
        s3user = get_s3user()
        eq(s3user.list_objects(bucket, filename), [filename])
        # List objects using Swift (second user)
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), [filename])
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
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), [filename])
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
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), [filename])
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
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), [filename])
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
        swiftuser = get_s3user()
        eq(swiftuser.list_objects(bucket, filename), [filename])
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        eq(unauthuser.get_contents(bucket, filename), [filename])

class TestPrivateWriteS3Bucket(unittest.TestCase):
    def test_default_swift_object():
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket,filename,text)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        swiftconn.get_container(bucket)
        ### eq test

        # Create Swift object (main user)
        swiftconn.put_object(bucket,filename,text)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        swiftconn.get_container(bucket)
        ### eq test

        # Create Swift object (main user)
        swiftconn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket,filename)
        # Check that bucket is empty
        swiftconn.get_container(bucket)
        ### eq test

        # Create Swift object with second user
        swiftuser.put_object(bucket,filename,text)
        # Create S3 object with second user
        s3user.put_object(bucket,filename,text)
        # Create S3 object with unauthenticated user
        unauthuser.put_object(bucket,filename,text)

        ## MAIN USER
        # Read with Swift
        # Read with S3
        # Delete with Swift
        # Delete with S3

        ### Test again with unauthenticated user
    def test_delete_default_swift_object(self):
        # Create Swift object (main user)
        bucket = self.bucket
        filename = 'default-swift-object'
        text = 'defualt swift object'
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
        
        # Create Swift object (main user)
        swiftconn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket,filename)
        # Check that bucket is empty
        swiftconn.get_container(bucket)
        eq(swiftconn.list_objects(bucket), [])
    def test_delete_default_s3_object(self):
        bucket = self.bucket
        filename = 'default-s3-object'
        text = 'defualt s3 object'
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
        
        # Create S3 object (main user)
        s3conn.put_object(bucket,filename,text)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket,filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
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
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
        
        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('READ', bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
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
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
        
        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', username, bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
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
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
        
        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
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
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])
        
        # Create S3 object (main user)
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('FULL_CONTROL', username, bucket, filename)
        # Delete object with unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.delete_object(bucket, filename)
        # Check that bucket is empty
        eq(swiftconn.list_objects(bucket), [])

class TestPublicFullControlS3Bucket:
class TestPrivateFullControlS3Bucket: