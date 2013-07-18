
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

### NOTE: Should test the main user's ability to delete/read objects that weren't created by it
class TestPublicReadSwiftContainer:
    def setUp():
        # Create a Swift public read container
        self.bucket = create_swift_container_with_acl({'x-container-read':'.r:*'})
    def tearDown():
        delete_all()
    def test_default_swift_object():
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket, filename, text)
        # Read object using S3 (second user)
        s3user = get_s3user()
        s3user.get_object(bucket)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        swiftuser.get_object(bucket)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.get_contents(bucket)
    def test_default_s3_object():
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        # Read object using S3 (second user)
        s3user = get_s3user()
        s3user.get_object(bucket)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        swiftuser.get_object(bucket)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.get_contents(bucket)
    def test_public_read_s3_object():
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('WRITE', bucket, filename)
        # Read object using S3 (second user)
        s3user = get_s3user()
        s3user.get_object(bucket)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        swiftuser.get_object(bucket)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.get_contents(bucket)
    def test_private_read_s3_object():
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_private_acl('READ', USERNAME, bucket, filename)
        # Read object using S3 (second user)
        s3user = get_s3user()
        s3user.get_object(bucket)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        swiftuser.get_object(bucket)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.get_contents(bucket)
    def test_public_full_control_s3_object():
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket, filename, text)
        s3conn.add_public_acl('FULL_CONTROL', bucket, filename)
        # Read object using S3 (second user)
        s3user = get_s3user()
        s3user.get_object(bucket, filename)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        swiftuser.get_object(bucket)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.get_contents(bucket)
    def test_private_full_control_s3_object():
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket,filename,text)
        s3conn.add_private_acl('FULL_CONTROL',USERNAME,bucket,filename)
        # Read object using S3 (second user)
        s3user = get_s3user()
        s3user.get_object(bucket)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        swiftuser.get_object(bucket)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.get_contents(bucket)
    ### TODO: TEST ACLs
class TestPublicWriteSwiftContainer:
    def setUp():
        # Create a Swift public write container
        bucket = create_swift_container_with_acl({'x-container-write':'.r:*'})
    def tearDown():
        delete_all()
    def test_default_swift_object():
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(bucket,filename,text)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(swiftconn.list_objects(bucket), [])

        # Create Swift object (main user)
        swiftconn.put_object(bucket,filename,text)
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

        # Create Swift object with second user
        swiftuser.put_object(bucket,filename,text)
        # Create S3 object with second user
        s3user.put_object(bucket,filename,text)
        # Create S3 object with unauthenticated user
        unauthuser.put_object(bucket,filename,text)
    def test_default_s3_object():
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(bucket,filename,text)
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(bucket, filename)
        # Check that container is empty
        eq(swiftconn.list_objects(bucket), [])

        # Create S3 object (main user)
        s3conn.put_object(bucket,filename,text)
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
        swiftconn.get_container(bucket)
        eq(swiftconn.list_objects(bucket), [])

        # Create Swift object with second user
        swiftuser.put_object(bucket,filename,text)
        # Create S3 object with second user
        s3user.put_object(bucket,filename,text)
        # Create S3 object with unauthenticated user
        unauthuser.put_object(bucket,filename,text)
class TestPrivateReadSwiftContainer:
    def setUp():
        ### Create a Swift public read container...
        bucket = create_swift_container_with_acl({'x-container-read':'SECOND USER, MAIN ACCOUNT'})
    def tearDown():
        ### Delete container and objects...
class TestPrivateWriteSwiftContainer:
    def setUp():
        ### Create a Swift public read container...
        bucket = create_swift_container_with_acl({'x-container-write':'SECOND USER, MAIN ACCOUNT'})
    def tearDown():
        ### Delete container and objects...


class TestPublicWriteS3Bucket:
    def setUp():
        # Create an S3 public write bucket
        bucket = create_s3_container_with_acl()
    def tearDown():
        ### Delete container and objects...

    ### SET S3 PERMISSIONS? DO LATER
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
class TestPublicReadS3Bucket:

class TestPrivateWriteS3Bucket:
    def setUp():
        bucket=create_s3_container_with_acl({'ACL HERE'})
    def tearDown():
        ### Delete bucket and objects
    ### Similar to private write swift
class TestPrivateReadS3Bucket:
    # Just try to list the buckets

class TestPublicFullControlS3Bucket:
    # Same as public write control...

class TestPrivateFullControlS3Bucket: