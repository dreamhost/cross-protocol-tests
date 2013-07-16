
# listing objects should fail

def create_swift_container_with_acl(acl_headers):
    # Create Swift container
    swiftconn = get_swiftconn()
    container = create_valid_name()
    ### Note: Bobtail does not support updating container ACLs using PUT
    swiftconn.put_container(container)
    swiftconn.post_container(container, acl_headers)
    return container

class TestPublicReadSwiftContainer:
    def setUp():
        ### Create a swift public read container...
        self.bucket = create_swift_container_with_acl({'x-container-read':'.r:*'})
    def tearDown():
        ### Delete container and objects...

    def test_default_swift_object():
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(self.bucket,'test','test object')
        # Read object using S3 (second user)
        s3user = get_s3user()
        s3user.get_object(self.bucket)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        swiftuser.get_object(self.bucket)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.get_object(self.bucket)
    def test_default_s3_object():
        # Create S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(self.bucket,'test','test object')
        # Read object using S3 (second user)
        s3user = get_s3user()
        s3user.get_object(self.bucket)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        swiftuser.get_object(self.bucket)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.get_object(self.bucket)

    def test_public_read_s3_object():
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(self.bucket,'test.txt','test text here')
        ###s3conn.set_object_acl('ACL HERE')
        # Read object using S3 (second user)
        s3user = get_s3user()
        s3user.get_object(self.bucket)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        swiftuser.get_object(self.bucket)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.get_object(self.bucket)
    def test_private_read_s3_object():
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(self.bucket,'test.txt','test text here')
        ###s3conn.set_object_acl('ACL HERE')
        # Read object using S3 (second user)
        s3user = get_s3user()
        s3user.get_object(self.bucket)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        swiftuser.get_object(self.bucket)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.get_object(self.bucket)
    def test_public_full_control_s3_object():
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(self.bucket,'test.txt','test text here')
        ###s3conn.set_object_acl('ACL HERE')
        # Read object using S3 (second user)
        s3user = get_s3user()
        s3user.get_object(self.bucket)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        swiftuser.get_object(self.bucket)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.get_object(self.bucket)
    def test_private_full_control_s3_object():
        # Create public read S3 object (main user)
        s3conn = get_s3conn()
        s3conn.put_object(self.bucket,'test.txt','test text here')
        ###s3conn.set_object_acl('ACL HERE')
        # Read object using S3 (second user)
        s3user = get_s3user()
        s3user.get_object(self.bucket)
        # Read object using Swift (second user)
        swiftuser = get_s3user()
        swiftuser.get_object(self.bucket)
        # Read object using unauthenticated user
        unauthuser = get_unauthuser()
        unauthuser.get_object(self.bucket)
    ### TEST ACLs??

class TestPublicWriteSwiftContainer:
    def setUp():
        ### Create a swift public read container...
        self.bucket = create_swift_container_with_acl({'x-container-write':'.r:*'})
    def tearDown():
        ### Delete container and objects...

    def test_default_swift_object():
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(self.bucket,'test','test object')
        # Delete object with Swift second user
        swiftuser = get_swiftuser()
        swiftuser.delete_object(self.bucket, 'test')
        # Check that container is empty
        swiftconn.get_container(self.bucket)
        ### eq test

        # Create Swift object (main user)
        swiftconn.put_object(self.bucket,'test','test object')
        # Delete object with S3 second user
        s3user = get_s3user()
        s3user.delete_object(self.bucket, 'test')
        # Check that bucket is empty
        swiftconn.get_container(self.bucket)
        ### eq test

        # Create Swift object with second user
        swiftuser.put_object(self.bucket,'test','test object')
        # Create S3 object with second user
        s3user.put_object(self.bucket,'test','test object')
        
        ### Test again with unauthenticated user
    def test_default_s3_object():
        # Create Swift object (main user)
        swiftconn = get_swiftconn()
        swiftconn.put_object(self.bucket,'test','test object')
        # Delete object with Swift second user
        # Check that container is empty

        # Create Swift object (main user)
        # Delete object with S3 second user
        # Check that bucket is empty

        # Create Swift object with second user
        # Create S3 object with second user

        ### Test again with unauthenticated user