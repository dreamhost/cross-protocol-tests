import boto
import boto.s3.connection
import boto.s3
from boto.s3.key import Key

import swiftclient

import random
from nose.tools import eq_ as eq

# url
url = "objects.dreamhost.com"

# S3 keys
access_key = 'fs46-5HPyNCbQ_HIYUwO'
secret_key = 'pz71sbAR4OK_OffU4BfZIpfFq5P0OFm68RgDRVg0'

# Swift keys
username = 'kevinchoi:kevchoi'
api_key = 'YGzYAnKmAeG6I9XatSVkGf0lAeUHH09NZiGMM30u'
swifturl = 'http://objects.dreamhost.com/auth/v1.0'

s3conn = boto.connect_s3(
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    host = url,
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )

swiftconn = swiftclient.Connection(
    authurl = swifturl,
    user = username,
    key = api_key
    )

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

class SwiftUtils():
    # Add Swift utilities
    @classmethod
    def list_containers(self, conn):
        buckets = conn.get_account()[1]
        list_of_buckets = [bucket_dictionary[u'name'] \
        for bucket_dictionary in buckets]
        return list_of_buckets
    @classmethod
    def swift_delete_all_containers(self, conn):
        for container in conn.get_account()[1]:
            name = container.get(u'name')
            objects = conn.get_container(name)[1]
            if objects:
                for fileInfo in objects:
                    fileName = fileInfo.get(u'name')
                    conn.delete_object(name, fileName)
                conn.delete_container(name)
            if not objects:
                conn.delete_container(name)
    @classmethod
    def md5(self, conn, bucket, object_name):
        return conn.head_object(bucket, object_name)['etag']
    @classmethod
    def size(self, conn, bucket, object_name):
        return int(conn.head_object(bucket, object_name)['content-length'])


    # Not Needed...
    @classmethod
    def get_swift_auth(url):
        parsed, conn = http_connection(url)
        method = 'GET'
        conn.request(method, parsed.path, '',
                     {'X-Auth-User': username, 'X-Auth-Key': api_key})
        resp = conn.getresponse()
        body = resp.read()
        http_log((url, method,), {}, resp, body)
        url = resp.getheader('x-storage-url')

        return resp.getheader('x-storage-token', resp.getheader('x-auth-token'))


class S3Utils():
    # Add S3 utilities
    @classmethod
    def list_containers(self, conn):
        buckets = conn.get_all_buckets()
        list_of_buckets = [bucket.name for bucket in buckets]
        return list_of_buckets
    @classmethod
    def md5(self, conn, bucket, object_name):
        resp = conn.make_request('HEAD', bucket, object_name)
        for x in resp.getheaders():
            if x[0]=='etag':
                return x[1]
class Utils:
    # 3-63 chars, must start/end with lowercase letter or number
    # can contain lowercase letters, numbers, and dashes (no periods)
    # cannot start/end with dash or period
    # no consecutive period/dashes
    @classmethod
    def create_valid_name(self):
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


## SET UP TESTS AS CLASSES AND USE INSTANCE VARIABLES (ie. NAME)

def test_list_bucket():
    # Create bucket using S3
    name = Utils.create_valid_name()
    bucket = s3conn.create_bucket(name)
    # Check if it's there
    s3conn.get_bucket(bucket.name)
    swiftconn.get_container(name)
    # List buckets
    name = name.encode('utf-8')
    assert(name in SwiftUtils.list_containers(swiftconn))
    assert(name in S3Utils.list_containers(s3conn))
    # FIXME: remove bucket or teardown

def test_list_container():
    # Create bucket using Swift
    name = Utils.create_valid_name()
    swiftconn.put_container(name)
    # Check if it's there
    swiftconn.get_container(name)
    s3conn.get_bucket(name)
    # List buckets
    assert(name in SwiftUtils.list_containers(swiftconn))
    assert(name in S3Utils.list_containers(s3conn))
    # FIXME: remove bucket or teardown

def test_delete_empty_bucket():
    # Create bucket using S3
    name = Utils.create_valid_name()
    bucket = s3conn.create_bucket(name)
    # Delete using Swift
    swiftconn.delete_container(name)
    # List buckets
    eq([], SwiftUtils.list_containers(swiftconn))
    eq([], S3Utils.list_containers(s3conn))
    # Check if it's there

    assert_raises(boto.exception.S3ResponseError, s3conn.delete_bucket, name)
    assert_raises(swiftclient.ClientException, swiftconn.delete_container, name)

def test_delete_non_empty_bucket():
    pass

def test_delete_container():
    # Create bucket using Swift
    name = Utils.create_valid_name()
    swiftconn.put_container(name)
    # Delete using S3
    s3conn.delete_bucket(name)
    # List buckets
    eq([], SwiftUtils.list_containers(swiftconn))
    eq([], S3Utils.list_containers(s3conn))
    # Check if it's there

    assert_raises(boto.exception.S3ResponseError, s3conn.delete_bucket, name)
    assert_raises(swiftclient.ClientException, swiftconn.delete_container, name)

def test_delete_non_empty_container():
    pass

def test_create_existing_bucket():
    # Create container using Swift
    name = Utils.create_valid_name()
    swiftconn.put_container(name)
    # FIXME: add assertRaise - try to create same bucket using S3


def test_create_existing_container():
    # Create bucket using S3
    name = Utils.create_valid_name()
    bucket = s3conn.create_bucket(name)
    # FIXME: add assertRaise - try to create same container using Swift


def test_create_object_in_bucket():
    # Create container using Swift
    name = Utils.create_valid_name()
    swiftconn.put_container(name)
    # Create object using S3
    bucket = s3conn.get_bucket(name)
    k = Key(bucket)
    k.key = 'foobar'
    k.set_contents_from_string('Create object using S3')
    # FIXME (check bucket)

def test_create_object_in_container():
    # Create bucket using S3
    name = Utils.create_valid_name()
    bucket = s3conn.create_bucket(name)
    # Create object using Swift
    swiftconn.put_object(name, 'foobar', 'Create object using Swift')
    # FIXME (check bucket)

def test_size_object_in_container():
    # Create container using Swift
    name = Utils.create_valid_name()
    swiftconn.put_container(name)
    # Create object using S3
    bucket = s3conn.get_bucket(name)
    k = Key(bucket)
    k.key = 'foobar'
    f = open("testfile.txt", "wb+")
    f.write("0" * 500)
    f.seek(0)
    k.set_contents_from_file(f)
    # Checksum
    eq(S3Utils.md5(s3conn, name, 'foobar'), SwiftUtils.md5(swiftconn, name, 'foobar'))
    # Size
    eq(k.size, SwiftUtils.size(swiftconn, name, 'foobar'))

def test_size_object_in_bucket():
    # Create bucket using S3
    name = Utils.create_valid_name()
    bucket = s3conn.create_bucket(name)
    # Create object using Swift
    f = open("testfile.txt", "wb+")
    f.write("0" * 500)
    f.seek(0)
    swiftconn.put_object(name, 'foobar', f)
    bucket = s3conn.get_bucket(name)
    k = Key(bucket, 'foobar')
    # Checksum
    eq(S3Utils.md5(s3conn, name, 'foobar'), SwiftUtils.md5(swiftconn, name, 'foobar'))
    # Size
    eq(k.size, SwiftUtils.size(swiftconn, name, 'foobar'))

def test_checksum_object_in_container():
    # Create container using Swift
    name = Utils.create_valid_name()
    swiftconn.put_container(name)
    # Create object using S3
    bucket = s3conn.get_bucket(name)
    k = Key(bucket)
    k.key = 'foobar'
    f = open("testfile.txt", "wb+")
    f.write("0" * 500)
    f.seek(0)
    k.set_contents_from_file(f)
    # Checksum
    eq(S3Utils.md5(s3conn, name, 'foobar'), SwiftUtils.md5(swiftconn, name, 'foobar'))
    # Size
    eq(k.size, SwiftUtils.size(swiftconn, name, 'foobar'))

def test_checksum_object_in_bucket():
    # Create bucket using S3
    name = Utils.create_valid_name()
    bucket = s3conn.create_bucket(name)
    # Create object using Swift
    f = open("testfile.txt", "wb+")
    f.write("0" * 500)
    f.seek(0)
    swiftconn.put_object(name, 'foobar', f)
    bucket = s3conn.get_bucket(name)
    k = Key(bucket, 'foobar')
    # Checksum
    eq(S3Utils.md5(s3conn, name, 'foobar'), SwiftUtils.md5(swiftconn, name, 'foobar'))
    # Size
    eq(k.size, SwiftUtils.size(swiftconn, name, 'foobar'))

def list_permisions_s3():
    pass
def list_permisions_swift():
    pass
