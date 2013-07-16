import boto
import boto.s3.connection
import boto.s3.acl
import boto.s3
from boto.s3.key import Key

import swiftclient

import os.path
import sys
import yaml

## FROM https://github.com/ceph/swift/blob/master/test/__init__.py
def get_config():
    """
    Attempt to get a functional config dictionary.
    """
    # config_file = os.environ.get('CROSS_PROTOCOL_TEST_CONFIG_FILE')
    try:
        # Get config.yaml in the same directory
        __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(sys.argv[0])))
        f = open(os.path.join(__location__, 'config.yaml'))
        # use safe_load instead load
        conf = yaml.safe_load(f)
        f.close()
    except IOError:
        print >>sys.stderr, 'UNABLE TO READ FUNCTIONAL TESTS CONFIG FILE'
    return conf

conf = get_config()
s3keys = conf['s3']
swiftkeys = conf['swift']
s3userkeys = conf['s3user']
swiftuserkeys = conf['swiftuser']

## HELPER FUNCTIONS

# For more parameters:
# https://github.com/boto/boto/blob/develop/boto/s3/connection.py
def get_s3conn():
    return boto.connect_s3(
        aws_access_key_id = s3keys['aws_access_key_id'],
        aws_secret_access_key = s3keys['aws_secret_access_key'],
        host = s3keys['host'],
        is_secure = True,
        port = None,
        proxy = None,
        proxy_port = None,
        https_connection_factory = None,
        calling_format = boto.s3.connection.OrdinaryCallingFormat()
    )

# For more parameters:
# https://github.com/openstack/python-swiftclient/blob/master/swiftclient/client.py
def get_swiftconn():
    return swiftclient.Connection(
        authurl = swiftkeys['authurl'],
        user = swiftkeys['user'],
        key = swiftkeys['key'],
        preauthurl = None
        # NOTE TO SELF: Port, HTTPS/HTTP, etc. all contained in authurl/preauthurl
    )

def get_s3user():
    return boto.connect_s3(
        aws_access_key_id = s3userkeys['aws_access_key_id'],
        aws_secret_access_key = s3userkeys['aws_secret_access_key'],
        host = s3userkeys['host'],
        is_secure = True,
        port = None,
        proxy = None,
        proxy_port = None,
        https_connection_factory = None,
        calling_format = boto.s3.connection.OrdinaryCallingFormat()
    )

def get_swiftuser():
    return swiftclient.Connection(
        authurl = swiftuserkeys['authurl'],
        user = swiftuserkeys['user'],
        key = swiftuserkeys['key'],
        preauthurl = None
        # NOTE TO SELF: Port, HTTPS/HTTP, etc. all contained in authurl/preauthurl
    )

def get_s3_noauth():
    return boto.connect_s3(
        aws_access_key_id = None,
        aws_secret_access_key = None,
        host = s3userkeys['host'],
        anon=True,
        calling_format = boto.s3.connection.OrdinaryCallingFormat()
    )

def get_swift_noauth():
    return swiftclient.Connection(
        authurl = swiftuserkeys['authurl'],
        user = None,
        key = None,
        preauthurl = objects.dreamhost.com
        # NOTE TO SELF: Port, HTTPS/HTTP, etc. all contained in authurl/preauthurl
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

def create_swift_container_with_acl(acl_headers):
    try:
        # Create Swift container
        swiftconn = get_swiftconn()
        container = SWIFTCONTAINERNAME
        ### Note: Bobtail does not support updating container ACLs using PUT
        swiftconn.put_container(container)
        swiftconn.post_container(container, {'x-container-read':'.r:*'}) ### Replace with acl_headers
        return container
    except:

## START SWIFT CONTAINER TESTS

def test_public_read_swift_container_default_swift_object():
    # Create public read Swift container
    swiftconn = get_swiftconn()
    container = 'swift-public-read'
    swiftconn.put_container(container)
    swiftconn.post_container(container, {'x-container-read':'.r:*'})
    # Write object using Swift main user
    swiftconn.put_object(container, 'test', 'this is a test file')

    # Read object using S3 second user
    s3user
    eq(s3_get_object(swiftconn, container, 'test'), True)
    # Read object using Swift second user
    eq(swift_get_object(swiftconn, container, 'test'), True)
    # Read object using unauthenticated user


def test_public_read_swift_container_default_s3_object():
    ## Create public read Swift container
    swiftconn = get_swiftconn()
    container = 'swift-public-read'
    swiftconn.put_container(container)
    swiftconn.post_container(container, {'x-container-read':'.r:*'})
    # Write object (default perms) using S3 main user
    s3conn = get_s3conn()
    s3conn.make_request('PUT', container, 'test', data='this is a test file')
    # Read object using S3 second user (?)
    eq(s3_get_object(swiftconn, container, 'test'), True)
    # Read object using Swift second user (?)
    eq(swift_get_object(swiftconn, container, 'test'), True)
    # Read object using unauthenticated user

def test_public_read_swift_container_private_s3_object():
    # Create public read Swift container
    swiftconn = get_swiftconn()
    container = 'swift-public-read'
    swiftconn.put_container(container)
    swiftconn.post_container(container, {'x-container-read':'.r:*'})
    # Write object (private full_control) using S3 main user
    s3conn = get_s3conn()
    s3conn.make_request('PUT', container, 'test', data='this is a test file')
    ## Change ACLs
    # Read object using S3 second user (?)
    eq(s3_get_object(swiftconn, container, 'test'), True)
    # Read object using Swift second user (?)
    eq(swift_get_object(swiftconn, container, 'test'), True)
    # Read object using unauthenticated user

    # Read object using S3 unauthenticated user
    # Do the same with Swift
    # Read/write ACLs

def test_public_read_swift_container_public_s3_object():
    # Create public read Swift container
    swiftconn = get_swiftconn()
    container = 'swift-public-read'
    swiftconn.put_container(container)
    swiftconn.post_container(container, {'x-container-read':'.r:*'})
    # Write object (private full_control) using S3 main user
    s3conn = get_s3conn()
    s3conn.make_request('PUT', container, 'test', data='this is a test file')
    ## Change ACLs
    # Read object using S3 second user (?)
    eq(s3_get_object(swiftconn, container, 'test'), True)
    # Read object using Swift second user (?)
    eq(swift_get_object(swiftconn, container, 'test'), True)
    # Read object using unauthenticated user


    # Read object using S3 unauthenticated user
    # Do the same with Swift
    # Read/write ACLs

def test_public_write_swift_container_default_s3_object():
    ## Create public write Swift container
    swiftconn = get_swiftconn()
    container = 'swift-public-read'
    swiftconn.put_container(container)
    swiftconn.post_container(container, {'x-container-write':'.r:*'})

    # Create object with S3 second user
    # Do the same with Swift
    s3user = get_s3user()
    s3user.make_request('PUT', container, 'test', data='this is a test file')
    swiftuser = get_swiftuser()
    swiftuser.put_object(containers[i],'test.txt','test text here')

    # Create DEFAULT object with S3 main user
    # Delete object with S3 second user
    # Do the same with Swift

    # Create object with S3 unauthenticated user
    # Do the same with Swift

    # Create DEFAULT object with S3 main user
    # Delete object with S3 unauthenticated user
    # Do the same with Swift
    pass
 
def test_public_write_swift_container_default_swift_object():
    ## Create public write Swift container

    # Create object with S3 second user
    # Do the same with Swift

    # Create DEFAULT object with Swift main user
    # Delete object with S3 second user
    # Do the same with Swift

    # Create object with S3 unauthenticated user
    # Do the same with Swift

    # Create DEFAULT object with Swift main user
    # Delete object with S3 unauthenticated user
    # Do the same with Swift

def test_public_write_swift_container_private_s3_object():
    ## Create public write Swift container

    ## Write object (private full_control) using S3 second user
    ## Read object using S3 second user (should work)
    ## Read object using Swift second user (?)

    # Read object using S3 main user (Depends on who gets private access, main or third)
    # Read object using Swift main user

def test_public_write_swift_container_public_s3_object():
    ## Create public write Swift container

    ## Write object (public full_control) using S3 second user
    ## Read object using S3 second user (should work)
    ## Read object using Swift second user (?)

    # Read object using S3 main user
    # Read object using Swift main user

    # (If necessary:) Read object using S3 third user... etc

def test_private_read_swift_container_default_swift_object():
    ## Create private read Swift container

    ## Write object using Swift main user

    # Read object using S3 second user (?)
    # Read object using Swift second user (?)

    # Read object using S3 third user (not allowed)
    # Read object using Swift third user (not allowed)

def test_private_read_swift_container_default_s3_object():
    ## Create private read Swift container

    ## Write object (default perms) using S3 main user

    # Read object using S3 second user (?)
    # Read object using Swift second user (?)

    # Read object using S3 third user (not allowed)
    # Read object using Swift third user (not allowed)

def test_private_read_swift_container_private_s3_object():
    ## Create private read Swift container

    ## Write object (private full_control) using S3 main user

    # Read object using S3 second user (?)
    # Read object using Swift second user (?)

    # Read object using S3 third user (?)
    # Read object using Swift third user (?)

def test_private_read_swift_container_public_s3_object():
    ## Create private read Swift container

    ## Write object (public full_control) using S3 main user

    # Read object using S3 second user (?)
    # Read object using Swift second user (?)

    # Read object using S3 third user (?)
    # Read object using Swift third user (?)

def test_private_write_swift_container_default_s3_object():
    ## Create private write Swift container

    # Create object with S3 second user
    # Do the same with Swift
    # Delete object with S3 second user
    # Do the same with Swift

    # Create object with S3 unauthenticated user
    # Do the same with Swift
    # Delete object with S3 unauthenticated user
    # Do the same with Swift
    pass
 
def test_private_write_swift_container_default_swift_object():
    ## Create private write Swift container

    ## Write object (default perms) using Swift second user
    ## Read object using S3 second user (should work)
    ## Read object using Swift second user (?)

    # Write object using S3 third user (not allowed)
    # Write object using Swift third user (not allowed)

def test_private_write_swift_container_private_s3_object():
    ## Create private write Swift container

    ## Write object (private full_control) using S3 second user
    ## Read object using S3 second user (should work)
    ## Read object using Swift second user (?)

    # Write object using S3 third user (not allowed)
    # Write object using Swift third user (not allowed)

def test_private_write_swift_container_public_s3_object():
    ## Create private write Swift container

    ## Write object (public full_control) using S3 second user
    ## Read object using S3 second user (should work)
    ## Read object using Swift second user (?)

    # Write object using S3 third user (not allowed)
    # Write object using Swift third user (not allowed)

    # (If necessary:) Read object using S3 third user... etc

# Unnecessary?
def test_default_s3_bucket():
    # Create default S3 bucket
    pass

def test_private_write_s3_bucket_create_object():
    # Create private write S3 bucket
    s3conn = get_s3conn()
    name = 'bucketname'
    bucket = s3conn.create_bucket(name)
    bucket.add_user_grant('READ',s3user)

    # Create object with S3/Swift second user
    s3user = get_s3user()
    # Delete object with S3/Swift main user

    # Create object with Swift/S3 unauthenticated user (fail)
    # Delete object with Swift/S3 unauthenticated user (fail)
    pass
def test_private_write_s3_bucket_delete_object():
    # Create private write S3 bucket
    s3conn = get_s3conn()
    name = 'bucketname'
    bucket = s3conn.create_bucket(name)
    bucket.add_user_grant('READ',s3user)

    # Create object with S3/Swift second user
    s3user = get_s3user()
    bucket = s3user.get_bucket(name)
    # Delete object with S3/Swift main user

    # Create object with Swift/S3 unauthenticated user (fail)
    # Delete object with Swift/S3 unauthenticated user (fail)
    pass

def test_public_write_s3_bucket():
    # Create default S3 bucket

    # Create object with S3

    # Create object with Swift second user
    # Do the same with S3
    # Delete object with Swift second user
    # Do the same with S3

    # Create object with Swift unauthenticated user
    # Do the same with S3
    # Delete object with Swift unauthenticated user
    # Do the same with S3
    pass

def test_private_read_s3_bucket():
    # Create default S3 bucket

    # Create object with S3

    # List objects with Swift second user
    # Do the same with S3
    # List objects with Swift unauthenticated user (fail)
    # Do the same with S3
    pass

def test_public_read_s3_bucket():
    # Create default S3 bucket

    # Create object with S3

    # List objects with Swift second user
    # Do the same with S3
    # List objects with Swift unauthenticated user
    # Do the same with S3
    pass

def test_private_full_control_s3_bucket():
    # Create default S3 bucket

    # Create object with Swift second user

    # List objects with Swift second user
    # Do the same with S3
    # Delete object with Swift second user
    # Do the same with S3
    # List objects with Swift unauthenticated user (fail)

    # Create object with Swift unauthenticated user (fail)
    # Do the same with S3
    # Delete object with Swift unauthenticated user (fail)
    # Do the same with S3
    pass

def test_public_full_control_s3_bucket():
    # Create default S3 bucket

    # Create object with Swift second user

    # List objects with Swift second user
    # Do the same with S3
    # Delete object with Swift second user
    # Do the same with S3

    # List objects with Swift unauthenticated user (fail)
    # Do the same with S3
    # Create object with Swift unauthenticated user (fail)
    # Do the same with S3
    # Delete object with Swift unauthenticated user (fail)
    # Do the same with S3
    pass
