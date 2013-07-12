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



def swift_list_container_objects(swiftconn, container):
    try:
        swiftconn.get_container(container)
        return True
    except swiftclient.ClientException:
        return False
    #eq(swift_err.http_reason, 'Authorization Required')
    #eq(swift_err.http_response_content, 'NoSuchBucket')
    #eq(swift_err.status, 401)

def s3_list_container_objects(s3conn, bucket):
    try:
        b = s3conn.get_bucket(bucket)
        return True
    except boto.exception.S3ResponseError:
        return False
    #eq(s3_err.status, 403)
    #eq(s3_err.reason, 'Forbidden')
    #eq(s3_err.error_code, 'AccessDenied')

def swift_get_object(swiftconn, container, filename):
    try:
        swiftconn.get_object(container, filename)
        return True
    except swiftclient.ClientException:
        return False

def s3_get_object(s3conn, bucket, filename):
    try:
        b = s3conn.get_bucket(bucket)
        k = Key(b)
        k.key = filename
        k.get_contents_to_filename(filename)
        return True
    except boto.exception.S3ResponseErorr:
        return False

def swift_delete_container(swiftconn, container):
    try:
        swiftconn.delete_container(container)
        return True
    except swiftclient.ClientException:
        return False

def s3_delete_container(s3conn, bucket):
    try:
        s3conn.delete_bucket(bucket)
        return True
    except boto.exception.S3ResponseErorr:
        return False

def swift_create_object(swiftconn, container, filename):
    try:
        swiftconn.put_object(container, filename)
        return True
    except swiftclient.ClientException:
        return False

def s3_create_object(s3conn, bucket, filename):
    try:
        b = s3conn.get_bucket(bucket)
        # FIXME
        return True
    except boto.exception.S3ResponseErorr:
        return False

def swift_delete_object(swiftconn):
    pass

def s3_delete_object(s3conn):
    pass

def s3_read_bucket_acl(s3conn):
    pass

def s3_write_bucket_acl(s3conn):
    pass

def s3_read_object_acl(s3conn):
    pass

def s3_write_object_acl(s3conn):
    pass

def test_default_s3():
    pass


## START SWIFT CONTAINER TESTS
def test_default_swift():
    # Create default Swift container
    swiftconn = get_swiftconn()
    container = 'swift-default'
    header = {}
    swiftconn.put_container(container)
    swiftconn.post_container(container, header)

    # Create Swift object (main user)
    swiftconn.put_object(containers[i],'test.txt','test text here')

    # Access object (other user)
    swiftuser = get_swiftuser()
    s3user = get_swiftuser()
    # Try to list the contents
    s3_err = assert_raises(boto.exception.S3ResponseError, s3user.get_bucket, name)
    swift_err = assert_raises(swiftclient.ClientException, swiftuser.get_container, name)

 
def test_public_read_swift_container_default_swift_object():
    ## Create public read Swift container

    ## Write object using Swift main user

    # Read object using S3 second user (?)
    # Read object using Swift second user (?)

    # Read object using S3 unauthenticated user
    # Do the same with Swift

    # Should I test write object / list objects here?

def test_public_read_swift_container_default_s3_object():
    ## Create public read Swift container

    ## Write object (default perms) using S3 main user

    # Read object using S3 second user (?)
    # Read object using Swift second user (?)

    # Read object using S3 unauthenticated user
    # Do the same with Swift

def test_public_read_swift_container_private_s3_object():
    ## Create public read Swift container

    ## Write object (private full_control) using S3 main user

    # Read object using S3 second user (?)
    # Read object using Swift second user (?)
    # Read/write ACLs

    # Read object using S3 unauthenticated user
    # Do the same with Swift
    # Read/write ACLs

def test_public_read_swift_container_public_s3_object():
    ## Create public read Swift container

    ## Write object (public full_control) using S3 main user

    # Read object using S3 second user (?)
    # Read object using Swift second user (?)
    # Read/write ACLs

    # Read object using S3 unauthenticated user
    # Do the same with Swift
    # Read/write ACLs

def test_public_write_swift_container_default_s3_object():
    ## Create public write Swift container

    # Create object with S3 second user
    # Do the same with Swift

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

def test_private_write_s3_bucket():
    # Create default S3 bucket

    # Create object with S3

    # Create object with Swift second user
    # Do the same with S3
    # Delete object with Swift second user
    # Do the same with S3

    # Create object with Swift unauthenticated user (fail)
    # Do the same with S3
    # Delete object with Swift unauthenticated user (fail)
    # Do the same with S3
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

# listing objects should fail