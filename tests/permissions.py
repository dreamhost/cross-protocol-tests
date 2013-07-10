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

def test_default_swift():
    # Create default Swift container
    swiftconn = get_swiftconn()
    container = 'swift-default'
    header = {}
    swiftconn.put_container(container)
    swiftconn.post_container(container, header)

    # Creat Swift object (main user)
    swiftconn.put_object(containers[i],'test.txt','test text here')

    # Access object (other user)
    swiftuser = get_swiftuser()
    s3user = get_swiftuser()
    # Try to list the contents
    s3_err = assert_raises(boto.exception.S3ResponseError, s3user.get_bucket, name)
    swift_err = assert_raises(swiftclient.ClientException, swiftuser.get_container, name)


# USE A TRY CATCH HERE?

def swift_list_container_objects(swiftconn, container):
    swift_err = assert_raises(swiftclient.ClientException, swiftconn.get_container, container)
    if swift_err.http_status >= 400 or swift_err.http_status <= 500:
        return True
    else:
        return False
    #eq(swift_err.http_reason, 'Authorization Required')
    #eq(swift_err.http_response_content, 'NoSuchBucket')
    #eq(swift_err.status, 401)

def s3_list_container_objects(s3conn, bucket):
    s3_err = assert_raises(boto.exception.S3ResponseError, s3conn.get_bucket, bucket)
    if s3_err.http_status >= 400 or swift_err.http_status <= 500:
        return True
    else:
        return False
    #eq(s3_err.status, 403)
    #eq(s3_err.reason, 'Forbidden')
    #eq(s3_err.error_code, 'AccessDenied')

def swift_get_object(swiftconn, container, filename):
    swift_err = assert_raises(swiftclient.ClientException, swiftconn.get_object, container, filename)
    pass

def s3_get_object(s3conn, bucket, filename):
    s3_err = assert_raises(boto.exception.S3ResponseError, s3user.make_request, 'GET', bucket, filename)
    pass

def swift_delete_container(swiftconn):
    pass

def s3_delete_container(s3conn):
    pass

def swift_create_object(swiftconn):
    pass

def s3_create_object(s3conn):
    pass

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