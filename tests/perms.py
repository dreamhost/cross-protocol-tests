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
    
s3conn = get_s3conn()
swiftconn = get_swiftconn()
s3user = get_s3user()
swiftuser = get_swiftuser()

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

def create_swift_containers(swiftconn):
    containers=[
    'swift-default',
    'swift-public-read',
    'swift-public-write',
    'swift-public-read-write',
    'swift-private-read',
    'swift-private-write',
    'swift-private-read-write',
    'swift-public-read-private-write',
    'swift-private-read-public-write'
    ]
    headers=[
    {},
    {'x-container-read':'.r:*'},
    {'x-container-write':'.r:*'},
    {'x-container-read':'.r:*', 'x-container-write':'.r:*'},
    {'x-container-read':'otheruser'},
    {'x-container-write':'otheruser'},
    {'x-container-read':'otheruser', 'x-container-write':'otheruser'},
    {'x-container-read':'.r:*', 'x-container-write':'otheruser'},
    {'x-container-read':'otheruser', 'x-container-write':'.r:*'}
    ]
    assert(len(containers)==len(headers))
    for i in range(len(containers)):
        swiftconn.put_container(containers[i])
        swiftconn.post_container(containers[i], headers[i])
        swiftconn.put_object(containers[i],'test.txt','test text here')

def create_s3_buckets(s3conn):
    buckets=[
    'as3-default',
    'as3-public-read',
    'as3-public-read-write',
    'as3-authenticated-read'
    ]
    headers=[
    {'x-amz-acl':'private'},
    {'x-amz-acl':'public-read'},
    {'x-amz-acl':'public-read-write'},
    {'x-amz-acl':'authenticated-read'}
    ]
    assert(len(buckets)==len(headers))
    for i in range(len(buckets)):
        s3conn.make_request('PUT', buckets[i], headers=headers[i])
        for i in range(len(buckets)):
            s3conn.make_request('PUT', buckets[i], buckets[i]+'.txt', headers[i], 'test text here')

    k = Key(bucket)
    k.key = 'test'
    k.set_contents_from_string('this is a test file')

def create_s3_custom_buckets(s3conn):
    # FIXME : MANUALLY UPLOAD ACLs
    containers=[
    'as3-public-write',
    'as3-private-read',
    'as3-private-write',
    'as3-private-read-write',
    'as3-public-read-private-write',
    'as3-private-read-public-write'
    ]
    for i in range(len(containers)):
        s3conn.make_request('PUT', containers[i], headers=headers[i])
        s3conn.make_request('PUT', containers[i], 'test.txt', headers[i], data='test text here')

def list_swift_acls(swiftconn):
    containers=[
    's3-private',
    's3-public-read',
    's3-public-read-write',
    's3-authenticated-read'
    ]
    for i in range(len(containers)):
        print swiftconn.head_container(containers[i])

def list_s3_acls(s3conn):
    containers=[
    's3-private',
    's3-public-read',
    's3-public-read-write',
    's3-authenticated-read'
    ]
    for i in range(len(containers)):
        resp = s3conn.make_request('GET', containers[i], query_args='acl')
        print resp.read()

def add(s3conn):
    basepath = os.path.dirname(sys.argv[0])
    filepath = os.path.abspath(os.path.join(basepath, "..", "..", "sloth.gif"))
    containers=[
    's3-private',
    's3-public-read',
    's3-public-read-write',
    's3-authenticated-read'
    ]

    for i in range(len(containers)):
        bucket = s3conn.get_bucket(containers[i])
        key = Key(bucket)
        key.key = 'sloth.gif'
        key.set_contents_from_filename(filepath)

def test_s3_object_owner(s3conn, s3user, swiftconn, swiftuser, name):
    print 'Name of the container is ' + name
    swiftconn.put_container(name)
    swiftconn.post_container(name, {'x-container-read':'user-test', 'x-container-write':'user-test'})
    r=s3user.make_request('PUT', name, 'test.txt', data='test text here')
    print r.status
    print ''

    print 'Swift container ACL(using main account):'
    print swiftconn.head_container(name)
    print ''

    resp = s3conn.make_request('GET', name, query_args='acl')
    print 'S3 container ACL (using main account):'
    print resp.read()
    print ''

    print 'Swift object ACL (using main account):'
    try:
        rdict={}
        print swiftconn.get_object(name, 'test.txt', rdict)
    except swiftclient.ClientException, e:
        print rdict
    print ''

    resp = s3conn.make_request('GET', name, 'test.txt', query_args='acl')
    print 'S3 object ACL (using main account):'
    print resp.read()
    print ''

    print 'Now using second user:'
    print 'Swift object ACL:'
    print swiftuser.head_object(name, 'test.txt')
    print ''

    resp = s3user.make_request('GET', name, 'test.txt', query_args='acl')
    print 'S3 object ACL:'
    print resp.read()

def test_swift_object_owner(s3conn, s3user, swiftconn, swiftuser, name):
    print 'Name of the bucket is ' + name
    s3conn.make_request('PUT', name)
    b=s3conn.get_bucket(name)
    b.add_user_grant('FULL_CONTROL', 'user-test')
    swiftuser.put_object(name, 'test.txt', 'test text here')
    print ''

    print 'Swift container ACL(using main account):'
    print swiftconn.head_container(name)
    print ''

    resp = s3conn.make_request('GET', name, query_args='acl')
    print 'S3 container ACL (using main account):'
    print resp.read()
    print ''

    print 'Swift object ACL (using main account):'
    try:
        rdict={}
        print swiftconn.get_object(name, 'test.txt', rdict)
    except swiftclient.ClientException, e:
        print rdict
    print ''

    resp = s3conn.make_request('GET', name, 'test.txt', query_args='acl')
    print 'S3 object ACL (using main account):'
    print resp.read()
    print ''

    print 'Now using second user:'
    print 'Swift object ACL:'
    print swiftuser.head_object(name, 'test.txt')
    print ''

    resp = s3user.make_request('GET', name, 'test.txt', query_args='acl')
    print 'S3 object ACL:'
    print resp.read()

def test_swift_container_object_owner(s3conn, s3user, swiftconn, swiftuser, name):
    print 'Name of the container is ' + name
    swiftconn.put_container(name)
    swiftconn.post_container(name, {'x-container-read':'user-test', 'x-container-write':'user-test'})
    swiftuser.put_object(name, 'test.txt', 'test text here')
    print ''

    print 'Swift container ACL(using main account):'
    print swiftconn.head_container(name)
    print ''

    resp = s3conn.make_request('GET', name, query_args='acl')
    print 'S3 container ACL (using main account):'
    print resp.read()
    print ''

    print 'Swift object ACL (using main account):'
    try:
        rdict={}
        swiftconn.get_object(name, 'test.txt', rdict)
    except swiftclient.ClientException, e:
        print rdict
    print ''

    resp = s3conn.make_request('GET', name, 'test.txt', query_args='acl')
    print 'S3 object ACL (using main account):'
    print resp.read()
    print ''

    print 'Now using second user:'
    print 'Swift object ACL:'
    print swiftuser.head_object(name, 'test.txt')
    print ''

    resp = s3user.make_request('GET', name, 'test.txt', query_args='acl')
    print 'S3 object ACL:'
    print resp.read()

def test_swift_public_container_object_owner(s3conn, s3user, swiftconn, swiftuser, name):
    print 'Name of the container is ' + name
    swiftconn.put_container(name)
    swiftconn.post_container(name, {'x-container-read':'.r:*', 'x-container-write':'.r:*'})
    swiftuser.put_object(name, 'swifttest.txt', 'test text here')
    r=s3user.make_request('PUT', name, 's3test.txt', data='test text here')
    print r.status
    print ''