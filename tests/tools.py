import boto
import boto.s3.connection
import boto.s3

from boto.exception import S3ResponseError
from boto.s3.key import Key
from boto.s3.acl import Grant

import swiftclient

import yaml

import httplib
import os.path
import random
import sys


## FROM https://github.com/ceph/swift/blob/master/test/__init__.py
def get_config():
    """
    Attempt to get a functional config dictionary.
    """
    # config_file = os.environ.get('CROSS_PROTOCOL_TEST_CONFIG_FILE')
    try:
        # Get config.yaml in the same directory
        __location__ = os.path.realpath(os.path.join(os.getcwd(),
                                        os.path.dirname(__file__)))
        f = open(os.path.join(__location__, 'config.yaml'))
        # use safe_load instead load
        conf = yaml.safe_load(f)
        f.close()
    except IOError:
        print >>sys.stderr, 'UNABLE TO READ FUNCTIONAL TESTS CONFIG FILE'
    return conf


# FROM https://github.com/ceph/s3-tests/blob/master/s3tests/functional/utils.py
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


def create_valid_name():
    """
    Create a valid bucket name usable by both S3 and Swift
    """
    name_length = random.randint(3, 63)
    name = ''
    for x in range(name_length):
        if x == 0 or x == name_length - 1:
            name += chr(random.randint(97, 122))
        elif x != 0 and name[x - 1] in '-.':
            name += chr(random.randint(97, 122))
        else:
            name += random.choice(chr(random.randint(97, 122)) + '-')
    return name


class S3Conn(boto.s3.connection.S3Connection):
    """
    Adapter/Wrapper class for boto's S3Connection
    """
    def head_account(self):
        # Get account stats (returns headers in dictionary format)
        resp = self.make_request('HEAD')
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        headers = {}
        for header, value in resp.getheaders():
            headers[header.lower()] = value
        return headers

    def head_bucket(self, bucket):
        # Get bucket stats (returns headers in dictionary format)
        resp = self.make_request('HEAD', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        headers = {}
        for header, value in resp.getheaders():
            headers[header.lower()] = value
        return headers

    def head_object(self, bucket, name):
        # Get object stats (returns headers in dictionary format)
        resp = self.make_request('HEAD')
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        headers = {}
        for header, value in resp.getheaders():
            headers[header.lower()] = value
        return headers

    def list_buckets(self):
        # Returns list of buckets
        buckets = self.get_all_buckets()
        list_of_buckets = [bucket.name for bucket in buckets]
        return list_of_buckets

    def list_objects(self, bucket):
        # Returns list of objects in a bucket
        bucket = self.get_bucket(bucket)
        list_of_objects = [obj.key for obj in bucket.list()]
        return list_of_objects

    def get_contents(self, bucket, filename):
        # Return object contents
        resp = self.make_request('GET', bucket, filename)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp.read()

    def delete_bucket(self, bucket):
        resp = self.make_request('DELETE', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)

    def delete_object(self, bucket, filename):
        resp = self.make_request('DELETE', bucket, filename)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)

    def put_bucket(self, bucket):
        resp = self.make_request('PUT', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        ## EASIER?
        # self.create_bucket(bucket)

    def put_object(self, bucket, filename, data):
        resp = self.make_request('PUT', bucket, filename, data=data)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        """
        b = self.get_bucket(bucket)
        k = Key(b)
        k.key = filename
        k.set_contents_from_string(data)
        """

    def post_account(self):
        # Unnecessary? Add headers eventually
        resp = self.make_request('POST')
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp

    def post_bucket(self, bucket):
        # Unnecessary? Add headers eventually
        resp = self.make_request('POST', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp

    def post_object(self, bucket, filename):
        # Unnecessary? Add headers eventually
        resp = self.make_request('POST', bucket, filename)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp

    def get_acl(self, bucket, filename=None):
        # Get the ACL of the bucket or file
        b = self.get_bucket(bucket)
        if filename:
            k = Key(b)
            k.key = filename
            return k.get_acl()
        return b.get_acl()

    def add_public_acl(self, permission, bucket, filename=None):
        # Add a public (Group) ACL
        grant = Grant(permission=permission, type='Group',
                      uri='http://acs.amazonaws.com/groups/global/AllUsers')
        b = self.get_bucket(bucket)
        if filename:
            k = Key(b)
            k.key = filename
            policy = k.get_acl()
            policy.acl.add_grant(grant)
            k.set_acl(policy)
            return k.get_acl()
        policy = b.get_acl()
        policy.acl.add_grant(grant)
        b.set_acl(policy)
        return b.get_acl()

    def add_private_acl(self, permission, username, bucket, filename=None):
        # Add a private (CanonicalUser) ACL
        grant = Grant(permission=permission, type='CanonicalUser',
                      id=username, display_name=username)
        b = self.get_bucket(bucket)
        if filename:
            k = Key(b)
            k.key = filename
            policy = k.get_acl()
            policy.acl.add_grant(grant)
            k.set_acl(policy)
            return k.get_acl()
        policy = b.get_acl()
        policy.acl.add_grant(grant)
        b.set_acl(policy)
        return b.get_acl()

    def compare_list_objects(self, bucket):
        # Return object contents
        resp = self.make_request('GET', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp.read()

    def get_md5(self, bucket, filename):
        # Returns object md5
        resp = self.make_request('HEAD', bucket, filename)
        for x in resp.getheaders():
            if x[0] == 'etag':
                return x[1][1:-1]

    def get_size(self, bucket, filename):
        # Returns object size
        resp = self.make_request('HEAD', bucket, filename)
        for x in resp.getheaders():
            if x[0] == 'content-length':
                return x[1]


class SwiftConn(swiftclient.Connection):
    """
    Adapter/Wrapper class for swiftclient's Connection
    """
    def list_containers(self):
        # Returns list of containers
        containers = self.get_account()[1]
        list_of_containers = [container_dict[u'name']
                              for container_dict in containers]
        return list_of_containers

    def list_objects(self, container):
        # Returns list of objects
        objects = self.get_container(container)[1]
        list_of_objects = [object_dictionary[u'name']
                           for object_dictionary in objects]
        return list_of_objects

    def get_contents(self, container, filename):
        # Return object get_contents
        return self.get_object(container, filename)[1]

    def get_md5(self, container, filename):
        # Returns object md5
        return self.head_object(container, filename)['etag']

    def get_size(self, container, filename):
        # Returns object content-length
        return int(self.head_object(container, filename)['content-length'])


class HTTPConn(httplib.HTTPConnection):
    """
    Adapter/Wrapper class for httplib's HTTPConn
    """
    def get_contents(self, bucket, filename):
        self.request('GET', '/'+bucket+'/'+filename)
        resp = self.getresponse()
        if resp.status < 200 or resp.status >= 300:
            raise httplib.HTTPException
        return resp.read()

    def put_object(self, bucket, filename, data):
        self.request('PUT', '/'+bucket+'/'+filename, body=data)
        resp = self.getresponse()
        if resp.status < 200 or resp.status >= 300:
            raise httplib.HTTPException
        return resp

    def delete_object(self, bucket, filename):
        self.request('DELETE', '/'+bucket+'/'+filename)
        resp = self.getresponse()
        if resp.status < 200 or resp.status >= 300:
            raise httplib.HTTPException
        return resp

    def list_objects(self, bucket):
        self.request('GET', '/'+bucket)
        resp = self.getresponse()
        if resp.status < 200 or resp.status >= 300:
            raise httplib.HTTPException
        return resp.read()


# For more parameters:
# https://github.com/boto/boto/blob/develop/boto/s3/connection.py
def get_s3conn():
    """
    Return the main user's S3 connection
    """
    conf = get_config()
    s3keys = conf['s3']
    return S3Conn(
        aws_access_key_id=s3keys['aws_access_key_id'],
        aws_secret_access_key=s3keys['aws_secret_access_key'],
        host=s3keys['host'],
        is_secure=True,
        port=None,
        proxy=None,
        proxy_port=None,
        https_connection_factory=None,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        debug=0
    )


def get_s3user():
    """
    Return the second user's S3 connection
    """
    conf = get_config()
    s3user = conf['s3user']
    return S3Conn(
        aws_access_key_id=s3user['aws_access_key_id'],
        aws_secret_access_key=s3user['aws_secret_access_key'],
        host=s3user['host'],
        is_secure=True,
        port=None,
        proxy=None,
        proxy_port=None,
        https_connection_factory=None,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        debug=0
    )


# For more parameters:
# https://github.com/openstack/python-swiftclient/blob/master/swiftclient/client.py
def get_swiftconn():
    """
    Return the main user's Swift connection
    """
    conf = get_config()
    swiftkeys = conf['swift']
    return SwiftConn(
        authurl=swiftkeys['authurl'],
        user=swiftkeys['user'],
        key=swiftkeys['key'],
        preauthurl=None
    )


def get_swiftuser():
    """
    Return the second user's Swift connection
    """
    conf = get_config()
    swiftuser = conf['swiftuser']
    return SwiftConn(
        authurl=swiftuser['authurl'],
        user=swiftuser['user'],
        key=swiftuser['key'],
        preauthurl=None
    )


def get_unauthuser():
    """
    Return an unauthenticated http connection
    """
    conf = get_config()
    s3keys = conf['s3']
    return HTTPConn(s3keys['host'])