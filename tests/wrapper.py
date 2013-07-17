import boto
from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import boto.s3.acl
import boto.s3

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

class S3Conn(S3Connection):
    def head_account(self):
        # Get account stats (returns headers in dictionary format)
        resp = self.make_request('HEAD')
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        headers = {}
        return headers[header.lower()] = value for header, value in resp.getheaders()
    def head_bucket(self, bucket):
        # Get bucket stats (returns headers in dictionary format)
        resp = self.make_request('HEAD', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        headers = {}
        return headers[header.lower()] = value for header, value in resp.getheaders()
    def head_object(self, bucket, name):
        # Get object stats (returns headers in dictionary format)
        resp = self.make_request('HEAD')
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        headers = {}
        return headers[header.lower()] = value for header, value in resp.getheaders()
    def list_buckets(self):
        # Returns list of buckets
        buckets = self.get_all_buckets()
        list_of_buckets = [bucket.name for bucket in buckets]
        return list_of_buckets
    def list_objects(self, bucket):
        # Returns list of objects in a bucket
        bucket = s3conn.get_bucket(name)
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
        s3conn.create_bucket(bucket)
    def put_object(self, bucket, filename, data):
        resp = self.make_request('PUT', bucket, filename, data)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        ## EASIER?
        b = s3conn.get_bucket(bucket)
        k = Key(b)
        k.key = filename
        k.set_contents_from_string(data)
    def post_account(self):
        # Unnecessary?
        resp = self.make_request('POST')
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def post_bucket(self, bucket):
        # Unnecessary?
        resp = self.make_request('POST', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def post_object(self, bucket, filename):
        # Unnecessary?
        resp = self.make_request('POST', bucket, filename)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def get_acl(self, bucket, filename=None):
        b = self.get_bucket(bucket)
        if filename:
            k = Key(b)
            k.key = filename
            return k.get_acl()
        return b.get_acl()
    def add_public_acl(self, permission, bucket, filename=None):
        grant = Grant(permission=permission, type='Group', uri='http://acs.amazonaws.com/groups/global/AllUsers')
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
        grant = Grant(permission=permission, type='CanonicalUser', id=username, display_name=username)
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

class SwiftConn(Connection):
    def list_containers(self):
        # Returns list of containers
        containers = self.get_account()[1]
        list_of_containers = [container_dict[u'name'] for container_dict in containers]
        return list_of_containers
    def list_objects(self, container):
        # Returns list of objects
        objects = self.get_container(name)[1]
        list_of_objects = [object_dictionary[u'name'] for object_dictionary in objects]
        return list_of_objects
    def get_contents(self, container, filename):
        # Return object get_contents
        return self.get_object(container, filename)[1]
class HTTPConn(httplib.HTTPConnection):
    def get_contents(self, container, filename):
        self.request('GET', '/'+container+'/'+filename)
        resp = self.getresponse()
        return resp
    def put_object(self, container, filename, data):
        self.request('GET','/'+container+'/'+filename, body=data)
        resp = self.getresponse()
        return resp
# For more parameters:
# https://github.com/boto/boto/blob/develop/boto/s3/connection.py
def s3conn():
    return S3Conn(
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
def get_unauthuser():
    return HTTPConn('objects.dreamhost.com')