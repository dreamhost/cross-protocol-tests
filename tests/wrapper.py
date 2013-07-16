import boto
from boto.s3.connection import S3Connection
import boto.s3.acl
import boto.s3
from boto.s3.key import Key
from boto.exception import S3ResponseError

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
        resp = self.make_request('HEAD')
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def head_bucket(self, bucket):
        resp = self.make_request('HEAD', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def head_object(self, bucket, name):
        resp = self.make_request('HEAD')
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def get_account(self):
        resp = self.make_request('GET')
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def get_bucket(self, bucket):
        resp = self.make_request('GET', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def get_object(self, bucket, filename):
        resp = self.make_request('GET', bucket, filename)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def delete_bucket(self, bucket):
        resp = self.make_request('DELETE', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def delete_object(self, bucket, filename):
        resp = self.make_request('DELETE', bucket, filename)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def post_account(self):
        resp = self.make_request('POST')
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def post_bucket(self, bucket):
        resp = self.make_request('POST', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def post_object(self, bucket, filename):
        resp = self.make_request('POST', bucket, filename)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def put_bucket(self, bucket):
        resp = self.make_request('PUT', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def put_object(self, bucket, filename, data):
        resp = self.make_request('PUT', bucket, filename, data)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def get_acl(self, bucket, filename=None, acl):
        resp = self.make_request('GET', bucket, filename, data=acl, query_arges='acl')
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
        return resp
    def set_acl(self, bucket, filename=None, acl):
        resp = self.make_request('POST', bucket, filename, data=acl, query_arges='acl')
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read)
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