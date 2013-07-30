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
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        headers = {}
        for header, value in resp.getheaders():
            headers[header.lower()] = value
        return headers

    def head_bucket(self, bucket):
        # Get bucket stats (returns headers in dictionary format)
        resp = self.make_request('HEAD', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        headers = {}
        for header, value in resp.getheaders():
            headers[header.lower()] = value
        return headers

    def head_object(self, bucket, name):
        # Get object stats (returns headers in dictionary format)
        resp = self.make_request('HEAD')
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
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

    def get_contents(self, bucket, objectname):
        # Return object contents
        resp = self.make_request('GET', bucket, objectname)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        return resp.read()

    def delete_bucket(self, bucket):
        resp = self.make_request('DELETE', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())

    def delete_object(self, bucket, objectname):
        resp = self.make_request('DELETE', bucket, objectname)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())

    def put_bucket(self, bucket):
        resp = self.make_request('PUT', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        ## EASIER?
        # self.create_bucket(bucket)

    def put_object(self, bucket, objectname, data):
        resp = self.make_request('PUT', bucket, objectname, data=data)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        """
        b = self.get_bucket(bucket)
        k = Key(b)
        k.key = objectname
        k.set_contents_from_string(data)
        """

    def post_account(self):
        # Unnecessary? Add headers eventually
        resp = self.make_request('POST')
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        return resp

    def post_bucket(self, bucket):
        # Unnecessary? Add headers eventually
        resp = self.make_request('POST', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        return resp

    def post_object(self, bucket, objectname):
        # Unnecessary? Add headers eventually
        resp = self.make_request('POST', bucket, objectname)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        return resp

    def get_acl(self, bucket, objectname=None):
        # Get the ACL of the bucket or file
        b = self.get_bucket(bucket)
        if objectname:
            k = Key(b)
            k.key = objectname
            return k.get_acl()
        return b.get_acl()

    def add_public_acl(self, permission, bucket, objectname=None):
        # Add a public (Group) ACL
        grant = Grant(permission=permission, type='Group',
                      uri='http://acs.amazonaws.com/groups/global/AllUsers')
        b = self.get_bucket(bucket)
        if objectname:
            k = Key(b)
            k.key = objectname
            policy = k.get_acl()
            policy.acl.add_grant(grant)
            k.set_acl(policy)
            return k.get_acl()
        policy = b.get_acl()
        policy.acl.add_grant(grant)
        b.set_acl(policy)
        return b.get_acl()

    def add_private_acl(self, permission, username, bucket, objectname=None):
        # Add a private (CanonicalUser) ACL
        grant = Grant(permission=permission, type='CanonicalUser',
                      id=username, display_name=username)
        b = self.get_bucket(bucket)
        if objectname:
            k = Key(b)
            k.key = objectname
            policy = k.get_acl()
            policy.acl.add_grant(grant)
            k.set_acl(policy)
            return k.get_acl()
        policy = b.get_acl()
        policy.acl.add_grant(grant)
        b.set_acl(policy)
        return b.get_acl()

    def remove_public_acl(self, permission, bucket, objectname=None):
        # Remove a public (Group) ACL
        grant_type = 'Group'
        uri = 'http://acs.amazonaws.com/groups/global/AllUsers'
        b = self.get_bucket(bucket)
        if objectname:
            k = Key(b)
            k.key = objectname
            policy = k.get_acl()
            new_grants = []
            for grant in policy.acl.grants:
                if grant.permission == permission and grant.uri == uri \
                   and grant.type == grant_type:
                   continue
                else:
                    new_grants.append(grant)
            policy.acl.grants = new_grants
            k.set_acl(policy)
            return k.get_acl()
        policy = b.get_acl()
        new_grants = []
        for grant in policy.acl.grants:
            if grant.permission == permission and grant.uri == uri \
               and grant.type == grant_type:
               continue
            else:
                new_grants.append(grant)
        policy.acl.grants = new_grants
        b.set_acl(policy)
        return b.get_acl()

    def remove_private_acl(self, permission, username, bucket, objectname=None):
        # Remove a private (CanonicalUser) ACL
        grant_type = 'CanonicalUser'
        username = username
        b = self.get_bucket(bucket)
        if objectname:
            k = Key(b)
            k.key = objectname
            policy = k.get_acl()
            new_grants = []
            for grant in policy.acl.grants:
                if grant.permission == permission and \
                   grant.type == grant_type and grant.id == username:
                   continue
                else:
                    new_grants.append(grant)
            policy.acl.grants = new_grants
            k.set_acl(policy)
            return k.get_acl()
        policy = b.get_acl()
        new_grants = []
        for grant in policy.acl.grants:
            if grant.permission == permission and \
               grant.type == grant_type and grant.id == username:
               continue
            else:
                new_grants.append(grant)
        policy.acl.grants = new_grants
        b.set_acl(policy)
        return b.get_acl()

    def compare_list_objects(self, bucket):
        # Return object contents
        resp = self.make_request('GET', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        return resp.read()

    def get_md5(self, bucket, objectname):
        # Returns object md5
        resp = self.make_request('HEAD', bucket, objectname)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        for x in resp.getheaders():
            if x[0] == 'etag':
                return x[1][1:-1]

    def get_size(self, bucket, objectname=None):
        if objectname:
            # Returns object size
            resp = self.make_request('HEAD', bucket, objectname)
            if resp.status < 200 or resp.status >= 300:
                raise S3ResponseError(resp.status, resp.reason, resp.read())
            for x in resp.getheaders():
                if x[0] == 'content-length':
                    return int(x[1])
        b = self.get_bucket(bucket)
        total_size = 0
        for objectname in self.list_objects(bucket):
            k = b.lookup(objectname)
            total_size += k.size
        return total_size
    def put_random_object(self, bucket, objectname):
        # Returns object size
        f = open('/dev/urandom', 'r')
        bytes = random.randint(1,30000)
        data = f.read(bytes)
        f.close()
        self.put_object(bucket, objectname, data)
        return bytes

    def copy_object(self, bucket, objectname, destination_bucket,
                    destination_objectname):
        b = self.get_bucket(destination_bucket)
        b.copy_key(destination_objectname, bucket, objectname)

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

    def get_contents(self, container, objectname):
        # Return object get_contents
        return self.get_object(container, objectname)[1]

    def get_md5(self, container, objectname):
        # Returns object md5
        return self.head_object(container, objectname)['etag']

    def get_size(self, container, objectname=None):
        # Returns bucket content-length
        # Returns object content-length
        if objectname:
            return int(self.head_object(container, objectname)['content-length'])
        list_of_containers = self.get_account()[1]
        return next((container_dict['bytes'] for container_dict in list_of_containers if container_dict['name'] == container), None)

    def put_random_object(self, bucket, objectname):
        # Returns object size
        f = open('/dev/urandom', 'r')
        bytes = random.randint(1,30000)
        data = f.read(bytes)
        f.close()
        self.put_object(bucket, objectname, data)
        return bytes

    def copy_object(self, bucket, objectname, destination_bucket,
                    destination_objectname):
        self.put_object(container=destination_bucket,
                        obj=destination_objectname,
                        contents=None,
                        headers={'x-copy-from':bucket+'/'+objectname})

class HTTPConn(httplib.HTTPConnection):
    """
    Adapter/Wrapper class for httplib's HTTPConn
    """
    def get_contents(self, bucket, objectname):
        self.request('GET', '/'+bucket+'/'+objectname)
        resp = self.getresponse()
        if resp.status < 200 or resp.status >= 300:
            raise httplib.HTTPException(resp.status, resp.reason, resp.read())
        return resp.read()

    def put_object(self, bucket, objectname, data):
        self.request('PUT', '/'+bucket+'/'+objectname, body=data)
        resp = self.getresponse()
        if resp.status < 200 or resp.status >= 300:
            raise httplib.HTTPException(resp.status, resp.reason, resp.read())
        return resp

    def delete_object(self, bucket, objectname):
        self.request('DELETE', '/'+bucket+'/'+objectname)
        resp = self.getresponse()
        if resp.status < 200 or resp.status >= 300:
            raise httplib.HTTPException(resp.status, resp.reason, resp.read())
        return resp

    def list_objects(self, bucket):
        self.request('GET', '/'+bucket)
        resp = self.getresponse()
        if resp.status < 200 or resp.status >= 300:
            raise httplib.HTTPException(resp.status, resp.reason, resp.read())
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
