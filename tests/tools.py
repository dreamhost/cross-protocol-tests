import httplib
import os.path
import random
import string
import sys

import boto
import boto.s3.connection
import boto.s3
from boto.exception import S3ResponseError
from boto.s3.key import Key
from boto.s3.acl import Grant
import swiftclient
import yaml


# FROM https://github.com/ceph/swift/blob/master/test/__init__.py
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


def create_valid_name(length=None):
    """
    Create a valid bucket name usable by both S3 and Swift
    """
    if length is None:
        name_length = 15
    else:
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


# FROM https://github.com/ceph/swift/blob/master/test/functional/tests.py
def create_valid_utf8_name(length=None):
    """
    Create a valid UTF-8 name
    """
    if length is None:
        length = 15
    else:
        length = int(length)
    utf8_chars = u'\uF10F\uD20D\uB30B\u9409\u8508\u5605\u3703\u1801'\
                 u'\u0900\uF110\uD20E\uB30C\u940A\u8509\u5606\u3704'\
                 u'\u1802\u0901\uF111\uD20F\uB30D\u940B\u850A\u5607'\
                 u'\u3705\u1803\u0902\uF112\uD210\uB30E\u940C\u850B'\
                 u'\u5608\u3706\u1804\u0903\u03A9\u2603'
    """
    # ALTERNATIVE UTF-8 CHARS (random Greek letters):
    utf8_chars = u'\u0388\u0389\u038A\u038C\u038E\u038F\u0380'\
                 u'\u0390\u0391\u0392\u0393\u0394\u0395\u0396'\
                 u'\u0397\u0398\u0399\u039A\u039B\u039C\u039D'\
                 u'\u039E\u039F\u03A0\u03A1\u03A3\u03A4\u03A5'
    """
    return ''.join([random.choice(utf8_chars) for x in
                    xrange(length)]).encode('utf-8')


def delete_buckets(main=True):
    """
    Delete all objects and buckets from the acount
    """
    # Delete everything from main account
    if main:
        swiftconn = get_swiftconn()
        for container in swiftconn.list_containers():
            objects = swiftconn.list_objects(container)
            for name in objects:
                swiftconn.delete_object(container, name)
            swiftconn.delete_container(container)
    # Delete everything from second account
    else:
        swiftuser = get_swiftuser()
        for container in swiftuser.list_containers():
            objects = swiftuser.list_objects(container)
            for name in objects:
                swiftuser.delete_object(container, name)
            swiftuser.delete_container(container)

    # Does not work with boto
    """
    s3conn = get_s3conn()
    for bucket in s3conn.list_buckets():
        b = s3conn.get_bucket(bucket)
        keys = b.list()
        for key in keys:
            key.delete()
        s3conn.delete_bucket(bucket)
    """


def generate_random_string(length=8):
    """
    Create random string for custom object metadata
    """
    return ''.join(random.choice(string.ascii_lowercase + string.digits)
                   for x in range(length))


class S3Conn(boto.s3.connection.S3Connection):
    """
    Adapter/Wrapper class for boto's S3Connection
    """
    def head_account(self):
        # Get account stats (returns a dictionary of headers)
        resp = self.make_request('HEAD')
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        headers = {}
        for header, value in resp.getheaders():
            headers[header.lower()] = value
        return headers

    def head_bucket(self, bucket):
        # Get bucket stats (returns a dictionary of headers)
        resp = self.make_request('HEAD', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        headers = {}
        for header, value in resp.getheaders():
            headers[header.lower()] = value
        return headers

    def head_object(self, bucket, objectname):
        # Get object stats (returns a dictionary of headers)
        resp = self.make_request('HEAD', bucket, objectname)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        headers = {}
        for header, value in resp.getheaders():
            headers[header.lower()] = value
        return headers

    def list_buckets(self):
        # Returns a list of buckets
        buckets = self.get_all_buckets()
        list_of_buckets = [(bucket.name).encode('utf-8') for bucket in buckets]
        return list_of_buckets

    def list_objects(self, bucket, prefix=None, delimiter=None):
        # PREFIX should be of the form 'folder/subfolder/'
        # Returns a list of objects in a bucket
        bucket = self.get_bucket(bucket)
        if prefix:
            list_of_keys = [(key.name).encode('utf-8') for key
                            in bucket.list(prefix, delimiter=delimiter)]
            return list_of_keys
        list_of_objects = [(key.name).encode('utf-8') for key
                           in bucket.list(delimiter=delimiter)]
        return list_of_objects

    def get_contents(self, bucket, objectname):
        # Returns object content
        resp = self.make_request('GET', bucket, objectname)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        return resp.read()

    def delete_bucket(self, bucket):
        # Delete bucket
        resp = self.make_request('DELETE', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())

    def delete_object(self, bucket, objectname):
        # Delete object
        resp = self.make_request('DELETE', bucket, objectname)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())

    def put_bucket(self, bucket):
        # Create bucket
        resp = self.make_request('PUT', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        """
        ALTERNATIVE METHOD:
        self.create_bucket(bucket)
        """

    def put_object(self, bucket, objectname, data):
        # Create object
        resp = self.make_request('PUT', bucket, objectname, data=data)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        """
        ALTERNATIVE METHOD:
        b = self.get_bucket(bucket)
        k = Key(b)
        k.key = objectname
        k.set_contents_from_string(data)
        """

    def add_custom_metadata(self, bucket, objectname, key_value_pairs):
        # Adds custom metadata to object
        headers = {}
        for pair in key_value_pairs:
            key, value = pair
            headers['x-amz-meta-' + key] = value
        resp = self.make_request('PUT', bucket, objectname, headers=headers)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())

    def list_custom_metadata(self, bucket, objectname):
        # Returns a list of custom metadata
        resp = self.make_request('HEAD', bucket, objectname)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        headers = resp.getheaders()
        metadata = []
        for header in headers:
            key, value = header
            if key.startswith('x-amz-meta-'):
                metadata += [(key[len('x-amz-meta-'):], value)]
        return metadata

    def get_acl(self, bucket, objectname=None):
        # Returns the ACL of the bucket or file
        b = self.get_bucket(bucket)
        if objectname:
            k = Key(b)
            k.key = objectname
            return k.get_acl()
        return b.get_acl()

    def add_public_acl(self, permission, bucket, objectname=None):
        # Adds a public (Group) ACL to the bucket or file, then
        # returns the ACL of the bucket or file
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
        # Adds a private (CanonicalUser) ACL to the bucket or file, then
        # returns the ACL of the bucket or file
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
        # Removes a public (Group) ACL to the bucket or file, then
        # returns the ACL of the bucket or file
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

    def remove_private_acl(self, permission, username,
                           bucket, objectname=None):
        # Remove a private (CanonicalUser) ACL to the bucket or file
        # Returns the ACL of the bucket or file
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
        # Returns object contents
        resp = self.make_request('GET', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        return resp.read()

    def get_md5(self, bucket, objectname):
        # Returns object md5
        # NOTE: The S3 API's 'etag' header contains the md5 hash in nested
        # quotes, eg. '"md5hash"'
        resp = self.make_request('HEAD', bucket, objectname)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        for x in resp.getheaders():
            if x[0] == 'etag':
                return x[1][1:-1]

    def get_size(self, bucket, objectname=None):
        # Returns size of bucket or object
        if objectname:
            resp = self.make_request('HEAD', bucket, objectname)
            if resp.status < 200 or resp.status >= 300:
                raise S3ResponseError(resp.status, resp.reason, resp.read())
            for x in resp.getheaders():
                if x[0] == 'content-length':
                    return int(x[1])

        resp = self.make_request('HEAD', bucket)
        if resp.status < 200 or resp.status >= 300:
            raise S3ResponseError(resp.status, resp.reason, resp.read())
        headers = resp.getheaders()
        return int(next((header[1] for header in headers
                         if header[0] == 'x-rgw-bytes-used'), None))

    def put_random_object(self, bucket, objectname):
        # Creates and uploads a random object with random size, then
        # returns the size of object
        f = open('/dev/urandom', 'r')
        bytes = random.randint(1, 30000)
        data = f.read(bytes)
        f.close()
        self.put_object(bucket, objectname, data)
        return bytes

    def copy_object(self, bucket, objectname, destination_bucket,
                    destination_objectname, preserve_acl=False):
        # Copies object from bucket to destination bucket
        b = self.get_bucket(destination_bucket)
        b.copy_key(destination_objectname, bucket,
                   objectname, preserve_acl=preserve_acl)


class SwiftConn(swiftclient.Connection):
    """
    Adapter/Wrapper class for swiftclient's Connection
    """
    def list_containers(self):
        # Returns a list of containers
        containers = self.get_account()[1]
        list_of_containers = [(container_dict[u'name']).encode('utf-8')
                              for container_dict in containers]
        return list_of_containers

    def list_objects(self, container):
        # Returns a list of objects
        objects = self.get_container(container)[1]
        list_of_objects = [(object_dictionary[u'name']).encode('utf-8')
                           for object_dictionary in objects]
        return list_of_objects

    def get_contents(self, container, objectname):
        # Return object contents
        return self.get_object(container, objectname)[1]

    def get_md5(self, container, objectname):
        # Returns object md5
        return self.head_object(container, objectname)['etag']

    def get_size(self, container, objectname=None):
        # Returns size of bucket or object
        if objectname:
            return int(self.head_object(container, objectname)
                       ['content-length'])
        headers = self.head_container(container)
        return int(headers['x-container-bytes-used'])

    def put_random_object(self, bucket, objectname):
        # Creates and uploads a random object with random size, then
        # returns the size of object
        f = open('/dev/urandom', 'r')
        bytes = random.randint(1, 30000)
        data = f.read(bytes)
        f.close()
        self.put_object(bucket, objectname, data)
        return bytes

    def copy_object(self, bucket, objectname, destination_bucket,
                    destination_objectname):
        # Copies object from bucket to destination bucket
        self.put_object(container=destination_bucket,
                        obj=destination_objectname,
                        contents=None,
                        headers={'x-copy-from': bucket+'/'+objectname})

    def add_custom_metadata(self, bucket, objectname, key_value_pairs):
        # Add metadata to the object
        headers = {}
        for pair in key_value_pairs:
            key, value = pair
            headers['x-object-meta-'+key] = value
        self.post_object(bucket, objectname, headers=headers)

    def list_custom_metadata(self, bucket, objectname):
        # Returns list of custom metadata
        headers = self.head_object(bucket, objectname)
        metadata = []
        for header in headers:
            if header.startswith('x-object-meta-'):
                metadata += [(header[len('x-object-meta-'):], headers[header])]
        return metadata


class HTTPConn(httplib.HTTPConnection):
    """
    Adapter/Wrapper class for httplib's HTTPConn
    """
    def get_contents(self, bucket, objectname):
        # Returns object contents
        self.request('GET', '/'+bucket+'/'+objectname)
        resp = self.getresponse()
        if resp.status < 200 or resp.status >= 300:
            raise httplib.HTTPException(resp.status, resp.reason, resp.read())
        return resp.read()

    def put_object(self, bucket, objectname, data):
        # Create object
        self.request('PUT', '/'+bucket+'/'+objectname, body=data)
        resp = self.getresponse()
        if resp.status < 200 or resp.status >= 300:
            raise httplib.HTTPException(resp.status, resp.reason, resp.read())
        return resp

    def delete_object(self, bucket, objectname):
        # Delete object
        self.request('DELETE', '/'+bucket+'/'+objectname)
        resp = self.getresponse()
        if resp.status < 200 or resp.status >= 300:
            raise httplib.HTTPException(resp.status, resp.reason, resp.read())
        return resp

    def list_objects(self, bucket):
        # List objects
        self.request('GET', '/'+bucket)
        resp = self.getresponse()
        if resp.status < 200 or resp.status >= 300:
            raise httplib.HTTPException(resp.status, resp.reason, resp.read())
        return resp.read()


# For more parameters (s3conn):
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


# For more parameters (swiftconn):
# https://github.com/openstack/python-swiftclient/blob/master/swiftclient/client.py
def get_swiftconn():
    """
    Returns the main user's Swift connection
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
    Returns the second user's Swift connection
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
    Returns an unauthenticated http connection
    """
    conf = get_config()
    s3keys = conf['s3']
    return HTTPConn(s3keys['host'])
