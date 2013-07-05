import yaml
import sys
import os

## FROM https://github.com/ceph/swift/blob/master/test/__init__.py
def get_config():
    """
    Attempt to get a functional config dictionary.
    """
    # config_file = os.environ.get('CROSS_PROTOCOL_TEST_CONFIG_FILE')
    try:
        # Get config.yaml in the same directory
        __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))
        f = open(os.path.join(__location__, 'config.yaml'))
        # use safe_load instead load
        conf = yaml.safe_load(f)
        f.close()
    except IOError:
        print >>sys.stderr, 'UNABLE TO READ FUNCTIONAL TESTS CONFIG FILE'
    return conf
"""
conf = get_config()
s3keys = conf['s3']
swiftkeys = conf['swift']

s3conn = boto.connect_s3(
    aws_access_key_id = s3keys['aws_access_key_id'],
    aws_secret_access_key = s3keys['aws_secret_access_key'],
    host = s3keys['host'],
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )

swiftconn = swiftclient.Connection(
    authurl = swiftkeys['authurl'],
    user = swiftkeys['user'],
    key = swiftkeys['key']
    )
"""
