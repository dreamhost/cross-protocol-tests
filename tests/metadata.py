## RadosGW does NOT support container metadata (as of August 2013)

from tools import get_config
from tools import assert_raises
from tools import create_valid_name
from tools import get_s3conn
from tools import get_swiftconn
from tools import get_s3user
from tools import get_swiftuser
from tools import get_unauthuser

import httplib

import unittest
from nose.tools import eq_ as eq

