# S3/SWIFT CROSS PROTOCOL TESTS FOR [CEPH OBJECT GATEWAY](http://ceph.com/docs/next/radosgw/) #

---

## RUNNING THE TESTS ##

*   Modify and rename sample_config.yaml to config.yaml
*   Needs two accounts, both with S3 and Swift keys
*   Main user credentials go under MAIN USER
*   The second user credentials go under SECOND USER
*   Make sure to include the second user's USERNAME

---

## TESTS ##

### basic-sanity.py ###

Test basic operations between Swift and S3, ie:
*   Creating buckets/objects
*   Deleting buckets/objects
*   Listing buckets/objects
*   Copying objects
*   Sizes of objects
*   Checksums of objects
*   Bucket size accounting


### perms.py ###

Test different permissions across Swift and S3, ie:
*   Read objects using a variety of Swift/S3 permissions and accounts
*   Create/Delete objects using a variety of Swift/S3 permissions and accounts
*   List objects using a variety of Swift/S3 permissions and accounts

---

## NOTES ##

ETAG header using S3 API has weird formatting compared to Swift API
    eg. '"md5hash"' (S3) vs 'md5hash' (Swift)

Permissions in S3:
    Bucket:
        Read: Gives user/group permission to list buckets
        Write: Gives user/group permission to create/delete objects
    Object:
        Read: Gives user/group permission to read object
        (No write permission on objects)

Permissions in Swift:
    Bucket:
        Read: Gives user/group permission to read objects in bucket
            (Includes .rlistings, which gives permission to list objects
            in bucket - not available)
        Write: Gives user/group permission to create/delete objects
    Object:
        (No object permissions)

Swift ACLs:
    Cannot create containers with custom ACL using a PUT request,
        must use a POST request updating the ACL after the container
        is created

"Unauthorized user" using HTTPConnection:
    Using the path /gateway/swift/bucket/object is the same as
        using the path /gateway/bucket/object
    Responses return appropriately (using the Swift path returns
        a Swift response and vice versa)

Swift Public Write Containers:
    Changing the S3 bucket permissions overrides the Swift
        public write permission

Custom Object metadata:
    Updating object custom metadata using Swift/S3 will overwrite any
        existing custom metadata
    Object custom metadata can be updated with the header
        'x-container-meta-{key}' which shouldn't be allowed
