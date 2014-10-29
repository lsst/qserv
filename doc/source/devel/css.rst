###################
CSS metadata layout
###################

The Central State System (CSS) maintains information about the data
stored in Qserv. Currently, this consists of the data schema: the
names of databases and what tables they contain, the partitioning
parameters used by the database, etc.

**************
Implementation
**************

CSS content is stored in a hierarchical key-value structure, with keys
structured much like filename paths.
In standard Qserv installations, CSS data is kept in a Zookeeeper
cluster, although parts of qserv may be tested with CSS content stored
in a more feature-light form, e.g., a file.

***********
Packed keys
***********

In order to reduce the number of key-value updates when manipulating
CSS content, some portions of the content tree are stored packed in
JSON format. This is signified by a ".json" in the key name. The
presence of ".json" in a key name indicates that its contents should
be interpreted as children of the key. For example, suppose CSS
content includes (specified in python dict syntax):
.. code-block:: Python
  { "/foo/bar" : "12345",
    "/foo/bar.json" : '{"a":"aa", "b":"bb"}'}

The key "/foo/bar.json" is unpacked, thus the above content will be
interpreted as:
..code-block::Python
  { "/foo/bar" : "12345",
    "/foo/bar/a" : "aa",
    "/foo/bar/b" : "bb" }

Note that the presence of "/foo/bar.json" does not prevent the
presence of "/foo/bar". In this version, the value for the parent
"/foo/bar" may not be stored in "/foo/bar.json", and so the
JSON-encoded string in "/foo/bar.json" must contain a key-value
structure (this may change in the future).

