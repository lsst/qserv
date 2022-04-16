###################
CSS metadata layout
###################

The Central State System (CSS) maintains information about the data stored in
Qserv. Currently, this consists of the data schema: the names of databases and
what tables they contain, the partitioning parameters used by the database, etc.

**************
Implementation
**************

CSS content is stored in a hierarchical key-value structure, with keys
structured much like filename paths. In standard Qserv installations, CSS data
is kept in a MySQL database, although parts of qserv may be tested with CSS
content stored in a more feature-light form, e.g., a file.

***********
Packed keys
***********

In order to reduce the number of key-value updates when manipulating CSS
content, some portions of the content tree are stored packed in JSON format.
This is signified by presence of ".packed.json" key. The content of this key is
a structure (JSON object) which contains some of the keys appearing at the same
level as ".packed.json" key. For example, suppose CSS content includes
(specified in python dict syntax):

.. code-block:: python

    { "/foo/bar" : "12345",
      "/foo/bar/baz" : "regular data",
      "/foo/bar/.packed.json" : '{"a":"aa", "b":"bb"}' }

The key "/foo/bar/.packed.json" is unpacked, thus the above content will be
interpreted as:

.. code-block:: python

    { "/foo/bar" : "12345",
      "/foo/bar/baz" : "regular data",
      "/foo/bar/a"   : "aa",
      "/foo/bar/b"   : "bb" }

Note that the presence of "/foo/bar/.packed.json" does not prevent the presence
of "/foo/bar/baz". It is not specified what happens if the same key appears in
both regular CSS structure and in packed key value like in this structure:

.. code-block:: python

    # it is unspecified which value /foo/bar/a has when unpacked
    { "/foo/bar" : "12345",
      "/foo/bar/a" : "regular key data",
      "/foo/bar/.packed.json" : '{"a":"aa", "b":"bb"}' }

The values in JSON object can be simple types like strings or numbers, complex
objects are not supported there. All values in JSON object are transformed to
strings when unpacked, so the packed object ``"{"key": 1}"`` is treated
identically to ``"{"key": "1"}"``.

*************
CSS interface
*************

CSS key-value storage is hidden completely behind CSS interface
(``css/CssAccess`` class). This interface defines all logical operations on
databases, tables, nodes, etc. and translates those operations into key-value
structure manipulatons, including packing and upacking of the keys.
