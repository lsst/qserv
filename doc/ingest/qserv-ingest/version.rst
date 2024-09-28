##########################################
The version history of the Ingest Workflow
##########################################



Version numbers
===============

Version numbers are consistent with `Ingest API version numbers <https://confluence.lsstcorp.org/display/DM/Ingest%3A+4.+The+version+history+of+the+Ingest+API>`_

The summary info on the versions
================================

Qserv ingest workflow API version management has started when ingest API version **12** was delivered, because it was offering a more advanced mechanism for working with the versions.


- Version nil: Initial version number
- :ref:`Version 12`

.. _Version 12:

Version 12: Add mandatory version numbers in configuration files
----------------------------------------------------------------

- ``metadata.json`` (see :ref:`Metadata`)

1. A mandatory ``version`` field with value *12* has been added
2. An optional ``charset_name`` field has been added, with default value to ``latin1``

.. code:: json

    {
    "version": 12,
    "charset_name": "latin1",
    "database":"...",
    }

- ``ingest.yaml`` (see :ref:`Configuration`)

1. A mandatory ``version`` field with value *12* has been added

.. code:: yaml

    version: 12
    ingest:
    ...