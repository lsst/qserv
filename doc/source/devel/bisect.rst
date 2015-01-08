########################
Track a commit for a bug
########################

Qserv integration tests provide a tool which automates testing a given
version of Qserv source code.

**************
Basic use case
**************

Please follow next procedure :ref:`quick-start-devel-setup-qserv` and then:

.. code-block:: bash

   setup --keep qserv_testdata
   qserv-test-head.sh

   # see online help for additional informations
   qserv-test-head.sh -h

Previous command will build, install and configure a Qserv mono-node instance
using a given Qserv source repository. It will then launch integration tests
against it. The whole process is logged to standard ouput and the command
returns 0 if successful.

***************
With git bisect
***************

`git bisect` is a powerful tool which allow to track the commit which introduced a bug in a given branch. Please see online help for instructions.
