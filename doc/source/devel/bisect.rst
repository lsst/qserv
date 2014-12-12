########################
Track a commit for a bug
########################

Qserv test provide a tool which automate testing a given version of Qserv source code.

**************
Basic use case
**************

Please follow next procedure ref:`quick-start-devel-setup-qserv` and then:

.. code-block:: bash

   setup --keep qserv_testdata
   qserv-test-head.sh

***************
With git bisect
***************

`git bisect` is a powerful tool which allow to track the commit which introduced a bug in a given branch. Please see online help for instructions:

.. code-block:: bash

   qserv-test-head.sh -h
