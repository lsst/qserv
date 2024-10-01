.. _ingest-api-advanced-contributions:

Options for making contribution requests
----------------------------------------

The API ofers a wide spectrum of options for making contribution requests. The choice of the most appropriate method depends on
the specific requirements of the client application. The following list summarizes the available options:

- **pull** mode: tell the ingest service to pull the data from the Web server:

  - :ref:`ingest-worker-contrib-by-ref` (WORKER)

- **read** mode: tell the ingest service to read the data directly from the locally mounted filesystem that is accessible
  to the worker:    

  - :ref:`ingest-worker-contrib-by-ref` (WORKER)

- **push** mode: send the data over the proprietary binary protocol or ``http`` protocol to the ingest service:

  - :ref:`ingest-tools-qserv-replica-file` (TOOLS)
  - :ref:`ingest-worker-contrib-by-val` (WORKER)

  Where the workflow may read the data from the local filesystem or have the data in memory.

In terms of supported data formats, all methods supports ``CSV``. In addition, the **push** mode over ``http`` protocol also
supports ``JSON`` format. See the documentation on the corresponding service for more details.

The **pull** and **read** modes also allow the workflow to ingest data using the *synchronous* or *asynchronous* approaches.

Pulling data from the Web server
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The idea is illustrated on the following diagram:

.. image:: /_static/ingest-options-pull.png
   :target: ../../../_images/ingest-options-pull.png
   :alt: Pull Mode

Reading data from the local filesystem of the worker
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The idea is illustrated on the following diagram:

.. image:: /_static/ingest-options-read.png
   :target: ../../../_images/ingest-options-read.png
   :alt: Read Mode

Pushing data from the workflow to the service
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The idea is illustrated on the following diagram:

.. image:: /_static/ingest-options-push.png
   :target: ../../../_images/ingest-options-push.png
   :alt: Read Mode
