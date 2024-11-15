.. _ingest-api-advanced-contributions:

Options for making contribution requests
----------------------------------------

The API provides a variety of options for making contribution requests. The choice of the most suitable method depends on
the specific needs of the client application. The following list outlines the available ingest modes:

- **pull**: Tell the ingest service to pull the data from the Web server:

  - :ref:`ingest-worker-contrib-by-ref` (WORKER)

- **read**: Tell the ingest service to read the data directly from the locally mounted filesystem that is accessible
  to the worker:    

  - :ref:`ingest-worker-contrib-by-ref` (WORKER)

- **push**: Send the data over the proprietary binary protocol or ``http`` protocol to the ingest service:

  - :ref:`ingest-tools-qserv-replica-file` (TOOLS)
  - :ref:`ingest-worker-contrib-by-val` (WORKER)

  The workflow can either read the data from the local filesystem or access the data directly from memory.

All methods support the ``CSV`` data format. Additionally, the **push** mode over the ``http`` protocol also supports
the ``JSON`` format. For more details, refer to the documentation of the respective service.

The **pull** and **read** modes also support both *synchronous* and *asynchronous* data ingestion approaches.

The following diagrams illustrate the three modes of making contribution requests:

Pulling data from the Web server
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. image:: /_static/ingest-options-pull.png
   :target: ../../../_images/ingest-options-pull.png
   :alt: Pull Mode

Reading data from the local filesystem of the worker
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. image:: /_static/ingest-options-read.png
   :target: ../../../_images/ingest-options-read.png
   :alt: Read Mode

Pushing data from the workflow to the service
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. image:: /_static/ingest-options-push.png
   :target: ../../../_images/ingest-options-push.png
   :alt: Read Mode
