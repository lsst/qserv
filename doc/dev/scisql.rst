.. note::
    The oficial documentation for the ``sciSQL`` project can be found at https://smonkewitz.github.io/scisql/

======================
sciSQL Developer Notes
======================

The document presents the development methodology for ``sciSQL`` in the context of the Qserv container environment.

Making a build container
------------------------

The most straight-forward way to do this involves starting with a generic MariaDB distribution container,
then layering in the development toolchain. Build and test then take place within this customized container.
Here is an example of a small ``Dockerfile`` for this:

.. code-block:: dockerfile

    FROM mariadb:10.6
    RUN apt-get update \
        && apt-get install -y g++ git make libmariadb-dev python3 python3-pip vim \
        && pip3 install future mako mysqlclient \
        && update-alternatives --install /usr/bin/python python /usr/bin/python3 0
    ENV MARIADB_ROOT_PASSWORD=CHANGEME
    VOLUME /root

With that file the desired image is built by:

.. code-block:: bash

    docker build -t scisql-dev - < Dockerfile

The ``ENV`` and ``VOLUME`` lines of the ``Dockerfile`` file are for convenience when running the resulting container.
The stock MariaDB container already has a ``VOLUME`` for ``/var/lib/sql``. Passing this and the additional ``VOLUME``
for ``/root`` conveniently captures your development state in case the container crashes or you otherwise wish to restart it. 

Running the container
---------------------

To run the container built as above:

.. code-block:: bash

    docker run --name scisql-dev --init -d \
        -v scisql-dev-data:/var/lib/mysql \
        -v scisql-dev-home:/root scisql-dev

You need to provide names for volumes holding MariaDB state and the root user home directory. This exact same command can be
repeated to re-launch with preserved state (providing, e.g., you check out and build under ``/root`` within the container).

Now the container will start. If it is a first run on a given data volume, it will take some tens of seconds for MariaDB
to initialize. You can monitor docker logs on the container. When it is ready to go you will see "ready for connections"
near the end of the log. There will be a running MariaDB server within this container, into which scisql can be installed
and tested. 

At this point, it's recommended using a tool like ``VSCode``'s ``"connect to running container"`` to attach
the IDE to the container. It can take ``VSCode`` a little while to download and install its server-side support within
the container (another nice reason to have this persisted in the ``/root`` volume). You may wish to install a few niceties
like your favorite ``.profile``, ssh keys for GitHub, etc. now in ``/root``.

Building and testing sciSQL
---------------------------

Now, inside the container, clone out from ``github.com/smonkewitz/scisql``. Configure and build with: 

.. code-block:: bash

    git clone https://github.com/smonkewitz/scisql.git
    cd scisql
    ./configure --mysql-includes=/usr/include/mariadb
    make

From here, the somewhat non-obvious iterated incantation to rebuild, deploy into the local MariaDB, and run the test
suite is:

.. code-block:: bash

    make install && echo $MARIADB_ROOT_PASSWORD | PYTHONPATH=/usr/local/python \
        scisql-deploy.py --mysql-dir=/usr \
                         --mysql-socket=/run/mysqld/mysqld.sock \
                         --mysql-plugin-dir=/lib/mysql/plugin

If you don't want to undeploy/redeploy the UDFs and plugin, but are just iterating on the unit tests themselves,
the following shortcut version is also useful:

.. code-block:: bash

    make install && echo $MARIADB_ROOT_PASSWORD | PYTHONPATH=/usr/local/python \
        scisql-deploy.py --mysql-dir=/usr \
        --mysql-socket=/run/mysqld/mysqld.sock \
        --mysql-plugin-dir=/lib/mysql/plugin \
        --test

Updating the HTML documentation
-------------------------------

The HTML documentation is rendered by the Mako template library for Python: https://www.makotemplates.org from comments
embedded in the sources and templates in ``tools/templates/``. Incantation to build is just: 

.. code-block:: bash

    make html_docs

If you are using ``VSCode``, you can get a tunneled live view on the documentation in your working tree by popping
an additional terminal, and incanting:

.. code-block:: bash

    cd doc
    exec python -m http.server

Don't forget to add and commit the re-rendered documentation (under ``doc/``) on your PR. After a PR is merged to master,
the documentation will automatically update on github pages at https://smonkewitz.github.io/scisql/
