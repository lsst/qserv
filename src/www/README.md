This directory holds static web content served at runtime by Qserv. At present, this is principally
the administration dashboard app.

Contents of this directory (excepting `CMakeLists.txt` and `README.md`) will be copied to
`/usr/local/qserv/www` in the Qserv run container image, and served from that location by the `qhttp` server
embedded in the replication controller (see `replica/HttpProcessor.cc`.)
