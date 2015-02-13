.. _scons-targets:

###################
Scons Build Targets
###################

Below is a list of the most frequently used build targets defined by
standard ``SConstruct`` file with a brief explanation of what targets do.
List of targets (without explanation) can be produced by running
``scons -h`` command.

``all``
    This is a default target if you do not specify any target at all,
    ``"scons all"`` is equivalent to ``"scons build test"``.

``build``
    Build (compiles and links) everyhting and places built products into
    a build directory. Build directory location is determined by ``build_dir``
    variable which can be passed to ``scons`` on command line, it defaults
    to ``build`` directory in qserv source directory.

``test``
    Build and run all unit tests, unit tests are built in the same build
    directory.

``install``
    Install all built products (rebuilding them if necessary), run unit tests.
    Location of install directory is determined by the ``prefix`` variable
    which defaults to qserv source directory. ``"scons install"`` is equivalent
    to ``"scons test install-notest"``, it will fail if unit tests fail.

``install-notest``
    Install all built products but does not run unit tests. This can be useful
    in situations when some unit tests are temporarily broken by the changes
    to qserv code but developers need to continue testing with installed
    products.

``doc``
    Build documentation in ``doc/build`` directory.
