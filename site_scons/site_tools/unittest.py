"""
Tool-specific initialization for UnitTest and UnitTestCheck builders.

This module defines SCons tool (with name unittest) which handles running of
the unit tests during the builds. The tool defines two builders with the name
UnitTest and UnitTestCheck, former is used to define target for running
individual unit test, the target is the log file produced by test application,
it may be empty. UnitTest builder does not fail if test application returns
non-zero code, instead it records failure in the environment variable
UNIT_TESTS_FAILED (which is a list of all failed unit test executables).

By default log file created by UnitTest has the same name as the unit test
executable with ".utest" suffix. If unit test fails this log file renamed to
have ".utest.failed" suffix.

If it is desired to abort the build (or just produce scons error in the build)
if any unit test fails then one should use UnitTestCheck builder which checks
UNIT_TESTS_FAILED variable and generates error if this variable is not empty.

Example of the use of UnitTest builder (after initializing unittest tool):
\\code
    test1 = env.Program("utest1.cc")
    utest1 = env.UnitTest(test1)
    test2 = env.Program("utest2.cc")
    utest2 = env.UnitTest(test2)
    env.Alias("test", [utest1, utest2])
\\endcode

Example which also checks unit test results:
\\code
    test1 = env.Program("utest1.cc")
    utest1 = env.UnitTest(test1)
    test2 = env.Program("utest2.cc")
    utest2 = env.UnitTest(test2)

    # target should heve some unique name
    utest = env.UnitTestCheck("unit-test-tag", [utest1, utest2])
    env.Alias("test", utest)
\\endcode
"""

import os
import shutil

from SCons.Builder import Builder
from SCons.Errors import StopError


class _unitTest(object):
    """
    Class which implements builder action for UnitTest.
    """

    def __call__(self, target, source, env):
        """Both target and source should be a single file"""
        if len(target) != 1:
            raise StopError("unexpected number of targets for unitTest: " + str(target))
        if len(source) != 1:
            raise StopError("unexpected number of sources for unitTest: " + str(source))

        out = str(target[0])
        exe = str(source[0])

        try:

            cmd = exe + ' > ' + out + ' 2>&1'
            ret = os.system(cmd)

            if ret != 0:
                shutil.move(out, out + '.failed')
                msg = '*** Unit test failed, check log file ' + out + '.failed ***'
                sep = '*' * len(msg)
                print sep + '\n' + msg + '\n' + sep
                # save failed target in UNIT_TESTS_FAILED list in env,
                # to be analyzed by UnitTestCheck
                env.Append(UNIT_TESTS_FAILED=source)

        except:
            # exception means we could not even run it
            print 'Failure running unit test ' + out
            env.Append(UNIT_TESTS_FAILED=source)

    def strfunction(self, target, source, env):
        return "UnitTest: " + str(source[0])


class _unitTestCheck(object):
    """
    Class which implements builder action for UnitTestCheck.
    """

    def __call__(self, target, source, env):
        # SCons works better when there is an actual file produced as the result
        # of builder call, so we just create an empty file if the builder succeeds
        # or remove that file if it fails.
        fpath = str(target[0])

        # all failures are recorded in UNIT_TESTS_FAILED list in the environment
        failures = env.Flatten(env.get('UNIT_TESTS_FAILED', []))
        if failures:
            print "Following UnitTest failed: %s" % ' '.join(str(x) for x in failures)
            try:
                os.unlink(fpath)
            except:
                pass
            return "%d unit test(s) failed" % len(failures)
        else:
            open(fpath, "w")

    def strfunction(self, target, source, env):
        pass


def generate(env):
    """Add Builders and construction variables for running unit tests."""

    try:
        # it may be defined already
        builder = env['BUILDERS']['UnitTest']
    except KeyError:
        env['BUILDERS']['UnitTest'] = Builder(action=_unitTest(), suffix='.utest')
        env['BUILDERS']['UnitTestCheck'] = Builder(action=_unitTestCheck())
        env['UNIT_TESTS_FAILED'] = []

def exists(env):
    return True
