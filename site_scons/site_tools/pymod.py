import fileutils
import os
import state
import subprocess
from SCons.Script import Action


def compile_python_module(env, sources, dst_dir=None):
    """
    Makes *.pyc files from *.py files.

    Sources is a list of Python targets (files). If dst_dir is None
    then compiled files are placed in the same directory as source,
    otherwise copiled files will be saved to that directory.
    """

    action = "python -c 'import py_compile; py_compile.compile(\"$SOURCE\", \"$TARGET\", doraise=True)'"
    if not state.log.verbose:
        action = Action(action, cmdstr="Compiling Python module $TARGET")

    targets = []
    for src in sources:
        src_path = str(src)
        if dst_dir is None:
            dst_path = src_path + 'c'
        else:
            dst_path = os.path.join(str(dst_dir), os.path.basename(str(src_path)) + 'c')
        env.Command(dst_path, src_path, action)
        targets.append(env.File(dst_path))

    return targets


def install_python_module(env, target, source):
    """ Define targets which will install all python file contained in
        source_dir_path and sub-directories in python_path_prefix.
    """
    python_path_prefix = target
    source_dir_path = source
    target_lst = []

    source_lst = fileutils.recursive_glob(source_dir_path, '*.py', env)

    for f in source_lst:
        target = fileutils.replace_base_path(source_dir_path, python_path_prefix, f, env)
        state.log.debug("install_python_module() : source %s, target %s" % (f, target))
        env.InstallAs(target, f)
        target_lst.append(target)

    target_lst += compile_python_module(env, target_lst)

    return target_lst


def generate(env):
    env.AddMethod(install_python_module, 'InstallPythonModule')
    env.AddMethod(compile_python_module, 'PyCompile')

    # define special variable for location of Python headers
    command = ['python', '-c', 'from distutils import sysconfig; print(sysconfig.get_python_inc())']
    output = subprocess.check_output(command).decode().strip()
    env['PYTHON_INC_DIR'] = output


def exists(env):
    return env.Detect('python')
