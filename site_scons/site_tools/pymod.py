import distutils.sysconfig
import logging
import os

from SCons.Script import Delete

import utils

def install_python_module(env, target, source):
    """ Define targets which will install all python file contained in 
        source_dir_path and sub-directories in python_path_prefix.
    """  
    log = logging.getLogger()

    python_path_prefix=target
    source_dir_path=source 
    env['pythonpath'] =distutils.sysconfig.get_python_lib(prefix=python_path_prefix)
    target_lst = []
    clean_target_lst = []

    source_lst = utils.recursive_glob(source_dir_path,'*.py',env)

    for f in source_lst :
        target = utils.replace_base_path(source_dir_path,env['pythonpath'],f,env)
        env.InstallAs(target, f)
        target_lst.append(target)
        # .pyc files will also be removed
        env.Clean(target, "%s%s" % (target ,"c"))

    # TODO : here AddPostAction to walk in env['pythonpath'] and remove all empty dirs
    env.CleanAction( target_lst, clean_python_path_dir)

    return target_lst

def clean_python_path_dir(target_lst, env):
    """ Delete empty directories in PYTHONPATH
    """
    for target in target_lst:
        print "TOTO tgt %s " % target
        # .pyc files will also be removed
        d = os.path.dirname(str(target))
        if os.path.exists(d) and os.listdir(d) == []: 
            print "TOTO %s" % target
            env.AddPostAction(target, env.Execute(Delete(d)))
    
    return ""

def generate(env):
    env.AddMethod(install_python_module,'InstallPythonModule')

def exists(env):
    return env.Detect('python')
