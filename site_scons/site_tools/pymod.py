import distutils.sysconfig
import shutil

import utils

def install_python_module(env, target,source):
    """ Define targets which will install all python file contained in 
        source_dir_path and sub-directories in python_path_prefix.
    """  

    print "DEBUG"

    python_path_prefix=target
    source_dir_path=source 
    target_dir_path=distutils.sysconfig.get_python_lib(prefix=python_path_prefix)
    target_lst = []

    for f in utils.recursive_glob(source_dir_path,'*.py',env) :
        target = utils.replace_base_path(source_dir_path,target_dir_path,f,env)
        env.InstallAs(target, f)
        target_lst.append(target)

    env.CleanAction( target_lst, clean_python_path)

    return target_lst

def clean_python_path(targets,env):
    """ targets is a list of installed python files in PYTHONPATH 
        remove the highest directory of installed python modules assuming it
        contain and __init__.py file 
    """

    dirs = utils.get_top_dirs(targets,env)

    print("Removing next directories : %s" % dirs)
    for d in dirs:
        shutil.rmtree(d)
    
    return ""

def clean_action(env, targets, action):

    test = env.GetOption('clean') 
    if env.GetOption('clean'):
        
        env.Execute(action(targets,env))

def generate(env):
    env.AddMethod(clean_action, 'CleanAction')
    env.AddMethod(install_python_module,'InstallPythonModule')

def exists(env):
    return env.Detect('python')
