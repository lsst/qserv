import logging
import os
import time

import commons 

from SCons.Script import Execute, Mkdir, Chmod, Copy, WhereIs   # for Execute and Mkdir
import SCons.Node.FS

def exists_and_is_writable(dir) :
    """
    Test if a dir exists. If no creates it, if yes checks if it is writeable.
    Return True if a writeable directory exists at the end of function execution, else False
    """
    logger = logging.getLogger()
    logger.debug("Checking existence and write access for : %s", dir)
    if not os.path.exists(dir):
    	if Execute(Mkdir(dir)):
            logger.debug("Unable to create dir : %s " % dir)
            return False 
    elif not commons.is_writable(dir):
        return False
    
    return True	 

def recursive_glob(dir_path,pattern,env):
    files = []
    has_files = env.Glob(os.path.join(dir_path,'*'))
    if has_files:
        files += env.Glob(os.path.join(dir_path,pattern))
        # Analyzing sub-directories
        files += recursive_glob(os.path.join(dir_path,"*"),pattern,env)
    return files

def replace_base_path(path_to_remove, path_to_add, scons_fs_node,env):
    """ strip path_to_remove out of inode_name in order to have the filepath relative to path_to_remove 
        throw exception if scons_fs_node doesn't start with path_to_remove or
        isn't an object of type SCons.Node.FS.Dir or SCons.Node.FS.File
    """
    if not (isinstance(scons_fs_node,SCons.Node.FS.File) or
        isinstance(scons_fs_node,SCons.Node.FS.Dir)) :
        raise Exception("replace_base_path() input error : %s must be of "
                        "type SCons.Node.FS.File or SCons.Node.FS.Dir" 
                        % scons_fs_node)
    
    source_node_name=str(scons_fs_node)
    path_to_remove = os.path.normpath(path_to_remove)

    if source_node_name.find(path_to_remove)!=0:
        raise Exception("replace_base_path() input error : '%s' has to start with"
                        "'%s'" % path_to_remove)
    else:
        target_node_name = source_node_name.replace(path_to_remove,path_to_add)
        if isinstance(scons_fs_node,SCons.Node.FS.File):
            return env.File(target_node_name) 
        elif isinstance(scons_fs_node,SCons.Node.FS.Dir):
            return env.Dir(target_node_name) 

