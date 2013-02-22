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
        index = len(path_to_remove+os.sep)
        target_node_name = os.path.join(path_to_add,source_node_name[index:])
        if isinstance(scons_fs_node,SCons.Node.FS.File):
            return env.File(target_node_name) 
        elif isinstance(scons_fs_node,SCons.Node.FS.Dir):
            return env.Dir(target_node_name) 


def get_top_dirs(targets,env):
    """ This function returns the lowest level directories list which contains
        all files given in the input
        targets : a list of files
    """
    existing_targets = [target for target in targets if env.File(target).exists()]
    
    if len(existing_targets)==0 :
        return []

    # duplicate removal
    dirs = list(set([os.path.dirname(str(target)) for target in existing_targets]))
    
    # shortest directory paths should be parents
    dirs.sort(key=len)

    print("INPUT %s" % dirs)
    top_dirs = rec_get_top_dirs(dirs[0], dirs[1:], [])
    print("PARENTS %s" % top_dirs)

    return top_dirs 

def rec_get_top_dirs(current_top_dir, dirs, top_dirs):
    """ current_top_dir : a non empty list
        dirs            : list of remaining directories which will be processes
        top_dirs        : an accumulator containing the final result 
        this function should be call by get_top_dirs() which pre-process input
        data 
    """
    top_dirs.append(current_top_dir)

    not_sub_dirs = [d for d in dirs if not d.startswith(current_top_dir)]

    if len(not_sub_dirs)==0:
        return top_dirs 
    else:    
        return rec_get_top_dirs(not_sub_dirs[0],not_sub_dirs_dirs[1:],top_dirs.append(d))

