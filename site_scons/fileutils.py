import os

import SCons.Node.FS

def recursive_glob(dir_path, pattern, env):
    files = []
    has_files = env.Glob(os.path.join(dir_path, '*'))
    if has_files:
        files += env.Glob(os.path.join(dir_path, pattern))
        # Analyzing sub-directories
        files += recursive_glob(os.path.join(dir_path, "*"), pattern, env)
    return files

def replace_base_path(path_to_remove, path_to_add, scons_fs_node, env):
    """ strip path_to_remove out of inode_name in order to have the filepath relative to path_to_remove
        throw exception if scons_fs_node doesn't start with path_to_remove or
        isn't an object of type SCons.Node.FS.Dir or SCons.Node.FS.File

        if path_to_remove=None, only basename of scons_fs_node is kept
    """
    if not isinstance(scons_fs_node, (SCons.Node.FS.File, SCons.Node.FS.Dir)):
        raise Exception("replace_base_path() input error : %s must be of "
                        "type SCons.Node.FS.File or SCons.Node.FS.Dir"
                        % scons_fs_node)

    source_node_name = str(scons_fs_node)

    if path_to_remove is not None:
        path_to_remove = os.path.normpath(path_to_remove)
    else:
        path_to_remove = os.path.dirname(source_node_name)

    if source_node_name.find(path_to_remove) != 0:
        raise Exception("replace_base_path() input error : '%s' has to start with '%s'" %
                        (source_node_name, path_to_remove))
    else:
        index = len(path_to_remove + os.sep)
        target_node_name = os.path.join(path_to_add, source_node_name[index:])
        if isinstance(scons_fs_node, SCons.Node.FS.File):
            return env.File(target_node_name)
        elif isinstance(scons_fs_node, SCons.Node.FS.Dir):
            return env.Dir(target_node_name)
