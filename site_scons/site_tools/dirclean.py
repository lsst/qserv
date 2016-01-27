
import collections
import os
import state
from SCons.Script import Dir, Entry, Split

def _all_children(nodes, children):
    for node in nodes:
        for child in node.children(scan=0):
            if child not in children:
                state.log.debug("_all_children() : add child: %s" % child)
                children.add(child)
                _all_children([child], children)

def all_children(nodes):
    """
    Returns all children of specified nodes, recursively.
    """
    children = set()
    _all_children(nodes, children)
    return children


def CleanEmptyDirs(env, targets, dir_list):
    """
    Recursively remove directories from specified directory list.

    Only removes directories which would become empty after cleanup
    was done for specified target list.
    """
    targets = env.arg2nodes(targets, Entry)
    state.log.debug("CleanEmptyDirs() : targets: %r" % map(str, targets))
    state.log.debug("CleanEmptyDirs() : dir_list: %r" % dir_list)

    nodes = all_children(targets)
    dir2nodes = collections.defaultdict(lambda: set())
    for node in nodes:
        try:
            path = node.abspath
        except AttributeError:
            # likely an Alias
            continue
        state.log.debug("CleanEmptyDirs() : node abspath: %s" % path)
        if not os.path.isdir(path):
            dirname, basename = os.path.split(path)
        dir2nodes[dirname].add(basename)

    empty_dirs = set()
    for dir in Split(dir_list):
        state.log.debug("CleanEmptyDirs() : checking dir: %s" % dir)
        for dirpath, dirnames, filenames in os.walk(Dir(dir).abspath, topdown=False):
            filenames = set(filenames) - dir2nodes[dirpath]
            dirnames = set(os.path.join(dirpath, d) for d in dirnames)
            dirnames = dirnames - empty_dirs
            if not filenames and not dirnames:
                state.log.debug("CleanEmptyDirs() : empty dir: %s" % dirpath)
                empty_dirs.add(dirpath)
            else:
                state.log.debug("CleanEmptyDirs() : dir: %s files: %s subdirs: %s" %
                                (dirpath, filenames, dirnames))

    env.Clean(targets, list(empty_dirs))

def generate(env):
    env.AddMethod(CleanEmptyDirs)

def exists(env):
    return True
