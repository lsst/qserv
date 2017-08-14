from __future__ import print_function
hooks.config.Eups.colorize = True
hooks.config.Eups.userTags += ["git"]

# For dev user, qserv repository, declared with 'git' tag
# in change-uid.sh , will be setup by default


def cmdHook(Eups, cmd, opts, args):
    if Eups and cmd == "setup":
        if not opts.tag:
            opts.tag = ["git", "qserv-dev"]

            if opts.verbose >= 0:
                from . import utils
                msg = "Using default tags: {0} to setup {1}" \
                    .format(", ".join(opts.tag), ", ".join(args))
                print(msg, file=utils.stdinfo)


eups.commandCallbacks.add(cmdHook)
