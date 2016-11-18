# Blocking issue

Swarm changes container inner IP/hostname at container restart and this confuse
xrootd and more precisely its cmsd cache, which use ip adress to track avaible
resources.

# xrootd setup

Following directive provide a partial fix to above issue

In lsp.cf master section add:

    # Immediately kill the cache and forget about the
    # host/ip address the moment is disconnects
    # Required by docker-swarm which changes
    # hostname/ip address when restarting a swarm service
    cms.delay drop 1

and in lsp.cf general section add:

    # Required for docker-swarm
    xrd.network cache 0


