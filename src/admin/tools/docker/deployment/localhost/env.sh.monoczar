# Rename this file to env.sh and edit variables
# Configuration file sourced by other scripts from the directory

# VERSION can be a git ticket branch but with _ instead of /
# example: u_fjammes_DM-4295
VERSION=dev

NB_WORKERS=3

# Set nodes names
DNS_DOMAIN=localdomain
MASTER=master."$DNS_DOMAIN"

for i in $(seq 1 "$NB_WORKERS");
do
    WORKERS="$WORKERS worker${i}.$DNS_DOMAIN"
done

# Set images names
MASTER_IMAGE="qserv/qserv:${VERSION}_master"
WORKER_IMAGE="qserv/qserv:${VERSION}_worker"


