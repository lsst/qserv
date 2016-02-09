# VERSION can be ticket branch but with _ instead of /
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
DOCKER_ORG=qserv
MASTER_IMAGE="$DOCKER_ORG/qserv:${VERSION}_master_$MASTER"
WORKER_IMAGE="$DOCKER_ORG/qserv:${VERSION}_worker_$MASTER"


