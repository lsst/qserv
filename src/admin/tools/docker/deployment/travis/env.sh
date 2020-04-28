# Rename this file to env.sh and edit variables
# Configuration file sourced by other scripts from the directory

IMAGE=$IMAGE

NB_WORKERS=3

# Set nodes names
DNS_DOMAIN=localdomain
MASTER=master."$DNS_DOMAIN"

for i in $(seq 1 "$NB_WORKERS");
do
    WORKERS="${WORKERS} worker${i}.${DNS_DOMAIN}"
done

# Set images names
MASTER_IMAGE="${IMAGE}_master"
WORKER_IMAGE="${IMAGE}_worker"
