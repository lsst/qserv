# Wait for all swarm nodes to be in "Healthy" status
# Swarm 'info' interface is weak
PENDING="TRUE"
while [ -n "$PENDING" ]
do
    STATUS=$(docker node ls)
    if [ -n "$STATUS" ]; then
        PENDING=$(echo "$STATUS" | grep "Pending" || true)
        echo "Waiting for all swarm node to reach 'Healthy' status"
        sleep 1
    fi
done

echo "Docker Swarm cluster ready"
