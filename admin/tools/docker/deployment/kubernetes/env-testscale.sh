# Override https://github.com/lsst/qserv_testscale/blob/master/S15/tests/env.sh 
# designed for shmux-installed Qserv cluster

# @author Fabrice Jammes SLAC/IN2P3

if [ -z "$MASTER" ]; then
    echo "ERROR: undefined \$MASTER"
    exit 1
fi

# Parallel invokes the shell indicated by the SHELL environment variable
export SHELL=$(type -p bash)

function mysql_query {
    sql="$1"
    sleep_delay="$2"

    SSH_CFG="$HOME/.lsst/qserv-cluster/ssh_config"
    user="qsmaster"
    db="LSST"
    port=4040

    echo "Query: $sql"
    echo "Date: $(date)"
    ssh $SSH_CFG_OPT "$MASTER" "kubectl exec 'master' -- bash -c '. /qserv/stack/loadLSST.bash && \
        setup mariadb && \
        start=\$(date +%s.%N) && \
        mysql -N -B --host \"127.0.0.1\" --port $port \
        --user=$user $db -e \"$sql\" && \
        end=\$(date +%s.%N) &&
        echo \"Execution time: \$(python -c \"print(\${end} - \${start})\")\"'"
	echo "Date: $(date)"

    runtime=$((end-start))

    echo
    if [ -n "$sleep_delay" ]; then
        sleep "$sleep_delay"
    fi
}

