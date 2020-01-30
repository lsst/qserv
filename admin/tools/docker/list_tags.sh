wget -q https://registry.hub.docker.com/v1/repositories/qserv/qserv/tags -O -  | sed -e 's/[][]//g' -e 's/"//g' -e 's/ //g' | tr '}' '\n'  | awk -F: '{print $3}' | grep 2019
