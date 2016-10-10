set -x

DIR="/qserv/log"
#DIR="/qserv/data"

for node in qserv-db{01..30}
do
    ssh -t "$node" "sudo sh -c \"sed -i -e 's/Defaults    requiretty.*/#Defaultsrequiretty/g' \
		/etc/sudoers\""
done
