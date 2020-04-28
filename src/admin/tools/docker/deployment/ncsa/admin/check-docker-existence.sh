
MASTER=qserv-master01
#WORKERS=qserv-db{01..30}
for node in $MASTER qserv-db{01..30}
do
    time ssh -t "$node" "docker info > /dev/null"
done
