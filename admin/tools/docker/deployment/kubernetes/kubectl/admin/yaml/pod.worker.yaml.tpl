apiVersion: v1
kind: Pod
metadata:
  name: <INI_POD_NAME>
  labels:
    app: qserv
spec:
  hostNetwork: true
  subdomain: qserv
  containers:
    - name: mariadb
      image: "<INI_IMAGE>"
      imagePullPolicy: Always
    # command: ["tail","-f", "/dev/null"]
      command: [<RESOURCE_START_MARIADB>]
      volumeMounts:
      - name: config-my-dot-cnf
        mountPath: /config-mariadb
      - name: config-mariadb-start
        mountPath: /config-start
    - name: worker
      image: "<INI_IMAGE>"
      imagePullPolicy: Always
      command: [<RESOURCE_START_WORKER>]
      securityContext:
        capabilities:
          add:
          - IPC_LOCK
      volumeMounts:
      - name: config-worker-start
        mountPath: /config-start
  nodeSelector:
    kubernetes.io/hostname: <INI_HOST>
  volumes:
    - name: config-mariadb-configure
      configMap:
        name: config-mariadb-configure
    - name: config-mariadb-start
      configMap:
        name: config-mariadb-start
    - name: config-my-dot-cnf
      configMap:
        name: config-my-dot-cnf
    - name: config-qserv-configure
      configMap:
        name: config-qserv-configure
    - name: config-worker-sql
      configMap:
        name: config-worker-sql
    - name: config-worker-start
      configMap:
        name: config-worker-start
  restartPolicy: Never
