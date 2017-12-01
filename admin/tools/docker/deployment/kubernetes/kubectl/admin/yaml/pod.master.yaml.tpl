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
    - name: master
      image: "<INI_IMAGE>"
      imagePullPolicy: Always
    # command: ["tail","-f", "/dev/null"]
      command: [<RESOURCE_START_MASTER>]
      securityContext:
        capabilities:
          add:
          - IPC_LOCK
      volumeMounts:
      - name: config-master
        mountPath: /config
  nodeSelector:
    kubernetes.io/hostname: <INI_HOST>
  volumes:
    - name: config-master
      configMap:
        name: config-master
  volumes:
    - name: config-sql
      configMap:
        name: config-sql
  restartPolicy: Never
