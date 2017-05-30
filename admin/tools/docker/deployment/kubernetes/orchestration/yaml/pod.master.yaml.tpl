apiVersion: v1
kind: Pod
metadata:
  name: <INI_POD_NAME>
  labels:
    app: qserv
spec:
  initContainers:
    - name: init-qserv-run-dir
      image: "<INI_IMAGE>" 
      command: ['sh', '-c', 'cp -rp /qserv/run/* /qserv-run-dir']
      volumeMounts:
      - mountPath: /qserv-run-dir
        name: qserv-run-dir-volume
  hostNetwork: true
  subdomain: qserv
  containers:
    - name: <INI_POD_NAME> 
      image: "<INI_IMAGE>"
      imagePullPolicy: Always
      env:
        - name: QSERV_MASTER
          value: "<INI_MASTER_HOSTNAME>"
    # command: ["tail","-f", "/dev/null"]
      securityContext:
        capabilities:
          add:
          - IPC_LOCK
      volumeMounts:
      - mountPath: /qserv/run
        name: qserv-run-dir-volume
  nodeSelector:
    kubernetes.io/hostname: <INI_HOST>
  volumes:
  - name: qserv-run-dir-volume
    emptyDir: {}
