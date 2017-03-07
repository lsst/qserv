apiVersion: v1
kind: Pod
metadata:
  name: <POD_NAME>
  labels:
    app: qserv
spec:
  hostNetwork: true
  subdomain: qserv
  containers:
    - name: master
      image: "<MASTER_IMAGE>"
      env:
        - name: QSERV_MASTER
          value: "<MASTER_HOSTNAME>"
    # command: ["tail","-f", "/dev/null"]
      securityContext:
        capabilities:
          add:
          - IPC_LOCK
  nodeSelector:
    kubernetes.io/hostname: <HOST>
