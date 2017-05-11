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
    - name: master
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
  nodeSelector:
    kubernetes.io/hostname: <INI_HOST>
