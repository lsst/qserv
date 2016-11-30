apiVersion: v1
kind: Pod
metadata:
  name: master
  labels: 
    app: qserv
spec:
  hostname: master
  subdomain: qserv
  containers:
  - name: master
    image: "<MASTER_IMAGE>"
    env:
    - name: QSERV_MASTER 
      value: "master.qserv.default.svc.cluster.local"
    # command: ["tail","-f", "/dev/null"]
  nodeSelector:
    kubernetes.io/hostname: <HOST> 
