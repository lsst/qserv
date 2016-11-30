apiVersion: v1
kind: Pod
metadata:
  name: worker-<NODE_ID>
  labels: 
    app: qserv 
spec:
  hostname: worker-<NODE_ID>
  subdomain: qserv
  containers:
  - name: worker
    image: "<WORKER_IMAGE>"
    env:
    - name: QSERV_MASTER
      value: "master.qserv.default.svc.cluster.local"
    # Uncomment line below for debugging purpose
    # command: ["tail","-f", "/dev/null"]
  nodeSelector:
    kubernetes.io/hostname: <HOST>
