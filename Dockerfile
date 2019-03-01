FROM alpine:latest
ADD dist/cacert.pem /etc/ssl/ca-bundle.pem
ADD dist/etcd3.Linux.x86_64 /bin/etcd3
ADD dist/etcdctl3.Linux.x86_64 /bin/etcdctl3
ADD dist/etcd-aws.Linux.x86_64 /bin/etcd-aws
ENV PATH=/bin
ENV TMPDIR=/
CMD ["/bin/etcd-aws"]
