FROM scratch
ADD cacert.pem /etc/ssl/ca-bundle.pem
ADD dist/etcd-aws.Linux.x86_64 /etcd-aws
ENV PATH=/
CMD ["/etcd-aws"]