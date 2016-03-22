
# etcd-aws

This repository contains tools for building a robust etcd cluster in AWS.

 

 TODO: 
  - move stuff out of Parameters into cfn params

tl;dr:

   etcd-aws --tag=aws:autoscaling:groupName --backup-bucket=mybackups --backup-key=etcd-backup.gz --data-dir=/var/lib/etcd2

