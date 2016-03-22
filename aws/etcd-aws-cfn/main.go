package main

import (
	"log"

	etcdaws "github.com/crewjam/etcd-aws/aws"
)

func main() {
	if err := etcdaws.Main(); err != nil {
		log.Fatalf("ERROR: %s", err)
	}
}
