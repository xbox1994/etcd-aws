package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/crewjam/awsregion"
	"github.com/crewjam/ec2cluster"
)

type etcdState struct {
	LeaderInfo etcdLeaderInfo `json:"leaderInfo"`
}

type etcdLeaderInfo struct {
	Leader string `json:"leader"`
}

type etcdMembers struct {
	Members []etcdMember `json:"members,omitempty"`
}

type etcdMember struct {
	ID         string   `json:"id,omitempty"`
	Name       string   `json:"name,omitempty"`
	PeerURLs   []string `json:"peerURLs,omitempty"`
	ClientURLs []string `json:"clientURLs,omitempty"`
}

var etcdLocalURL string

// handleEvent is invoked whenever we get a lifecycle terminate message. It removes
// terminated instances from the etcd cluster.
func handleEvent(m *ec2cluster.LifecycleMessage) (shouldContinue bool, err error) {
	if m.LifecycleTransition != "autoscaling:EC2_INSTANCE_TERMINATING" {
		return true, nil
	}

	// look for the instance in the cluster
	resp, err := http.Get(fmt.Sprintf("%s/v2/members", etcdLocalURL))
	if err != nil {
		return false, err
	}
	members := etcdMembers{}
	if err := json.NewDecoder(resp.Body).Decode(&members); err != nil {
		return false, err
	}
	memberID := ""
	for _, member := range members.Members {
		if member.Name == m.EC2InstanceID {
			memberID = member.ID
		}
	}

	if memberID == "" {
		log.Printf("instance:%s termination event for non-member", m.EC2InstanceID)
		return true, nil
	}

	log.Printf("member:%s instance:%s removing from the cluster", memberID, m.EC2InstanceID)
	req, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/v2/members/%s", etcdLocalURL, memberID), nil)
	_, err = http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}

	return false, nil
}

func main() {
	instanceID := flag.String("instance", "",
		"The instance ID of the cluster member. If not supplied, then the instance ID is determined from EC2 metadata")
	clusterTagName := flag.String("tag", "aws:autoscaling:groupName",
		"The instance tag that is common to all members of the cluster")
	doDeathWatch := flag.Bool("watch", false,
		"Watch for autoscaling events and perform remove cluster nodes as needed")
	flag.Parse()

	var err error
	*instanceID, err = ec2cluster.DiscoverInstanceID()
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}

	awsSession := session.New()
	if region := os.Getenv("AWS_REGION"); region != "" {
		awsSession.Config.WithRegion(region)
	}
	awsregion.GuessRegion(awsSession.Config)

	s := &ec2cluster.Cluster{
		AwsSession: awsSession,
		InstanceID: *instanceID,
		TagName:    *clusterTagName,
	}

	localInstance, err := s.Instance()
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}

	if *doDeathWatch {
		etcdLocalURL = fmt.Sprintf("http://%s:2379", *localInstance.PrivateIpAddress)
		if err := s.WatchLifecycleEvents(handleEvent); err != nil {
			log.Fatalf("ERROR: %s", err)
		}
		return
	}

	clusterInstances, err := s.Members()
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}

	initialClusterState := "new"
	initialCluster := []string{}
	for _, instance := range clusterInstances {
		if instance.PrivateIpAddress == nil {
			continue
		}

		// add this instance to the initialCluster expression
		initialCluster = append(initialCluster, fmt.Sprintf("%s=http://%s:2380",
			*instance.InstanceId, *instance.PrivateIpAddress))

		// skip the local node, since we know it is not running yet
		if *instance.InstanceId == *localInstance.InstanceId {
			continue
		}

		// fetch the state of the node.
		resp, err := http.Get(fmt.Sprintf("http://%s:2379/v2/stats/self", *instance.PrivateIpAddress))
		if err != nil {
			log.Printf("%s: http://%s:2379/v2/stats/self: %s", *instance.InstanceId,
				*instance.PrivateIpAddress, err)
			continue
		}
		nodeState := etcdState{}
		if err := json.NewDecoder(resp.Body).Decode(&nodeState); err != nil {
			log.Printf("%s: http://%s:2379/v2/stats/self: %s", *instance.InstanceId,
				*instance.PrivateIpAddress, err)
			continue
		}

		if nodeState.LeaderInfo.Leader == "" {
			log.Printf("%s: http://%s:2379/v2/stats/self: alive, no leader", *instance.InstanceId,
				*instance.PrivateIpAddress)
			continue
		}

		log.Printf("%s: http://%s:2379/v2/stats/self: has leader %s", *instance.InstanceId,
			*instance.PrivateIpAddress, nodeState.LeaderInfo.Leader)
		if initialClusterState != "existing" {
			initialClusterState = "existing"

			// inform the know we found about the new node we're about to add so that
			// when etcd starts we can avoid etcd thinking the cluster is out of sync.
			log.Printf("joining cluster via %s", *instance.InstanceId)
			m := etcdMember{
				Name:     *localInstance.InstanceId,
				PeerURLs: []string{fmt.Sprintf("http://%s:2380", *localInstance.PrivateIpAddress)},
			}
			body, _ := json.Marshal(m)
			http.Post(fmt.Sprintf("http://%s:2379/v2/members", *instance.PrivateIpAddress),
				"application/json", bytes.NewReader(body))
		}
	}

	fmt.Printf("ETCD_NAME=%s\n", *localInstance.InstanceId)
	fmt.Printf("ETCD_INITIAL_CLUSTER=%s\n", strings.Join(initialCluster, ","))
	fmt.Printf("ETCD_INITIAL_CLUSTER_STATE=%s\n", initialClusterState)

	asg, err := s.AutoscalingGroup()
	if asg != nil {
		fmt.Printf("ETCD_INITIAL_CLUSTER_TOKEN=%s\n", *asg.AutoScalingGroupARN)
	}
}
