package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/coreos/go-etcd/etcd"
	"github.com/crewjam/awsregion"
	"github.com/crewjam/ec2cluster"
)

type etcdState struct {
	Name       string         `json:"name"`
	ID         string         `json:"id"`
	State      string         `json:"state"`
	StartTime  time.Time      `json:"startTime"`
	LeaderInfo etcdLeaderInfo `json:"leaderInfo"`
}

type etcdLeaderInfo struct {
	Leader               string    `json:"leader"`
	Uptime               string    `json:"uptime"`
	StartTime            time.Time `json:"startTime"`
	RecvAppendRequestCnt int       `json:"recvAppendRequestCnt"`
	RecvPkgRate          int       `json:"recvPkgRate"`
	RecvBandwidthRate    int       `json:"recvBandwidthRate"`
	SendAppendRequestCnt int       `json:"sendAppendRequestCnt"`
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

// handleLifecycleEvent is invoked whenever we get a lifecycle terminate message. It removes
// terminated instances from the etcd cluster.
func handleLifecycleEvent(m *ec2cluster.LifecycleMessage) (shouldContinue bool, err error) {
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
		log.WithField("InstanceID", m.EC2InstanceID).Warn("received termination event for non-member")
		return true, nil
	}

	log.WithFields(log.Fields{
		"InstanceID": m.EC2InstanceID,
		"MemberID":   memberID}).Info("removing from cluster")
	req, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/v2/members/%s", etcdLocalURL, memberID), nil)
	_, err = http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}

	return false, nil
}

// backupService invokes backupOnce() periodically if the current node is the cluster leader.
func backupService(s *ec2cluster.Cluster, backupBucket, backupKey, dataDir string, interval time.Duration) error {
	instance, err := s.Instance()
	if err != nil {
		return err
	}

	ticker := time.Tick(interval)
	for {
		<-ticker

		resp, err := http.Get(fmt.Sprintf("http://%s:2379/v2/stats/self", *instance.PrivateIpAddress))
		if err != nil {
			return fmt.Errorf("%s: http://%s:2379/v2/stats/self: %s", *instance.InstanceId,
				*instance.PrivateIpAddress, err)
		}

		nodeState := etcdState{}
		if err := json.NewDecoder(resp.Body).Decode(&nodeState); err != nil {
			return fmt.Errorf("%s: http://%s:2379/v2/stats/self: %s", *instance.InstanceId,
				*instance.PrivateIpAddress, err)
		}

		// if the cluster has a leader other than the current node, then don't do the
		// backup.
		if nodeState.LeaderInfo.Leader != "" && nodeState.ID != nodeState.LeaderInfo.Leader {
			log.Printf("backup: %s: http://%s:2379/v2/stats/self: not the leader", *instance.InstanceId,
				*instance.PrivateIpAddress)
			<-ticker
			continue
		}

		if err := backupOnce(s, backupBucket, backupKey, dataDir); err != nil {
			return err
		}
	}
	panic("not reached")
}

// getInstanceTag returns the first occurrence of the specified tag on the instance
// or an empty string if the tag is not found.
func getInstanceTag(instance *ec2.Instance, tagName string) string {
	rv := ""
	for _, tag := range instance.Tags {
		if *tag.Key == tagName {
			rv = *tag.Value
		}
	}
	return rv
}

// dumpEtcdNode writes a JSON representation of the nodes and and below `key`
// to `w`. Returns the number of nodes traversed.
func dumpEtcdNode(key string, etcdClient *etcd.Client, w io.Writer) (int, error) {
	response, err := etcdClient.Get(key, false, false)
	if err != nil {
		return 0, err
	}

	childNodes := response.Node.Nodes
	response.Node.Nodes = nil
	if err := json.NewEncoder(w).Encode(response.Node); err != nil {
		return 0, err
	}
	count := 1

	// enumerate all the child nodes. If a child node is
	// a directory, then it must be backed up recursively.
	// Otherwise it can just be written
	for _, childNode := range childNodes {
		if childNode.Dir {
			c, err := dumpEtcdNode(childNode.Key, etcdClient, w)
			if err != nil {
				return 0, err
			}
			count += c
		} else {
			if err := json.NewEncoder(w).Encode(childNode); err != nil {
				return 0, err
			}
			count += 1
		}
	}

	return count, nil
}

// backupOnce dumps all the nodes in the etcd cluster to the specified S3 bucket. On
// success it emits a CloudWatch metric for the number of keys backed up. The absence
// of data on this metric indicates the backup has failed.
func backupOnce(s *ec2cluster.Cluster, backupBucket, backupKey, dataDir string) error {
	instance, err := s.Instance()
	if err != nil {
		return err
	}
	etcdClient := etcd.NewClient([]string{fmt.Sprintf("http://%s:2379", *instance.PrivateIpAddress)})
	if success := etcdClient.SyncCluster(); !success {
		return fmt.Errorf("backupOnce: cannot sync machines")
	}

	var valueCount int
	bodyReader, bodyWriter := io.Pipe()
	go func() {
		gzWriter := gzip.NewWriter(bodyWriter)

		var err error
		valueCount, err = dumpEtcdNode("/", etcdClient, gzWriter)
		if err != nil {
			gzWriter.Close()
			bodyWriter.CloseWithError(err)
			return
		}

		gzWriter.Close()
		bodyWriter.Close()
	}()

	s3mgr := s3manager.NewUploader(s.AwsSession)
	_, err = s3mgr.Upload(&s3manager.UploadInput{
		Key:                  aws.String(backupKey),
		Bucket:               aws.String(backupBucket),
		Body:                 bodyReader,
		ServerSideEncryption: aws.String(s3.ServerSideEncryptionAes256),
		ACL:                  aws.String(s3.ObjectCannedACLPrivate),
	})
	if err != nil {
		return fmt.Errorf("upload s3://%s%s: %s", backupBucket, backupKey, err)
	}

	log.Printf("backup written to s3://%s%s (%d values)", backupBucket, backupKey,
		valueCount)

	cloudwatch.New(s.AwsSession).PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace: aws.String("Local/etcd"),
		MetricData: []*cloudwatch.MetricDatum{
			&cloudwatch.MetricDatum{
				MetricName: aws.String("BackupKeyCount"),
				Dimensions: []*cloudwatch.Dimension{
					&cloudwatch.Dimension{
						Name:  aws.String("AutoScalingGroupName"),
						Value: aws.String(getInstanceTag(instance, "aws:autoscaling:groupName")),
					},
				},
				Unit:  aws.String(cloudwatch.StandardUnitCount),
				Value: aws.Float64(float64(valueCount)),
			},
		},
	})

	return nil
}

// loadEtcdNode reads `r` containing JSON objects representing etcd nodes and
// loads them into server.
func loadEtcdNode(etcdClient *etcd.Client, r io.Reader) error {
	jsonReader := json.NewDecoder(r)
	for {
		node := etcd.Node{}
		if err := jsonReader.Decode(&node); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if node.Key == "" && node.Dir {
			continue // skip root
		}

		// compute a new TTL
		ttl := 0
		if node.Expiration != nil {
			ttl := node.Expiration.Sub(time.Now()).Seconds()
			if ttl < 0 {
				// expired, skip
				continue
			}
		}

		if node.Dir {
			_, err := etcdClient.SetDir(node.Key, uint64(ttl))
			if err != nil {
				return fmt.Errorf("%s: %s", node.Key, err)
			}
		} else {
			_, err := etcdClient.Set(node.Key, node.Value, uint64(ttl))
			if err != nil {
				return fmt.Errorf("%s: %s", node.Key, err)
			}
		}
	}
	return nil
}

// restoreBackup reads the backup from S3 and applies it to the current cluster.
func restoreBackup(s *ec2cluster.Cluster, backupBucket, backupKey, dataDir string) error {
	instance, err := s.Instance()
	if err != nil {
		return err
	}
	etcdClient := etcd.NewClient([]string{fmt.Sprintf("http://%s:2379", *instance.PrivateIpAddress)})
	if success := etcdClient.SyncCluster(); !success {
		return fmt.Errorf("restore: cannot sync machines")
	}

	s3svc := s3.New(s.AwsSession)
	resp, err := s3svc.GetObject(&s3.GetObjectInput{
		Key:    &backupKey,
		Bucket: &backupBucket,
	})
	if err != nil {
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			if reqErr.StatusCode() == http.StatusNotFound {
				log.Printf("restore: s3://%s%s does not exist", backupBucket, backupKey)
				return nil
			}
		}
		return fmt.Errorf("cannot fetch backup file: %s", err)
	}

	gzipReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return err
	}
	defer gzipReader.Close()

	if err := loadEtcdNode(etcdClient, gzipReader); err != nil {
		return err
	}
	log.Printf("restore: complete")
	return nil
}

func buildCluster(s *ec2cluster.Cluster) (initialClusterState string, initialCluster []string, error) {
	clusterInstances, err := s.Members()
	if err != nil {
		return "", nil, fmt.Errorf("list members: %s", err)
	}

	initialClusterState = "new"
	initialCluster = []string{}
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
	return initialClusterState, initialCluster, nil
}

func main() {
	instanceID := flag.String("instance", "",
		"The instance ID of the cluster member. If not supplied, then the instance ID is determined from EC2 metadata")
	clusterTagName := flag.String("tag", "aws:autoscaling:groupName",
		"The instance tag that is common to all members of the cluster")

	defaultBackupInterval := 5 * time.Minute
	backupInterval := flag.Duration("backup-interval", defaultBackupInterval,
		"How frequently to back up the etcd data to S3")
	backupBucket := flag.String("backup-bucket", os.Getenv("ETCD_BACKUP_BUCKET"),
		"The name of the S3 bucket where tha backup is stored. "+
			"Environment variable: ETCD_BACKUP_BUCKET")
	defaultBackupKey := "/etcd-backup.gz"
	if d := os.Getenv("ETCD_BACKUP_KEY"); d != "" {
		defaultBackupKey = d
	}
	backupKey := flag.String("backup-key", defaultBackupKey,
		"The name of the S3 key where tha backup is stored. "+
			"Environment variable: ETCD_BACKUP_KEY")

	defaultDataDir := "/var/lib/etcd2"
	if d := os.Getenv("ETCD_DATA_DIR"); d != "" {
		defaultDataDir = d
	}
	dataDir := flag.String("data-dir", defaultDataDir,
		"The path to the etcd2 data directory. "+
			"Environment variable: ETCD_DATA_DIR")
	flag.Parse()

	var err error
	if *instanceID == "" {
		*instanceID, err = ec2cluster.DiscoverInstanceID()
		if err != nil {
			log.Fatalf("ERROR: %s", err)
		}
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

	initialClusterState, initialCluster, err := buildCluster(s)

	// start the backup and restore goroutine.
	shouldTryRestore := false
	if initialClusterState == "new" {
		_, err := os.Stat(filepath.Join(*dataDir, "member"))
		if os.IsNotExist(err) {
			shouldTryRestore = true
		} else {
			log.Printf("%s: %s", filepath.Join(*dataDir, "member"), err)
		}
	}
	go func() {
		// wait for etcd to start
		for {
			etcdClient := etcd.NewClient([]string{fmt.Sprintf("http://%s:2379",
				*localInstance.PrivateIpAddress)})
			if success := etcdClient.SyncCluster(); success {
				break
			}
			time.Sleep(time.Second)
		}

		if shouldTryRestore {
			if err := restoreBackup(s, *backupBucket, *backupKey, *dataDir); err != nil {
				log.Fatalf("ERROR: %s", err)
			}
		}

		if err := backupService(s, *backupBucket, *backupKey, *dataDir, *backupInterval); err != nil {
			log.Fatalf("ERROR: %s", err)
		}
	}()

	// watch for lifecycle events and remove nodes from the cluster as they are
	// terminated.
	go func() {
		etcdLocalURL = fmt.Sprintf("http://%s:2379", *localInstance.PrivateIpAddress)
		for {
			err := s.WatchLifecycleEvents(handleLifecycleEvent)

			// The lifecycle hook might not exist yet if we're being created
			// by cloudformation.
			if err == ec2cluster.ErrLifecycleHookNotFound {
				log.Printf("WARNING: %s", err)
				time.Sleep(10 * time.Second)
				continue
			}
			if err != nil {
				log.Fatalf("ERROR: WatchLifecycleEvents: %s", err)
			}
			panic("not reached")
		}
	}()

	// Run the etcd command
	cmd := exec.Command("etcd")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = []string{
		fmt.Sprintf("ETCD_NAME=%s", *localInstance.InstanceId),
		fmt.Sprintf("ETCD_DATA_DIR=/var/lib/etcd2"),
		fmt.Sprintf("ETCD_ELECTION_TIMEOUT=1200"),
		fmt.Sprintf("ETCD_ADVERTISE_CLIENT_URLS=http://%s:2379", *localInstance.PrivateIpAddress),
		fmt.Sprintf("ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379"),
		fmt.Sprintf("ETCD_LISTEN_PEER_URLS=http://0.0.0.0:2380"),
		fmt.Sprintf("ETCD_INITIAL_CLUSTER_STATE=%s", initialClusterState),
		fmt.Sprintf("ETCD_INITIAL_CLUSTER=%s", strings.Join(initialCluster, ",")),
		fmt.Sprintf("ETCD_INITIAL_ADVERTISE_PEER_URLS=http://%s:2380", *localInstance.PrivateIpAddress),
	}
	asg, _ := s.AutoscalingGroup()
	if asg != nil {
		cmd.Env = append(cmd.Env, fmt.Sprintf("ETCD_INITIAL_CLUSTER_TOKEN=%s", *asg.AutoScalingGroupARN))
	}
	for _, env := range cmd.Env {
		log.Printf("%s", env)
	}
	if err := cmd.Run(); err != nil {
		log.Fatalf("%s", err)
	}
}
