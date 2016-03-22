package aws

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"regexp"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/crewjam/awsregion"
	"github.com/crewjam/go-cloudformation/deploycfn"
)

func Main() error {
	application := flag.String("app", "myapp", "The name of the application")
	dnsName := flag.String("dns-name", "myapp.example.com", "The top level DNS name of the cluster.")
	keyPair := flag.String("key-pair", "", "the name of the EC2 SSH keypair to use")
	instanceType := flag.String("instance-type", "t1.micro",
		"The type of instance to use for master instances.")
	stackName := flag.String("stack", "", "The name of the CloudFormation stack. The default is derived from the DNS name.")
	doDryRun := flag.Bool("dry-run", false, "just print the cloudformation template, don't deploy it.")
	flag.Parse()

	awsSession := session.New()
	if region := os.Getenv("AWS_REGION"); region != "" {
		awsSession.Config.WithRegion(region)
	}
	awsregion.GuessRegion(awsSession.Config)

	availabilityZones, err := GetAvailablityZones(awsSession)
	if err != nil {
		return err
	}

	parameters := Parameters{
		Application:        *application,
		Region:             *awsSession.Config.Region,
		DnsName:            *dnsName,
		MasterInstanceType: *instanceType,
		KeyPair:            *keyPair,
		AvailabilityZones:  availabilityZones,
	}
	template, err := MakeTemplate(&parameters)
	if err != nil {
		return err
	}

	if *doDryRun {
		templateBuf, err := json.MarshalIndent(template, "", "  ")
		if err != nil {
			return err
		}
		os.Stdout.Write(templateBuf)
	}

	if *stackName == "" {
		*stackName = regexp.MustCompile("[^A-Za-z0-9\\-]").ReplaceAllString(
			parameters.DnsName, "")
	}

	if err := deploycfn.Deploy(awsSession, *stackName, template); err != nil {
		log.Fatalf("deploy: %s", err)
	}

	return nil
}
