package aws

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	cfn "github.com/crewjam/go-cloudformation"
)

type Parameters struct {
	Application       string
	DnsName           string
	Region            string
	AvailabilityZones []cfn.Stringable
	KeyPair           string
	VpcSubnets        []cfn.Stringable

	MasterAMI          string
	MasterInstanceType string
}

func GetAvailablityZones(awsSession *session.Session) ([]cfn.Stringable, error) {
	azinfo, err := ec2.New(awsSession).DescribeAvailabilityZones(&ec2.DescribeAvailabilityZonesInput{})
	if err != nil {
		return nil, err
	}

	goodZones := []cfn.Stringable{}
	badZones := []cfn.Stringable{}

	for _, availabilityZone := range azinfo.AvailabilityZones {
		if *availabilityZone.State == ec2.AvailabilityZoneStateAvailable {
			goodZones = append(goodZones, cfn.String(*availabilityZone.ZoneName))
		} else {
			badZones = append(badZones, cfn.String(*availabilityZone.ZoneName))
		}
	}
	return append(goodZones, badZones...), nil
}

// MakeTemplate returns a Cloudformation template for the project infrastructure
func MakeTemplate(parameters *Parameters) (*cfn.Template, error) {
	t := cfn.NewTemplate()
	t.Parameters = map[string]*cfn.Parameter{
		"MasterScale": &cfn.Parameter{
			Description: "Number of master instances",
			Type:        "String",
			Default:     "3",
		},
	}
	if err := GetCoreOSAMI(parameters); err != nil {
		return nil, err
	}
	if err := MakeVPC(parameters, t); err != nil {
		return nil, err
	}

	if err := MakeMaster(parameters, t); err != nil {
		return nil, err
	}

	if err := MakeMasterLoadBalancer(parameters, t); err != nil {
		return nil, err
	}

	if err := MakeHealthCheck(parameters, t); err != nil {
		return nil, err
	}

	return t, nil
}
