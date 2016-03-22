package aws

import (
	"encoding/json"
	"net/http"
)

var InstanceType2Arch = map[string]string{
	"t1.micro":    "pv",
	"t2.micro":    "hvm",
	"t2.small":    "hvm",
	"t2.medium":   "hvm",
	"m1.small":    "pv",
	"m1.medium":   "pv",
	"m1.large":    "pv",
	"m1.xlarge":   "pv",
	"m2.xlarge":   "pv",
	"m2.2xlarge":  "pv",
	"m2.4xlarge":  "pv",
	"m3.medium":   "hvm",
	"m3.large":    "hvm",
	"m3.xlarge":   "hvm",
	"m3.2xlarge":  "hvm",
	"c1.medium":   "pv",
	"c1.xlarge":   "pv",
	"c3.large":    "hvm",
	"c3.xlarge":   "hvm",
	"c3.2xlarge":  "hvm",
	"c3.4xlarge":  "hvm",
	"c3.8xlarge":  "hvm",
	"c4.large":    "hvm",
	"c4.xlarge":   "hvm",
	"c4.2xlarge":  "hvm",
	"c4.4xlarge":  "hvm",
	"c4.8xlarge":  "hvm",
	"g2.2xlarge":  "HVMG2",
	"r3.large":    "hvm",
	"r3.xlarge":   "hvm",
	"r3.2xlarge":  "hvm",
	"r3.4xlarge":  "hvm",
	"r3.8xlarge":  "hvm",
	"i2.xlarge":   "hvm",
	"i2.2xlarge":  "hvm",
	"i2.4xlarge":  "hvm",
	"i2.8xlarge":  "hvm",
	"hi1.4xlarge": "hvm",
	"hs1.8xlarge": "hvm",
	"cr1.8xlarge": "hvm",
	"cc2.8xlarge": "hvm",
}

func GetCoreOSAMI(parameters *Parameters) error {
	resp, err := http.Get("https://coreos.com/dist/aws/aws-stable.json")
	if err != nil {
		return err
	}
	data := map[string]interface{}{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}
	data2 := data[parameters.Region].(map[string]interface{})
	parameters.MasterAMI = data2[InstanceType2Arch[parameters.MasterInstanceType]].(string)

	return nil
}
