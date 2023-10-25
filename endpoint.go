package dynago

import "github.com/aws/aws-sdk-go-v2/aws"

type EndpointResolver struct {
	EndpointURL     string
	AccessKeyID     string
	SecretAccessKey string
}

func (r EndpointResolver) ResolveEndpoint(s string, region string, o ...interface{}) (aws.Endpoint, error) {
	return aws.Endpoint{URL: r.EndpointURL}, nil
}
