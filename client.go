package dynago

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type ClientOptions struct {
	TableName        string
	Region           string
	PartitionKeyName string
	SortKeyName      string
	Endpoint         *EndpointResolver
	Middlewares      []func(*aws.Config)
}

type Client struct {
	client    *dynamodb.Client
	TableName string
	Keys      map[string]string
}

// Create a new instance of DynamoTable. internally creates aws connection configuration for the db
// If DynamoTable.Endpoint is specified connects to the db at the given URL, or the default credential of the system is used
// To connect to AWS DynamoDB from a running pod or EC2 instance, use the default credentials without Endpoint option
// To connect to a local DynamoDB process, provider Endpoint
//
//	table, err := dynamite.NewDynamoTable(dynamite.ClientOptions{
//	  TableName: "test",
//	  Endpoint: &dynamite.EndpointResolver{
//	    EndpointURL:     "http://localhost:" + port,
//	    AccessKeyID:     "dummy",
//	    SecretAccessKey: "dummy",
//	  },
//	  PartitionKeyName: "pk",
//	  SortKeyName:      "sk",
//	  Region:           "us-east-1",
//	})
func NewClient(ctx context.Context, opt ClientOptions) (*Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(opt.Region),
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(so *retry.StandardOptions) {
				so.RateLimiter = ratelimit.NewTokenRateLimit(1000000)
			})
		}))

	// if an endpoint url is provided, connect to the remote/local dynamodb instead of AWS hosted dynamodb
	if opt.Endpoint != nil {
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithEndpointResolverWithOptions(opt.Endpoint),
			config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
				Value: aws.Credentials{
					AccessKeyID:     opt.Endpoint.AccessKeyID,
					SecretAccessKey: opt.Endpoint.SecretAccessKey,
				},
			}),
			config.WithRetryer(func() aws.Retryer {
				return retry.NewStandard(func(so *retry.StandardOptions) {
					so.RateLimiter = ratelimit.NewTokenRateLimit(1000000)
				})
			}),
		)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config, %w", err)
	}

	// aws sdk middlewares, usage tracing
	// TODO: better implementation, take a tracer impl and do much more
	for _, m := range opt.Middlewares {
		m(&cfg)
	}

	// Using the Config value, create the DynamoDB client
	c := dynamodb.NewFromConfig(cfg)
	return &Client{
		client:    c,
		TableName: opt.TableName,
		Keys: map[string]string{
			"pk": opt.PartitionKeyName,
			"sk": opt.SortKeyName,
		},
	}, nil
}

func (t *Client) GetDynamoDBClient() *dynamodb.Client {
	return t.client
}
