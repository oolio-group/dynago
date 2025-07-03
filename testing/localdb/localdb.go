package localdb

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	transport "github.com/aws/smithy-go/endpoints"
	"github.com/ory/dockertest/v3"
)

const DynamoImage = "amazon/dynamodb-local"

type TestDatabase struct {
	address string
}

func (db *TestDatabase) ready() error {
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: http.DefaultTransport,
	}
	res, err := client.Get(db.address)
	if err != nil {
		return fmt.Errorf("could not connect to database: %w", err)
	}
	if res == nil {
		return fmt.Errorf("database is not ready")
	}
	return nil
}

func (db TestDatabase) CreateTable(ctx context.Context, tableName, pk, sk string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(db.Credentials()),
	)
	if err != nil {
		return err
	}
	client := dynamodb.NewFromConfig(cfg, dynamodb.WithEndpointResolverV2(db))
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{{
			AttributeName: &pk,
			AttributeType: types.ScalarAttributeTypeS,
		}, {AttributeName: &sk, AttributeType: types.ScalarAttributeTypeS}},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: &pk, KeyType: types.KeyTypeHash},
			{AttributeName: &sk, KeyType: types.KeyTypeRange},
		},
		TableName:   &tableName,
		BillingMode: types.BillingModePayPerRequest,
		TableClass:  types.TableClassStandard,
	})
	if err != nil {
		return fmt.Errorf("failed to create table %w", err)
	}

	return nil
}

func (db TestDatabase) Endpoint() string {
	return db.address
}

func (db TestDatabase) Credentials() aws.CredentialsProvider {
	return credentials.NewStaticCredentialsProvider("dummy", "dummy", "")
}

// ResolveEndpoint implements dynamodb.EndpointResolverV2.
func (db TestDatabase) ResolveEndpoint(ctx context.Context, params dynamodb.EndpointParameters) (transport.Endpoint, error) {
	uri, err := url.Parse(db.address)
	if err != nil {
		return transport.Endpoint{}, fmt.Errorf("failed to parse endpoint URL %q: %w", db.address, err)
	}
	return transport.Endpoint{URI: *uri}, nil
}

func Start() (db *TestDatabase, purge func()) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalln("could not connect to docker", err)
	}

	resource, err := pool.Run(DynamoImage, "latest", []string{})
	if err != nil {
		log.Fatalf("could not start container: %s", err)
	}
	db = &TestDatabase{
		address: fmt.Sprintf("http://localhost:%s", resource.GetPort("8000/tcp")),
	}

	// exponential backoff-retry, because the container might not be ready to accept connections yet
	if err := pool.Retry(db.ready); err != nil {
		log.Fatalf("Could not connect to database: %s", err)
	}

	return db, func() {
		if err := pool.Purge(resource); err != nil {
			log.Println(err)
		}
	}
}
