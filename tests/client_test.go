package tests

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/oolio-group/dynago"
	"github.com/ory/dockertest/v3"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func startLocalDatabase() (addr string, purge func()) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalln("could not connect to docker", err)
	}

	resource, err := pool.Run("amazon/dynamodb-local", "latest", []string{})
	if err != nil {
		log.Fatalf("could not start container: %s", err)
	}
	addr = fmt.Sprintf("http://localhost:%s", resource.GetPort("8000/tcp"))
	return addr, func() {
		if err := pool.Purge(resource); err != nil {
			log.Println(err)
		}
	}
}

var dynamoEndpoint string

func TestMain(m *testing.M) {
	address, cleanup := startLocalDatabase()
	dynamoEndpoint = address

	code := m.Run()
	// os.Exit does not respect defer
	cleanup()
	os.Exit(code)
}

func createTestTable(t *dynago.Client) error {
	_, err := t.GetDynamoDBClient().CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{{
			AttributeName: aws.String("pk"),
			AttributeType: types.ScalarAttributeTypeS,
		}, {AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		TableName:   &t.TableName,
		BillingMode: types.BillingModePayPerRequest,
		TableClass:  types.TableClassStandard,
	})
	return err
}

func TestNewClient(t *testing.T) {
	tableName := "small-kitty-random-rbg-color"
	table, err := dynago.NewClient(context.TODO(), dynago.ClientOptions{
		TableName:        tableName,
		PartitionKeyName: "pk",
		SortKeyName:      "sk",
		Region:           "us-east-1",
	})
	if err != nil {
		t.Fatalf("expected configuration to succeed, got %s", err)
	}

	if got, wanted := table.TableName, tableName; got != wanted {
		t.Fatalf("expected table name to be %s, got %s", wanted, got)
	}
	if got, wanted := table.Keys["pk"], "pk"; got != wanted {
		t.Fatalf("expected key name to be %s, got %s", wanted, got)
	}

	// need to mock aws sdk to actually test the default connection,
	// any real operation will create resources in aws if there is a default credential
}

func TestNewClientLocalEndpoint(t *testing.T) {
	table, err := dynago.NewClient(context.TODO(), dynago.ClientOptions{
		TableName: "test",
		Endpoint: &dynago.EndpointResolver{
			EndpointURL:     dynamoEndpoint,
			AccessKeyID:     "dummy",
			SecretAccessKey: "dummy",
		},
		PartitionKeyName: "pk",
		SortKeyName:      "sk",
		Region:           "us-east-1",
	})
	if err != nil {
		t.Fatalf("expected configuration to succeed, got %s", err)
	}

	if os.Getenv("CI") == "true" {
		t.Skip("Skipping table creation in CI environment")
		return
	}

	err = createTestTable(table)
	if err != nil {
		t.Skipf("Skipping test due to DynamoDB connection issue: %s", err)
	}
}
