package tests

import (
	"context"
	"os"
	"testing"

	"github.com/oolio-group/dynago"
	"github.com/oolio-group/dynago/testing/localdb"
)

var testdb *localdb.TestDatabase

func TestMain(m *testing.M) {
	db, cleanup := localdb.Start()
	testdb = db

	code := m.Run()
	// os.Exit does not respect defer
	cleanup()
	os.Exit(code)
}

func TestNewClient(t *testing.T) {
	tableName := getRandomTableName(t)
	client, err := dynago.NewClient(context.TODO(), dynago.ClientOptions{
		TableName:        tableName,
		PartitionKeyName: "pk",
		SortKeyName:      "sk",
		Region:           "us-east-1",
		Endpoint: &dynago.EndpointResolver{
			EndpointURL:     testdb.Endpoint(),
			AccessKeyID:     "dummy",
			SecretAccessKey: "dummy",
		},
	})
	if err != nil {
		t.Fatalf("expected configuration to succeed, got %s", err)
	}

	if got, wanted := client.TableName, tableName; got != wanted {
		t.Fatalf("expected table name to be %s, got %s", wanted, got)
	}
	if got, wanted := client.Keys["pk"], "pk"; got != wanted {
		t.Fatalf("expected key name to be %s, got %s", wanted, got)
	}
	err = testdb.CreateTable(context.TODO(), tableName, "pk", "sk")
	if err != nil {
		t.Fatalf("expected table creation to succeed, got %s", err)
	}
}
