package tests

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/oolio-group/dynago"
)

type user struct {
	Pk string `dynamodbav:"pk"`
	Sk string `dynamodbav:"sk"`
}

func TestDeleteItem(t *testing.T) {
	table := prepareTable(t, dynamoEndpoint, "delete_test")

	ctx := context.TODO()

	// Insert a single item
	item := user{Pk: "users#org1", Sk: "user#1"}
	err := table.PutItem(ctx, dynago.StringValue(item.Pk), dynago.StringValue(item.Sk), item)
	if err != nil {
		t.Fatalf("failed to insert item; got %s", err)
	}

	// Query the item to ensure it exists
	var output []user
	_, err = table.Query(ctx, "pk = :pk and sk = :sk", map[string]types.AttributeValue{
		":pk": &types.AttributeValueMemberS{Value: item.Pk},
		":sk": &types.AttributeValueMemberS{Value: item.Sk},
	}, &output)
	if err != nil {
		t.Fatalf("expected query to succeed; got %s", err)
	}
	if len(output) == 0 {
		t.Fatalf("expected item to be found, but found none")
	}

	// Delete the item
	err = table.DeleteItem(ctx, dynago.StringValue(item.Pk), dynago.StringValue(item.Sk))
	if err != nil {
		t.Fatalf("expected delete to succeed; got %s", err)
	}

	// Query the item again to ensure it no longer exists
	var deleteOutput []user
	_, err = table.Query(ctx, "pk = :pk and sk = :sk", map[string]types.AttributeValue{
		":pk": &types.AttributeValueMemberS{Value: item.Pk},
		":sk": &types.AttributeValueMemberS{Value: item.Sk},
	}, &deleteOutput)

	if err != nil {
		t.Fatalf("expected query to succeed; got %s", err)
	}
	if len(deleteOutput) != 0 {
		t.Fatalf("expected item to be deleted; found %v", deleteOutput)
	}
}
