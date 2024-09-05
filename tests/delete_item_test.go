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

	items := []user{
		{Pk: "users#org1", Sk: "user#1"},
		{Pk: "users#org1", Sk: "user#2"},
		{Pk: "users#org1", Sk: "user#3"},
	}

	for _, item := range items {
		err := table.PutItem(ctx, dynago.StringValue(item.Pk), dynago.StringValue(item.Sk), item)
		if err != nil {
			t.Fatalf("failed to insert item %v; got %s", item, err)
		}
	}

	//query all items to check they exist
	for _, item := range items {
		var output []user
		_, err := table.Query(ctx, "pk = :pk and sk = :sk", map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: item.Pk},
			":sk": &types.AttributeValueMemberS{Value: item.Sk},
		}, &output)
		if err != nil {
			t.Fatalf("expected query to succeed for item %v; got %s", item, err)
		}
		if len(output) == 0 {
			t.Fatalf("expected item %v to be found, but found none", item)
		}
	}

	// delete 1st item
	err := table.DeleteItem(ctx, dynago.StringValue(items[0].Pk), dynago.StringValue(items[0].Sk))
	if err != nil {
		t.Fatalf("expected delete to succeed for item %v; got %s", items[0], err)
	}

	// query first item to confirm it is deleted
	var deleteOutput []user
	_, err = table.Query(ctx, "pk = :pk and sk = :sk", map[string]types.AttributeValue{
		":pk": &types.AttributeValueMemberS{Value: items[0].Pk},
		":sk": &types.AttributeValueMemberS{Value: items[0].Sk},
	}, &deleteOutput)

	if err != nil {
		t.Fatalf("expected query to succeed for item %v after deletion; got %s", items[0], err)
	}
	if len(deleteOutput) != 0 {
		t.Fatalf("expected item %v to be deleted; found %v", items[0], deleteOutput)
	}

	// query other items to confirm they still exist
	for i := 1; i < len(items); i++ {
		var output []user
		_, err := table.Query(ctx, "pk = :pk and sk = :sk", map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: items[i].Pk},
			":sk": &types.AttributeValueMemberS{Value: items[i].Sk},
		}, &output)
		if err != nil {
			t.Fatalf("expected query to succeed for item %v; got %s", items[i], err)
		}
		if len(output) == 0 {
			t.Fatalf("expected item %v to be found, but found none", items[i])
		}
	}
}
