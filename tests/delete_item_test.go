package tests

import (
	"context"
	"testing"

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
		var output user
		err, found := table.GetItem(ctx, dynago.StringValue(item.Pk), dynago.StringValue(item.Sk), &output)

		if err != nil {
			t.Fatalf("expected query to succeed for item %v; got %s", item, err)
		}
		if found == false {
			t.Fatalf("expected item %v to be found; got none", item)
		}
	}

	// delete 1st item
	err := table.DeleteItem(ctx, dynago.StringValue(items[0].Pk), dynago.StringValue(items[0].Sk))

	if err != nil {
		t.Fatalf("failed to delete item %v; got %s", items[0], err)
	}

	// query first item to confirm it is deleted
	var deletedItem user
	err, found := table.GetItem(ctx, dynago.StringValue(items[0].Pk), dynago.StringValue(items[0].Sk), &deletedItem)

	if err != nil {
		t.Fatalf("expected query to succeed for item %v; got %s", items[0], err)
	}
	if found == true {
		t.Fatalf("expected item to be deleted; got %v", items[0])
	}

	// query other items to confirm they still exist
	for i := 1; i < len(items); i++ {
		var output user
		err, found := table.GetItem(ctx, dynago.StringValue(items[i].Pk), dynago.StringValue(items[i].Sk), &output)

		if err != nil {
			t.Fatalf("expected query to succeed for item %v; got %s", items[i], err)
		}
		if found == false {
			t.Fatalf("expected item %v to be found; got none", items[i])
		}
	}
}
