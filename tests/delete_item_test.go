package tests

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/oolio-group/dynago"
)

type user struct {
	Name  string
	Phone string
}

type userKeys struct {
	Pk string
	Sk string
}

type testCase struct {
	title             string
	itemsToDelete     []userKeys
	expectedItemsLeft int
	seedData          []tableRecord
}

func TestDeleteItem(t *testing.T) {
	ctx := context.TODO()
	table := prepareTable(t, dynamoEndpoint, "delete_test")

	records := []tableRecord{
		{
			Pk: "users#org1",
			Sk: "user#1",
			Record: user{
				Name:  "User 1",
				Phone: "xyz",
			},
		},
		{
			Pk: "users#org1",
			Sk: "user#2",
			Record: user{
				Name:  "User 2",
				Phone: "asd",
			},
		},
		{
			Pk: "users#org1",
			Sk: "user#3",
			Record: user{
				Name:  "User 3",
				Phone: "qwe",
			},
		},
		{
			Pk: "users#org2",
			Sk: "user#4",
			Record: user{
				Name:  "User 4",
				Phone: "fgh",
			},
		},
	}

	cases := []testCase{
		{
			title: "Delete User 2",
			itemsToDelete: []userKeys{
				{
					Pk: "users#org1",
					Sk: "user#2",
				},
			},
			expectedItemsLeft: 3,
			seedData:          records, //each test case will have the same seed data
		},
		{
			title: "Delete item with wrong sk",
			itemsToDelete: []userKeys{
				{
					Pk: "users#org1",
					Sk: "user#none",
				},
			},
			expectedItemsLeft: 4,
			seedData:          records,
		},
		{
			title: "Delete item with wrong pk and sk",
			itemsToDelete: []userKeys{
				{
					Pk: "invalid#org",
					Sk: "invalid#user",
				},
			},
			expectedItemsLeft: 4,
			seedData:          records,
		},
	}

	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			//prepare records
			err := seedRecords(ctx, table, c.seedData)
			if err != nil {
				t.Fatalf("failed to prepare records; got %s", err)
			}

			//delete records
			for _, item := range c.itemsToDelete {
				err = table.DeleteItem(ctx, dynago.StringValue(item.Pk), dynago.StringValue(item.Sk))
				if err != nil {
					t.Fatalf("unable to delete records; got %s", err)
				}
			}

			//get original seed records
			itemsToGet := make([]map[string]types.AttributeValue, 0, len(c.seedData))
			for _, v := range c.seedData {
				itemsToGet = append(itemsToGet, map[string]types.AttributeValue{
					"pk": dynago.StringValue(v.Pk),
					"sk": dynago.StringValue(v.Sk),
				})
			}

			var remainingItems []tableRecord

			err = table.BatchGetItems(ctx, itemsToGet, &remainingItems)
			if err != nil {
				t.Fatalf("Unable to get seed data: %s", err)
			}

			//check if deleted items are not in db
			dataKeys := make([]string, 0, len(remainingItems))
			for _, item := range remainingItems {
				dataKeys = append(dataKeys, fmt.Sprintf("%s--%s", item.Pk, item.Sk))
			}

			for _, v := range c.itemsToDelete {
				recKey := fmt.Sprintf("%s--%s", v.Pk, v.Sk)
				if slices.Contains(dataKeys, recKey) {
					t.Fatalf("expected items to deleted in db but found it, pk %s; sk %s", v.Pk, v.Sk)
				}
			}

			//check if remaining records match expected number of items left
			if len(remainingItems) != c.expectedItemsLeft {
				t.Fatalf("expected items in db; %v found; %v", c.expectedItemsLeft, len(remainingItems))
			}
		})
	}
}
