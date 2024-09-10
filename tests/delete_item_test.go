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
	pk string
	sk string
}

func getSeedData(ctx context.Context, table *dynago.Client, seedData []tableRecord) ([]tableRecord, error) {
	itemsToGet := make([]map[string]types.AttributeValue, 0, len(seedData))
	for _, v := range seedData {
		itemsToGet = append(itemsToGet, map[string]types.AttributeValue{
			"pk": dynago.StringValue(v.Pk),
			"sk": dynago.StringValue(v.Sk),
		})
	}

	var out []tableRecord

	err := table.BatchGetItems(ctx, itemsToGet, &out)
	if err != nil {
		return nil, fmt.Errorf("Unable to get seed data: %w", err)
	}

	return out, nil
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

	type testCase struct {
		title         string
		itemsToDelete []userKeys
		itemsLeft     int
		seedData      []tableRecord
	}

	cases := []testCase{
		{
			title: "Delete User 2",
			itemsToDelete: []userKeys{
				{
					pk: "users#org1",
					sk: "user#2",
				},
			},
			itemsLeft: 3,
			seedData:  records,
		},
		{
			title: "Delete all users",
			itemsToDelete: []userKeys{
				{
					pk: "users#org1",
					sk: "user#1",
				},
				{
					pk: "users#org1",
					sk: "user#2",
				},
				{
					pk: "users#org1",
					sk: "user#3",
				},
				{
					pk: "users#org2",
					sk: "user#4",
				},
			},
			itemsLeft: 0,
			seedData:  records,
		},
		{
			title: "Delete wrong user",
			itemsToDelete: []userKeys{
				{
					pk: "users#org1",
					sk: "user#none",
				},
			},
			itemsLeft: 4,
			seedData:  records,
		},
		{
			title: "Delete user with invalid keys",
			itemsToDelete: []userKeys{
				{
					pk: "invalid#org",
					sk: "invalid#user",
				},
			},
			itemsLeft: 4,
			seedData:  records,
		},
	}

	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			err := seedRecords(ctx, table, c.seedData)
			if err != nil {
				t.Fatalf("failed to prepare records; got %s", err)
			}

			for _, item := range c.itemsToDelete {
				err = table.DeleteItem(ctx, dynago.StringValue(item.pk), dynago.StringValue(item.sk))
				if err != nil {
					t.Fatalf("unable to delete records; got %s", err)
				}
			}

			data, err := getSeedData(ctx, table, c.seedData)
			if err != nil {
				t.Fatalf("failed to prepare records; got %s", err)
			}

			// see if "itemsToDelete" are the ones deleted.
			dataKeys := make([]string, 0, len(data))
			for _, item := range data {
				dataKeys = append(dataKeys, fmt.Sprintf("%s--%s", item.Pk, item.Sk))
			}

			for _, v := range c.itemsToDelete {
				recKey := fmt.Sprintf("%s--%s", v.pk, v.sk)
				if slices.Contains(dataKeys, recKey) {
					t.Fatalf("expected items to deleted in db but found it, pk %s; sk %s", v.pk, v.sk)
				}
			}

			// compare items remaining is seed data matches test
			if len(data) != c.itemsLeft {
				t.Fatalf("expected items in db; %v found; %v", c.itemsLeft, len(data))
			}
		})
	}
}
