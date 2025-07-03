package tests

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/oolio-group/dynago"
)

type Terminal struct {
	Id string
	Pk string
	Sk string
}

func TestTransactItems(t *testing.T) {
	table := prepareTable(t)
	testCases := []struct {
		title     string
		condition string
		keys      map[string]types.AttributeValue
		opts      []dynago.QueryOptions
		//items to be added
		newItems   []Terminal
		operations []types.TransactWriteItem
		//items expected to exist in table after transaction operation
		expected    []Terminal
		expectedErr error
	}{{
		title:     "assign terminal - only add a terminal",
		condition: "pk = :pk",
		keys: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "merchant1"},
		},
		newItems: []Terminal{},
		operations: []types.TransactWriteItem{
			table.WithPutItem("merchant1", "terminal1", Terminal{
				Id: "1",
				Pk: "merchant1",
				Sk: "terminal1",
			}),
		},
		expected: []Terminal{
			{
				Id: "1",
				Pk: "merchant1",
				Sk: "terminal1",
			},
		},
	},
		{
			title:     "assign terminal - delete existing and update with new",
			condition: "pk = :pk",
			keys: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "merchant2"},
			},
			newItems: []Terminal{{
				Id: "1",
				Pk: "merchant2",
				Sk: "terminal1",
			}},
			operations: []types.TransactWriteItem{
				table.WithDeleteItem("merchant2", "terminal1"),
				table.WithPutItem("merchant2", "terminal2", Terminal{
					Id: "2",
					Pk: "merchant2",
					Sk: "terminal2",
				}),
			},
			expected: []Terminal{
				{
					Id: "2",
					Pk: "merchant2",
					Sk: "terminal2",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			t.Parallel()
			ctx := context.TODO()
			// Create Item
			if len(tc.newItems) > 0 {
				items := make([]*dynago.TransactPutItemsInput, 0, len(tc.newItems))
				for _, item := range tc.newItems {
					items = append(items, &dynago.TransactPutItemsInput{
						PartitionKeyValue: dynago.StringValue(item.Pk),
						SortKeyValue:      dynago.StringValue(item.Sk),
						Item:              item,
					})
				}
				err := table.TransactPutItems(ctx, items)
				if err != nil {
					t.Fatalf("transaction put items failed; got %s", err)
				}
			}
			//perform operations
			if len(tc.operations) > 0 {
				err := table.TransactItems(ctx, tc.operations...)
				if err != nil {
					t.Fatalf("error occurred %s", err)
				}

			}

			var out []Terminal
			_, err := table.Query(ctx, tc.condition, tc.keys, &out)
			if tc.expectedErr != nil {
				if err == nil {
					t.Fatalf("expected query to fail with %s", tc.expectedErr)
				}
				if !strings.Contains(err.Error(), tc.expectedErr.Error()) {
					t.Fatalf("expected query to fail with %s; got %s", tc.expectedErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected query to succeed; got %s", err)
			}
			if !reflect.DeepEqual(tc.expected, out) {
				t.Errorf("expected query to return %v; got %v", tc.expected, out)
			}

		})
	}

}
