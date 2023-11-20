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
	endoint, purge := startLocalDatabase(t)
	defer purge()

	table := prepareTable(t, endoint, "transcact_item_test")

	testCases := []struct {
		title       string
		condition   string
		keys        map[string]types.AttributeValue
		opts        []dynago.QueryOptions
		source1     []Terminal
		source2     []Terminal
		expected    []Terminal
		expectedErr error
	}{{
		title:     "assign terminal",
		condition: "pk = :pk",
		keys: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "terminal1"},
		},
		source1: []Terminal{
			{
				Id: "1",
				Pk: "terminal1",
				Sk: "merchant1",
			},
		},
		source2: []Terminal{
			{
				Id: "1",
				Pk: "terminal1",
				Sk: "merchant2",
			},
		},
		expected: []Terminal{
			{
				Id: "1",
				Pk: "terminal1",
				Sk: "merchant2",
			},
		},
	},
	}
	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			t.Helper()
			ctx := context.TODO()
			// Create Item
			if len(tc.source1) > 0 {
				items := make([]*dynago.TransactPutItemsInput, 0, len(tc.source1))
				for _, item := range tc.source1 {
					items = append(items, &dynago.TransactPutItemsInput{
						dynago.StringValue(item.Pk), dynago.StringValue(item.Sk), item,
					})
				}
				err := table.TransactPutItems(ctx, items)
				if err != nil {
					t.Fatalf("prepare table failed; got %s", err)
				}
			}
			// 	Update Item
			items := make([]types.TransactWriteItem, 0, len(tc.source1)+len(tc.source2))
			if len(tc.source1) > 0 {
				for _, item := range tc.source1 {
					items = append(items, table.WithDeleteItem(ctx, item.Pk,
						item.Sk))
				}
			}
			if len(tc.source2) > 0 {
				for _, item := range tc.source2 {
					items = append(items, table.WithPutItem(ctx, item.Pk,
						item.Sk,
						item))
				}
			}
			err := table.TransactItems(ctx, items)
			if err != nil {
				t.Fatalf("error occurred %s", err)
			}

			var out []Terminal
			_, err = table.Query(ctx, tc.condition, tc.keys, &out)
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
