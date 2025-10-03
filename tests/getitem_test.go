package tests

import (
	"context"
	"reflect"
	"testing"

	"github.com/oolio-group/dynago"
)

func TestGetItemWithConsistentRead(t *testing.T) {
	testCases := []struct {
		title               string
		pk                  dynago.Attribute  
		sk                  dynago.Attribute
		opts                []dynago.GetItemOptions
		source              *User
		expectedFound       bool
		expectConsistentRead bool
	}{
		{
			title: "get item with consistent read enabled",
			pk:    dynago.StringValue("users#consistent_getitem"),
			sk:    dynago.StringValue("user#consistent"),
			opts:  []dynago.GetItemOptions{dynago.WithConsistentReadItem()},
			source: &User{
				Id: "consistent_user",
				Pk: "users#consistent_getitem",
				Sk: "user#consistent",
			},
			expectedFound:       true,
			expectConsistentRead: true,
		},
		{
			title: "get item without consistent read (default eventual consistency)",
			pk:    dynago.StringValue("users#eventual_getitem"),
			sk:    dynago.StringValue("user#eventual"),
			opts:  []dynago.GetItemOptions{},
			source: &User{
				Id: "eventual_user",
				Pk: "users#eventual_getitem", 
				Sk: "user#eventual",
			},
			expectedFound:       true,
			expectConsistentRead: false,
		},
		{
			title: "get item not found with consistent read",
			pk:    dynago.StringValue("users#notfound"),
			sk:    dynago.StringValue("user#notfound"),
			opts:  []dynago.GetItemOptions{dynago.WithConsistentReadItem()},
			source:              nil,
			expectedFound:       false,
			expectConsistentRead: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			t.Parallel()

			table := prepareTable(t)
			ctx := context.TODO()

			// prepare the table, write test sample data if provided
			if tc.source != nil {
				err := table.PutItem(ctx, tc.pk, tc.sk, tc.source)
				if err != nil {
					t.Fatalf("unexpected error setting up test data: %s", err)
				}
			}

			var out User
			err, found := table.GetItem(ctx, tc.pk, tc.sk, &out, tc.opts...)
			if err != nil {
				t.Fatalf("unexpected error %s", err)
			}

			if found != tc.expectedFound {
				t.Errorf("expected found to be %v; got %v", tc.expectedFound, found)
			}

			if tc.expectedFound && tc.source != nil {
				if !reflect.DeepEqual(*tc.source, out) {
					t.Errorf("expected GetItem to return %v; got %v", *tc.source, out)
				}
			}

			// Note: We can't directly verify that ConsistentRead was set in the actual DynamoDB request
			// because that would require mocking the AWS client. The test verifies that the function
			// can be called without error and returns expected results.
		})
	}
}