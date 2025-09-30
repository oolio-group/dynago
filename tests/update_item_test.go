package tests

import (
	"context"
	"reflect"
	"testing"

	"github.com/oolio-group/dynago"
)

type UpdateRecord struct {
	ID      string `json:"id"`
	Pk      string `json:"pk"`
	Sk      string `json:"sk"`
	Name    string `json:"name"`
	Age     int    `json:"age"`
	Email   string `json:"email"`
	Version uint   `json:"version"`
}

func TestUpdateItem(t *testing.T) {
	table := prepareTable(t)
	ctx := context.Background()

	testCases := []struct {
		title       string
		initialItem UpdateRecord
		updateFields interface{}
		opts        []dynago.UpdateOption
		expected    UpdateRecord
		expectError bool
	}{
		{
			title: "update single field",
			initialItem: UpdateRecord{
				ID:   "test1",
				Pk:   "user#1",
				Sk:   "profile",
				Name: "John Doe",
				Age:  30,
			},
			updateFields: map[string]interface{}{
				"Name": "Jane Doe",
			},
			expected: UpdateRecord{
				ID:   "test1",
				Pk:   "user#1",
				Sk:   "profile",
				Name: "Jane Doe",
				Age:  30,
			},
		},
		{
			title: "update multiple fields",
			initialItem: UpdateRecord{
				ID:    "test2",
				Pk:    "user#2",
				Sk:    "profile",
				Name:  "Bob Smith",
				Age:   25,
				Email: "bob@example.com",
			},
			updateFields: map[string]interface{}{
				"Name":  "Robert Smith",
				"Age":   26,
				"Email": "robert@example.com",
			},
			expected: UpdateRecord{
				ID:    "test2",
				Pk:    "user#2",
				Sk:    "profile",
				Name:  "Robert Smith",
				Age:   26,
				Email: "robert@example.com",
			},
		},
		{
			title: "update with struct fields",
			initialItem: UpdateRecord{
				ID:   "test3",
				Pk:   "user#3",
				Sk:   "profile",
				Name: "Alice Johnson",
				Age:  28,
			},
			updateFields: struct {
				Name string `json:"name"`
				Age  int    `json:"age"`
			}{
				Name: "Alice Williams",
				Age:  29,
			},
			expected: UpdateRecord{
				ID:   "test3",
				Pk:   "user#3",
				Sk:   "profile",
				Name: "Alice Williams",
				Age:  29,
			},
		},
		{
			title: "update with optimistic lock",
			initialItem: UpdateRecord{
				ID:      "test4",
				Pk:      "user#4",
				Sk:      "profile",
				Name:    "David Brown",
				Age:     35,
				Version: 1,
			},
			updateFields: map[string]interface{}{
				"Name": "David Wilson",
			},
			opts: []dynago.UpdateOption{
				dynago.WithOptimisticLockForUpdate("Version", 1),
			},
			expected: UpdateRecord{
				ID:      "test4",
				Pk:      "user#4",
				Sk:      "profile",
				Name:    "David Wilson",
				Age:     35,
				Version: 2, // Should be incremented
			},
		},
		{
			title: "update with conditional expression",
			initialItem: UpdateRecord{
				ID:   "test5",
				Pk:   "user#5",
				Sk:   "profile",
				Name: "Emma Davis",
				Age:  22,
			},
			updateFields: map[string]interface{}{
				"Age": 23,
			},
			opts: []dynago.UpdateOption{
				dynago.WithConditionalUpdate(
					"attribute_exists(#name)", 
					map[string]dynago.Attribute{},
					map[string]string{
						"#name": "Name", // Use the correct field name that exists in the struct
					},
				),
			},
			expected: UpdateRecord{
				ID:   "test5",
				Pk:   "user#5",
				Sk:   "profile",
				Name: "Emma Davis",
				Age:  23,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			t.Parallel()

			// Create initial item
			pk := dynago.StringValue(tc.initialItem.Pk)
			sk := dynago.StringValue(tc.initialItem.Sk)

			err := table.PutItem(ctx, pk, sk, tc.initialItem)
			if err != nil {
				t.Fatalf("failed to create initial item: %s", err)
			}

			// Update the item
			err = table.UpdateItem(ctx, pk, sk, tc.updateFields, tc.opts...)
			if tc.expectError {
				if err == nil {
					t.Fatalf("expected update to fail, but it succeeded")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error during update: %s", err)
			}

			// Retrieve and verify the updated item
			var result UpdateRecord
			err, found := table.GetItem(ctx, pk, sk, &result)
			if err != nil {
				t.Fatalf("failed to retrieve updated item: %s", err)
			}
			if !found {
				t.Fatalf("item not found after update")
			}

			if !reflect.DeepEqual(tc.expected, result) {
				t.Errorf("expected updated item to be %+v; got %+v", tc.expected, result)
			}
		})
	}
}

func TestUpdateItemCustomExpression(t *testing.T) {
	table := prepareTable(t)
	ctx := context.Background()

	// Test custom update expression (ADD operation)
	initialItem := UpdateRecord{
		ID:   "expr_test",
		Pk:   "user#expr",
		Sk:   "profile",
		Name: "Counter User",
		Age:  10,
	}

	pk := dynago.StringValue(initialItem.Pk)
	sk := dynago.StringValue(initialItem.Sk)

	// Create initial item
	err := table.PutItem(ctx, pk, sk, initialItem)
	if err != nil {
		t.Fatalf("failed to create initial item: %s", err)
	}

	// Update using ADD expression to increment age
	err = table.UpdateItem(ctx, pk, sk, nil, dynago.WithUpdateExpression(
		"ADD #age :increment",
		map[string]dynago.Attribute{
			":increment": dynago.NumberValue(5),
		},
		map[string]string{
			"#age": "Age", // Use the actual struct field name
		},
	))
	if err != nil {
		t.Fatalf("failed to update with custom expression: %s", err)
	}

	// Verify the result
	var result UpdateRecord
	err, found := table.GetItem(ctx, pk, sk, &result)
	if err != nil {
		t.Fatalf("failed to retrieve updated item: %s", err)
	}
	if !found {
		t.Fatalf("item not found after update")
	}

	expectedAge := 15 // 10 + 5
	if result.Age != expectedAge {
		t.Errorf("expected age to be %d after ADD operation; got %d", expectedAge, result.Age)
	}
}

func TestUpdateItemErrors(t *testing.T) {
	table := prepareTable(t)
	ctx := context.Background()

	pk := dynago.StringValue("error#test")
	sk := dynago.StringValue("profile")

	testCases := []struct {
		title       string
		fields      interface{}
		description string
	}{
		{
			title:       "nil fields",
			fields:      nil,
			description: "should fail with nil fields",
		},
		{
			title:       "empty map",
			fields:      map[string]interface{}{},
			description: "should fail with empty fields",
		},
		{
			title: "only primary keys",
			fields: map[string]interface{}{
				"pk": "should_not_update",
				"sk": "should_not_update",
			},
			description: "should fail when only primary keys are provided",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			err := table.UpdateItem(ctx, pk, sk, tc.fields)
			if err == nil {
				t.Errorf("%s - expected error but got none", tc.description)
			}
		})
	}
}