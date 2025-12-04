package tests

import (
	"context"
	"reflect"
	"testing"

	"github.com/oolio-group/dynago"
)

type BatchTestRecord struct {
	ID   string
	Name string
	Age  int
}

func TestBatchPutItems(t *testing.T) {
	table := prepareTable(t)
	ctx := context.TODO()

	testCases := []struct {
		title    string
		inputs   []dynago.BatchPutItemsInput
		expected []BatchTestRecord
	}{
		{
			title: "single item batch",
			inputs: []dynago.BatchPutItemsInput{
				{
					PartitionKeyValue: dynago.StringValue("user_1"),
					SortKeyValue:      dynago.StringValue("profile_1"),
					Item: BatchTestRecord{
						ID:   "1",
						Name: "John Doe",
						Age:  30,
					},
				},
			},
			expected: []BatchTestRecord{
				{
					ID:   "1",
					Name: "John Doe",
					Age:  30,
				},
			},
		},
		{
			title: "multiple items batch",
			inputs: []dynago.BatchPutItemsInput{
				{
					PartitionKeyValue: dynago.StringValue("user_1"),
					SortKeyValue:      dynago.StringValue("profile_1"),
					Item: BatchTestRecord{
						ID:   "1",
						Name: "John Doe",
						Age:  30,
					},
				},
				{
					PartitionKeyValue: dynago.StringValue("user_2"),
					SortKeyValue:      dynago.StringValue("profile_2"),
					Item: BatchTestRecord{
						ID:   "2",
						Name: "Jane Smith",
						Age:  25,
					},
				},
				{
					PartitionKeyValue: dynago.StringValue("user_3"),
					SortKeyValue:      dynago.StringValue("profile_3"),
					Item: BatchTestRecord{
						ID:   "3",
						Name: "Bob Johnson",
						Age:  35,
					},
				},
			},
			expected: []BatchTestRecord{
				{
					ID:   "1",
					Name: "John Doe",
					Age:  30,
				},
				{
					ID:   "2",
					Name: "Jane Smith",
					Age:  25,
				},
				{
					ID:   "3",
					Name: "Bob Johnson",
					Age:  35,
				},
			},
		},
		{
			title: "batch with more than 25 items (chunking test)",
			inputs: func() []dynago.BatchPutItemsInput {
				var inputs []dynago.BatchPutItemsInput
				for i := 1; i <= 30; i++ {
					inputs = append(inputs, dynago.BatchPutItemsInput{
						PartitionKeyValue: dynago.StringValue("batch_test"),
						SortKeyValue:      dynago.StringValue("item_" + string(rune(i+'0'))),
						Item: BatchTestRecord{
							ID:   string(rune(i + '0')),
							Name: "User " + string(rune(i+'0')),
							Age:  20 + i,
						},
					})
				}
				return inputs
			}(),
			expected: func() []BatchTestRecord {
				var expected []BatchTestRecord
				for i := 1; i <= 30; i++ {
					expected = append(expected, BatchTestRecord{
						ID:   string(rune(i + '0')),
						Name: "User " + string(rune(i+'0')),
						Age:  20 + i,
					})
				}
				return expected
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			t.Parallel()

			err := table.BatchPutItems(ctx, tc.inputs)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			// Verify all items were written correctly
			for i, input := range tc.inputs {
				var result BatchTestRecord
				err, found := table.GetItem(ctx, input.PartitionKeyValue, input.SortKeyValue, &result)
				if err != nil {
					t.Fatalf("unexpected error retrieving item %d: %s", i, err)
				}
				if !found {
					t.Fatalf("item %d not found", i)
				}
				if !reflect.DeepEqual(tc.expected[i], result) {
					t.Errorf("item %d: expected %+v, got %+v", i, tc.expected[i], result)
				}
			}
		})
	}
}

func TestBatchPutItemsError(t *testing.T) {
	table := prepareTable(t)
	ctx := context.TODO()

	testCases := []struct {
		title       string
		inputs      []dynago.BatchPutItemsInput
		expectError bool
	}{
		{
			title: "empty partition key should error",
			inputs: []dynago.BatchPutItemsInput{
				{
					PartitionKeyValue: dynago.StringValue(""),
					SortKeyValue:      dynago.StringValue("profile_1"),
					Item: BatchTestRecord{
						ID:   "1",
						Name: "John Doe",
						Age:  30,
					},
				},
			},
			expectError: true,
		},
		{
			title: "empty sort key should error",
			inputs: []dynago.BatchPutItemsInput{
				{
					PartitionKeyValue: dynago.StringValue("user_1"),
					SortKeyValue:      dynago.StringValue(""),
					Item: BatchTestRecord{
						ID:   "1",
						Name: "John Doe",
						Age:  30,
					},
				},
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			t.Parallel()

			err := table.BatchPutItems(ctx, tc.inputs)
			if tc.expectError && err == nil {
				t.Fatalf("expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}