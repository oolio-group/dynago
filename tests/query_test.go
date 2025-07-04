package tests

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/oolio-group/dynago"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// getRandomTableName generates a unique random table name for testing purposes.
// This helps avoid table name conflicts when running tests in parallel.
// The returned name is prefixed with "test-table-" followed by a random hex string.
func getRandomTableName(t *testing.T) string {
	t.Helper()
	return "test-table-" + hex.EncodeToString(genRandomBytes(t, 6))
}

func prepareTable(t *testing.T) *dynago.Client {
	t.Helper()
	ctx := context.TODO()
	name := getRandomTableName(t)
	table, err := dynago.NewClient(ctx, dynago.ClientOptions{
		TableName: name,
		Endpoint: &dynago.EndpointResolver{
			EndpointURL:     testdb.Endpoint(),
			AccessKeyID:     "dummy",
			SecretAccessKey: "dummy",
		},
		PartitionKeyName: "pk",
		SortKeyName:      "sk",
		Region:           "us-east-1",
	})
	if err != nil {
		t.Fatalf("expected configuration to succeed, got %s", err)
	}
	err = testdb.CreateTable(ctx, name, "pk", "sk")
	if err != nil {
		t.Fatalf("expected table creation to succeed, got %s", err)
	}
	return table
}

type User struct {
	Id   string
	City string
	Age  uint

	Pk string
	Sk string
}

func TestQuery(t *testing.T) {
	testCases := []struct {
		title       string
		condition   string
		keys        map[string]types.AttributeValue
		opts        []dynago.QueryOptions
		source      []User
		expected    []User
		expectedErr error
	}{
		{
			title:     "no query matches",
			condition: "pk = :pk",
			keys: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "no records for this pk"},
			},
			opts:     []dynago.QueryOptions{},
			source:   []User{},
			expected: []User{},
		},
		{
			title:     "error prone query",
			condition: "pk - invalid",
			keys:      map[string]types.AttributeValue{},
			opts:      []dynago.QueryOptions{},
			source: []User{
				{
					Pk: "users#org1",
					Sk: "user#1",
				},
			},
			expectedErr: fmt.Errorf("api error ValidationException"),
		},
		{
			title:     "query in partition with sk matching prefix",
			condition: "pk = :pk and begins_with(sk, :sk)",
			keys: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "users#org1"},
				":sk": &types.AttributeValueMemberS{Value: "user#"},
			},
			opts: []dynago.QueryOptions{},
			source: []User{
				{
					Id: "1",
					Pk: "users#org1",
					Sk: "user#1",
				},
				{
					Id: "2",
					Pk: "users#org1",
					Sk: "user#2",
				},
				{
					Id: "3",
					Pk: "users#org2",
					Sk: "user#3",
				},
			},
			expected: []User{
				{
					Id: "1",
					Pk: "users#org1",
					Sk: "user#1",
				},
				{
					Id: "2",
					Pk: "users#org1",
					Sk: "user#2",
				},
			},
		},
		{
			title:     "query with contains filter condition on field",
			condition: "pk = :pk",
			keys: map[string]types.AttributeValue{
				":pk":     &types.AttributeValueMemberS{Value: "users#query_test"},
				":filter": &types.AttributeValueMemberS{Value: "Melbourne"},
			},
			opts: []dynago.QueryOptions{dynago.WithFilter("contains(#city, :filter)"), func(q *dynago.QueryInput) {
				q.ExpressionAttributeNames = map[string]string{
					"#city": "City",
				}
			}},
			source: []User{
				{
					Id:   "100",
					Pk:   "users#query_test",
					Sk:   "user#100",
					City: "West Melbourne",
				},
				{
					Id:   "200",
					Pk:   "users#query_test",
					Sk:   "user#200",
					City: "North Melbourne",
				},
				{
					Id:   "300",
					Pk:   "users#query_test",
					Sk:   "user#300",
					City: "Sydney",
				},
			},
			expected: []User{
				{
					Id:   "100",
					Pk:   "users#query_test",
					Sk:   "user#100",
					City: "West Melbourne",
				},
				{
					Id:   "200",
					Pk:   "users#query_test",
					Sk:   "user#200",
					City: "North Melbourne",
				}},
		},
		{
			title:     "query with limit on items returned",
			condition: "pk = :pk",
			keys: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "users#limit_test"},
			},
			opts: []dynago.QueryOptions{dynago.WithLimit(1)},
			source: []User{
				{
					Pk: "users#limit_test",
					Sk: "user#1",
				},
				{
					Pk: "users#limit_test",
					Sk: "user#2",
				},
			},
			expected: []User{
				{
					Pk: "users#limit_test",
					Sk: "user#1",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			t.Parallel()

			table := prepareTable(t)
			condition, keys, opts, source, expected := tc.condition, tc.keys, tc.opts, tc.source, tc.expected
			ctx := context.TODO()

			// prepare the table, write test sample data
			if len(source) > 0 {
				items := make([]*dynago.TransactPutItemsInput, 0, len(source))
				for _, item := range tc.source {
					items = append(items, &dynago.TransactPutItemsInput{
						PartitionKeyValue: dynago.StringValue(item.Pk),
						SortKeyValue:      dynago.StringValue(item.Sk),
						Item:              item,
					})
				}
				err := table.TransactPutItems(ctx, items)
				if err != nil {
					t.Fatalf("prepare table faile; got %s", err)
				}
			}

			var out []User
			_, err := table.Query(ctx, condition, keys, &out, opts...)
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
			if !reflect.DeepEqual(expected, out) {
				t.Errorf("expected query to return %v; got %v", expected, out)
			}
		})
	}
}

func genRandomBytes(t *testing.T, size int) (blk []byte) {
	t.Helper()
	blk = make([]byte, size)
	_, err := rand.Read(blk)
	if err != nil {
		t.Fatalf("rand failed %s", err)
	}
	return blk
}

func TestQueryPagination(t *testing.T) {
	table := prepareTable(t)
	// write 3MB worth of sample user records to the database for testing
	// dynamodb paginates query result by pages of 1MB recors by default.
	const batchSize = 100
	const batches = (3 * 1024) / batchSize
	for batch := 0; batch < batches; batch += 1 {
		items := make([]*dynago.TransactPutItemsInput, batchSize)
		for idx := range items {
			// 1 KB worth data in each user record
			record := User{
				Id: fmt.Sprintf("%x", genRandomBytes(t, 512)),
				Pk: "pk",
				Sk: fmt.Sprintf("user-%d-%d", batch, idx),
			}
			items[idx] = &dynago.TransactPutItemsInput{
				PartitionKeyValue: dynago.StringValue(record.Pk),
				SortKeyValue:      dynago.StringValue(record.Sk),
				Item:              record,
			}
		}
		err := table.TransactPutItems(context.TODO(), items)
		if err != nil {
			t.Fatalf("prepare table faile; got %s", err)
		}
	}

	testCases := []struct {
		title    string
		paginate bool
		limit    int32
		expected int
	}{
		{
			title:    "no limit, auto paginate 1MB chunks of records",
			expected: batches * batchSize,
		},
		{
			title:    "100 items within 1MB page",
			expected: 100,
			limit:    100,
		},
		{
			title:    "limit of 1500 spanning 2 1MB pages",
			expected: 1500,
			limit:    1500,
		},
		{
			title:    "limit of 2500 spanning 3 1MB pages",
			expected: 2500,
			limit:    2500,
		},
		{
			title:    "paginate 100 items per page; each page < 1MB",
			expected: batches * batchSize,
			paginate: true,
			limit:    100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			t.Parallel()
			limit, paginate, expected := tc.limit, tc.paginate, tc.expected
			var (
				results           = make([]User, 0, expected*2)
				exclusiveStartKey map[string]types.AttributeValue
			)

			for {
				var (
					out  []User
					opts = []dynago.QueryOptions{dynago.WithCursorKey(exclusiveStartKey)}
				)
				if limit != 0 {
					opts = append(opts, dynago.WithLimit(limit))
				}

				cursor, err := table.Query(context.TODO(), "pk = :pk", map[string]types.AttributeValue{
					":pk": &types.AttributeValueMemberS{Value: "pk"},
				}, &out, opts...)
				if err != nil {
					t.Fatalf("expected query to succeed; got %s", err)
				}
				exclusiveStartKey = cursor
				results = append(results, out...)
				if !paginate || cursor == nil {
					break
				}
				if len(results) > expected {
					t.Error("fetching more than expected; detected infinite loop")
					break
				}
			}

			if len(results) != expected {
				t.Errorf("expected query to return %d items; got %d", expected, len(results))
			}
			// check for duplicate entry in results. could happen if pagination is buggy
			occurances := map[string]bool{}
			for _, item := range results {
				if _, ok := occurances[item.Sk]; ok {
					t.Errorf("found duplicate item %s", item.Sk)
					return
				}
				occurances[item.Sk] = true
			}
		})
	}
}
