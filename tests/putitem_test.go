package tests

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/oolio-group/dynago"
)

type Record struct {
	ID string
	Pk string
	Sk string
}

func TestPutItem(t *testing.T) {
	table := prepareTable(t)
	testCases := []struct {
		title       string
		item        Record
		expected    Record
		expectedErr error
	}{
		{
			title: "success 1",
			item: Record{
				ID: "1",
				Pk: "account_1",
				Sk: "account_1",
			},
			expected: Record{
				ID: "1",
				Pk: "account_1",
				Sk: "account_1",
			},
		},
		{
			title: "success 2",
			item: Record{
				ID: "2",
				Pk: "account_2",
				Sk: "account_2",
			},
			expected: Record{
				ID: "2",
				Pk: "account_2",
				Sk: "account_2",
			},
		},
		{
			title: "pk is required",
			item: Record{
				ID: "3",
				Sk: "account_3",
			},
			expectedErr: fmt.Errorf("The AttributeValue for a key attribute cannot contain an empty string value. Key: pk"),
		},
		{
			title: "sk is required",
			item: Record{
				ID: "4",
				Pk: "account_4",
			},
			expectedErr: fmt.Errorf("The AttributeValue for a key attribute cannot contain an empty string value. Key: sk"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			t.Parallel()
			ctx := context.TODO()

			pk := dynago.StringValue(tc.item.Pk)
			sk := dynago.StringValue(tc.item.Sk)
			err := table.PutItem(ctx, pk, sk, &tc.item)
			if err != nil {
				if tc.expectedErr == nil {
					t.Fatalf("unexpected error %s", err)
				}
				if !strings.Contains(err.Error(), tc.expectedErr.Error()) {
					t.Fatalf("expected op to fail with %s; got %s", tc.expectedErr, err)
				}
				return
			}

			var out Record
			err, found := table.GetItem(ctx, pk, sk, &out)
			if err != nil {
				t.Fatalf("unexpected error %s", err)
			}
			if !found {
				t.Errorf("expected to find item with pk %s and sk %s", tc.item.Pk, tc.item.Sk)
			}
			if !reflect.DeepEqual(tc.expected, out) {
				t.Errorf("expected query to return %v; got %v", tc.expected, out)
			}
		})
	}
}

type LedgerAccount struct {
	ID      string
	Balance int
	Version uint
}

func TestPutItemWithOptimisticLock(t *testing.T) {
	table := prepareTable(t)
	ctx := context.Background()
	pk := dynago.StringValue("123")

	// Update method will add 100 to the current account balance
	update := func() error {
		var (
			acc LedgerAccount
		)
		err, _ := table.GetItem(ctx, pk, pk, &acc)
		if err != nil {
			return err
		}
		t.Log(acc)
		acc.Balance += 100
		return table.PutItem(ctx, pk, pk, acc, dynago.WithOptimisticLock("Version", acc.Version))
	}
	// Invoke Update in parallel 10 times to increment account balance
	// WithOptimisticLock should prevent concurrency overwriting issues
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				err := update()
				if err == nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	// We expect account balance to be 1000 after 10 update
	// If any update method overwrote with an outdated value then total balance will be less than 1000
	var acc LedgerAccount
	err, _ := table.GetItem(ctx, pk, pk, &acc)
	if err != nil {
		t.Fatalf("unexpected error %s", err)
		return
	}
	if acc.Balance != 1000 {
		t.Errorf("expected account balance to be 1000 after 10 increments of 100; got %d", acc.Balance)
	}
}
