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

type Account struct {
	ID      string
	Balance int
	Version uint
	Status  string

	Pk string
	Sk string
}

func TestUpdateItem(t *testing.T) {
	table := prepareTable(t, dynamoEndpoint, "update_test")
	testCases := []struct {
		title       string
		item        Account
		updates     map[string]dynago.Attribute
		options     []dynago.UpdateOption
		expected    Account
		expectedErr error
	}{
		{
			title: "update fields success",
			item: Account{
				ID:      "1",
				Balance: 100,
				Status:  "active",
				Pk:      "account_1",
				Sk:      "account_1",
			},
			updates: map[string]dynago.Attribute{
				"Balance": dynago.NumberValue(200),
				"Status":  dynago.StringValue("inactive"),
			},
			options: []dynago.UpdateOption{},
			expected: Account{
				ID:      "1",
				Balance: 200,
				Status:  "inactive",
				Pk:      "account_1",
				Sk:      "account_1",
			},
		},
		{
			title: "optimistic lock success",
			item: Account{
				ID:      "2",
				Balance: 100,
				Version: 1,
				Pk:      "account_2",
				Sk:      "account_2",
			},
			updates: map[string]dynago.Attribute{
				"Balance": dynago.NumberValue(300),
			},
			options: []dynago.UpdateOption{
				dynago.WithOptimisticLockForUpdate("Version", 1),
			},
			expected: Account{
				ID:      "2",
				Balance: 300,
				Version: 2,
				Pk:      "account_2",
				Sk:      "account_2",
			},
		},
		{
			title: "conditional update success",
			item: Account{
				ID:      "3",
				Balance: 100,
				Status:  "active",
				Pk:      "account_3",
				Sk:      "account_3",
			},
			updates: map[string]dynago.Attribute{
				"Status": dynago.StringValue("inactive"),
			},
			options: []dynago.UpdateOption{
				dynago.WithConditionalUpdate("attribute_exists(Balance)"),
			},
			expected: Account{
				ID:      "3",
				Balance: 100,
				Status:  "inactive",
				Pk:      "account_3",
				Sk:      "account_3",
			},
		},
		{
			title: "conditional update failure",
			item: Account{
				ID:      "4",
				Balance: 100,
				Pk:      "account_4",
				Sk:      "account_4",
			},
			updates: map[string]dynago.Attribute{
				"Status": dynago.StringValue("inactive"),
			},
			options: []dynago.UpdateOption{
				dynago.WithConditionalUpdate("attribute_exists(NonExistentField)"),
			},
			expectedErr: fmt.Errorf("ConditionalCheckFailedException"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			t.Helper()
			ctx := context.TODO()

			pk := dynago.StringValue(tc.item.Pk)
			sk := dynago.StringValue(tc.item.Sk)
			err := table.PutItem(ctx, pk, sk, &tc.item)
			if err != nil {
				t.Fatalf("unexpected error on initial put: %s", err)
			}

			err = table.UpdateItem(ctx, pk, sk, tc.updates, tc.options...)
			if err != nil {
				if tc.expectedErr == nil {
					t.Fatalf("unexpected error: %s", err)
				}
				if !strings.Contains(err.Error(), tc.expectedErr.Error()) {
					t.Fatalf("expected op to fail with %s; got %s", tc.expectedErr, err)
				}
				return
			}

			var out Account
			err, found := table.GetItem(ctx, pk, sk, &out)
			if err != nil {
				t.Fatalf("unexpected error on get: %s", err)
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

func TestUpdateItemOptimisticLockConcurrency(t *testing.T) {
	table := prepareTable(t, dynamoEndpoint, "update_optimistic_test")
	account := Account{ID: "123", Balance: 0, Version: 0, Pk: "123", Sk: "123"}
	ctx := context.Background()
	pk := dynago.StringValue("123")
	err := table.PutItem(ctx, pk, pk, account)
	if err != nil {
		t.Fatalf("unexpected error %s", err)
		return
	}

	update := func() error {
		var acc Account
		err, _ := table.GetItem(ctx, pk, pk, &acc)
		if err != nil {
			return err
		}
		
		updates := map[string]dynago.Attribute{
			"Balance": dynago.NumberValue(int64(acc.Balance + 100)),
		}
		
		return table.UpdateItem(ctx, pk, pk, updates, dynago.WithOptimisticLockForUpdate("Version", acc.Version))
	}
	
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			maxRetries := 10
			for i := 0; i < maxRetries; i++ {
				err := update()
				if err == nil {
					return
				}
				if i%3 == 0 {
					t.Logf("Retry %d: %v", i, err)
				}
			}
			t.Logf("Max retries reached, continuing")
		}()
	}
	wg.Wait()
	
	var acc Account
	err, _ = table.GetItem(ctx, pk, pk, &acc)
	if err != nil {
		t.Fatalf("unexpected error %s", err)
		return
	}
	if acc.Balance != 1000 {
		t.Errorf("expected account balance to be 1000 after 10 increments of 100; got %d", acc.Balance)
	}
	if acc.Version != 10 {
		t.Errorf("expected account version to be 10 after 10 updates; got %d", acc.Version)
	}
}
