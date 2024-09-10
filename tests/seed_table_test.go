package tests

import (
	"context"
	"fmt"

	"github.com/oolio-group/dynago"
)

type tableRecord struct {
	Pk     string `dynamodbav:"pk"`
	Sk     string `dynamodbav:"sk"`
	Record interface{}
}

func seedRecords(ctx context.Context, table *dynago.Client, input []tableRecord) error {
	//insert items
	items := make([]*dynago.TransactPutItemsInput, 0, len(input))
	for _, item := range input {
		in := dynago.TransactPutItemsInput{
			PartitionKeyValue: dynago.StringValue(item.Pk),
			SortKeyValue:      dynago.StringValue(item.Sk),
			Item:              item,
		}

		items = append(items, &in)
	}

	err := table.TransactPutItems(ctx, items)
	if err != nil {
		return fmt.Errorf("failed to insert items; got %s", err)
	}

	return nil
}
