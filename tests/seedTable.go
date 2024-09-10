package tests

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/oolio-group/dynago"
)

type tableRecord struct {
	Pk     string `dynamodbav:"pk"`
	Sk     string `dynamodbav:"sk"`
	Record interface{}
}

func seedRecords(ctx context.Context, table *dynago.Client, input []tableRecord) error {
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

	requests := make([]map[string]types.AttributeValue, 0, len(input))
	for _, item := range input {
		req := map[string]types.AttributeValue{
			"pk": dynago.StringValue(item.Pk),
			"sk": dynago.StringValue(item.Sk),
		}
		requests = append(requests, req)
	}

	var output []tableRecord
	err = table.BatchGetItems(ctx, requests, &output)
	if err != nil {
		return fmt.Errorf("failed to get items; got %s", err)
	}

	if len(input) != len(output) {
		return fmt.Errorf("count of items does not match input items; got %s", err)
	}

	return nil
}
