package dynago

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const ChunkSize = 25

type BatchPutItemsInput struct {
	PartitionKeyValue Attribute
	SortKeyValue      Attribute
	Item              any
}

/**
 * BatchPutItems writes multiple items to DynamoDB in batches.
 * Items are automatically chunked into groups of 25 (DynamoDB's batch limit).
 * Each item is marshaled and partition/sort keys are added before writing.
 * @param ctx context for the operation
 * @param inputs slice of items to put into DynamoDB
 * @return error if operation fails
 */
func (t *Client) BatchPutItems(ctx context.Context, inputs []BatchPutItemsInput) error {
	items := make([]types.WriteRequest, 0, len(inputs))

	for _, input := range inputs {
		item, err := attributevalue.MarshalMap(input.Item)
		table := t.TableName
		if err != nil {
			return fmt.Errorf("failed to marshall item; %s", err)
		}

		for k, v := range t.NewKeys(input.PartitionKeyValue, input.SortKeyValue) {
			item[k] = v
		}

		items = append(items, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})
	}
	chunkedItems := chunkBy(items, ChunkSize)
	for _, chunkedBatch := range chunkedItems {
		_, err := t.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				table: chunkedBatch,
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}
