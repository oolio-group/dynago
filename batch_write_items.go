package dynago

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
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
* Used to update records to  dynamodb
* @param input slice of record want to  put to DB
* @return error
 */

func (t *Client) BatchWriteItems(ctx context.Context, input []map[string]types.AttributeValue) error {
	items := make([]types.WriteRequest, 0, len(input))
	table := t.TableName
	for _, model := range input {
		items = append(items,
			types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: model,
				},
			},
		)
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

func (t *Client) BatchPutItems(ctx context.Context, inputs []*BatchPutItemsInput) error {
	items := make([]map[string]types.AttributeValue, len(inputs))
	for idx, in := range inputs {
		item, err := attributevalue.MarshalMap(in.Item)
		if err != nil {
			return fmt.Errorf("failed to marshall item; %s", err)
		}

		for k, v := range t.NewKeys(in.PartitionKeyValue, in.SortKeyValue) {
			item[k] = v
		}
		items[idx] = item
	}

	return t.BatchWriteItems(ctx, items)
}
