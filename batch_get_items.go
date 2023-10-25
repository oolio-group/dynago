package dynago

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type AttributeRecord = map[string]Attribute

func chunkBy[T any](items []T, chunkSize int) (chunks [][]T) {
	for chunkSize < len(items) {
		items, chunks = items[chunkSize:], append(chunks, items[0:chunkSize:chunkSize])
	}
	return append(chunks, items)
}

func getBatchResult(ctx context.Context, t *Client, keys []AttributeRecord) ([]AttributeRecord, error) {
	var err error
	table := t.TableName
	items := make([]AttributeRecord, 0, len(keys))
	unprocessedKeys := keys
	hasMore := true
	for hasMore {
		input := &dynamodb.BatchGetItemInput{
			RequestItems: map[string]types.KeysAndAttributes{
				table: {
					Keys: unprocessedKeys,
				},
			},
		}
		res, err := t.client.BatchGetItem(ctx, input)
		if err != nil {
			return nil, err
		}

		items = append(items, res.Responses[table]...)
		if res.UnprocessedKeys != nil && len(res.UnprocessedKeys) > 0 {
			unprocessedKeys = res.UnprocessedKeys[table].Keys
		} else {
			hasMore = false
		}
	}
	return items, err
}

func (t *Client) BatchGetItems(ctx context.Context, input []AttributeRecord, out interface{}) (err error) {
	var items = make([]AttributeRecord, 0, len(input))
	var batches = chunkBy(input, 100)
	for _, batch := range batches {
		res, err := getBatchResult(ctx, t, batch)
		if err != nil {
			return err
		}
		items = append(items, res...)
	}
	err = attributevalue.UnmarshalListOfMaps(items, &out)
	if err != nil {
		return err
	}
	return nil
}
