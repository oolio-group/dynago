package dynago

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

/**
* Used to batch delete records from  dynamodb
* @param input slice of record want to  put to DB
* @return error
 */
func (t *Client) BatchDeleteItems(ctx context.Context, input []map[string]types.AttributeValue) []map[string]types.AttributeValue {
	items := make([]types.WriteRequest, 0, len(input))
	errorRequests := make([]types.WriteRequest, 0, len(input))
	failedItems := make([]map[string]types.AttributeValue, 0, len(input))
	table := t.TableName
	for _, model := range input {
		items = append(items,
			types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: model,
				},
			},
		)
	}
	chunkedItems := chunkBy(items, ChunkSize)
	for _, chunkedBatch := range chunkedItems {
		output, err := t.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				table: chunkedBatch,
			},
		})
		if err != nil {
			errorRequests = append(errorRequests, chunkedBatch...)
		} else {
			if len(output.UnprocessedItems) > 0 {
				unprocessedItems := output.UnprocessedItems[table]
				errorRequests = append(errorRequests, unprocessedItems...)
			}
		}
	}

	if len(errorRequests) > 0 {
		for _, failedReq := range errorRequests {
			failedItems = append(failedItems, failedReq.DeleteRequest.Key)
		}
	}

	return failedItems

}
