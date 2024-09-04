package dynago

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

/**
* Used to delete a db record from dynamodb given a partition key and sort key
* @param pk the partition key of the record
* @param sk the sort key of the record
 * @return true if the record was deleted, false otherwise
*/
func (t *Client) DeleteItem(ctx context.Context, pk Attribute, sk Attribute) error {

	//delete item from dynamodb
	input := &dynamodb.DeleteItemInput{
		TableName: &t.TableName,
		Key: map[string]types.AttributeValue{
			"pk": pk,
			"sk": sk,
		},
	}
	_, err := t.client.DeleteItem(ctx, input)

	if err != nil {
		log.Println("failed to delete record from database. Error:" + err.Error())
		return err
	}

	return nil
}

type TransactDeleteItemsInput struct {
	PartitionKeyValue Attribute
	SortKeyValue      Attribute
}

// TODO: [low priority] The aggregate size of the items in the transaction cannot exceed 4 MB.
func (t *Client) TransactDeleteItems(ctx context.Context, inputs []*TransactDeleteItemsInput) error {
	requests := make([]types.TransactWriteItem, len(inputs))
	for idx, in := range inputs {
		requests[idx] = types.TransactWriteItem{
			Delete: &types.Delete{TableName: &t.TableName,
				Key: map[string]types.AttributeValue{
					"pk": in.PartitionKeyValue,
					"sk": in.SortKeyValue,
				}},
		}
	}

	_, err := t.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: requests,
	})
	return err
}
