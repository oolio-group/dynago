package dynago

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

/**
* Used to put and update a db record from dynamodb given a partition key and sort key
* @param item the item put into the database
 * @return true if the record was put, false otherwise
*/
func (t *Client) PutItem(ctx context.Context, pk Attribute, sk Attribute, item interface{}) error {
	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		log.Println("Failed to Marshal item" + err.Error())
		return err
	}

	for k, v := range t.NewKeys(pk, sk) {
		av[k] = v
	}

	_, err = t.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &t.TableName,
		Item:      av,
	})
	if err != nil {
		log.Println("Failed to Put item" + err.Error())
		return err
	}

	return nil
}

type TransactPutItemsInput struct {
	PartitionKeyValue Attribute
	SortKeyValue      Attribute
	Item              interface{}
}

// TransactWriteItems is a synchronous and idempotent write operation that groups up to 100 write actions in a single all-or-nothing operation.
// These actions can target up to 100 distinct items in one or more DynamoDB tables within the same AWS account and in the same Region.
// The aggregate size of the items in the transaction cannot exceed 4 MB.
// The actions are completed atomically so that either all of them succeed or none of them succeeds.
func (t *Client) TransactPutItems(ctx context.Context, inputs []*TransactPutItemsInput) error {
	requests := make([]types.TransactWriteItem, len(inputs))
	for idx, in := range inputs {
		item, err := attributevalue.MarshalMap(in.Item)
		if err != nil {
			return fmt.Errorf("failed to marshall item; %s", err)
		}

		// insert table partition key and sort key attribute value pairs
		for k, v := range t.NewKeys(in.PartitionKeyValue, in.SortKeyValue) {
			item[k] = v
		}
		requests[idx] = types.TransactWriteItem{
			Put: &types.Put{Item: item, TableName: &t.TableName},
		}
	}
	_, err := t.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: requests,
	})
	return err
}
