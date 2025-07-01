package dynago

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type PutOption func(*dynamodb.PutItemInput) error

// Enables concurrency control by using an optimistic lock
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.OptimisticLocking.html
//
// Provide key field acts as a version number (Usually called Version)
// GetItem retrieves current version number and you can update the item if the version number in DynamoDB hasn't changed
// Each update increments the version number and if the update fails fetch the record again to get latest version number and try again
func WithOptimisticLock(key string, currentVersion uint) PutOption {
	return func(input *dynamodb.PutItemInput) error {
		// Ensure the condition expression is set to check if the version attribute does not exist or matches the old version
		condition := "attribute_not_exists(#version) or #version = :oldVersion"
		input.ConditionExpression = &condition
		if input.ExpressionAttributeNames == nil {
			input.ExpressionAttributeNames = map[string]string{}
		}
		if input.ExpressionAttributeValues == nil {
			input.ExpressionAttributeValues = map[string]Attribute{}
		}
		input.ExpressionAttributeNames["#version"] = key
		input.ExpressionAttributeValues[":oldVersion"] = NumberValue(int64(currentVersion))
		input.Item[key] = NumberValue(int64(currentVersion + 1))
		return nil
	}
}

/**
* Used to put and update a db record from dynamodb given a partition key and sort key
* @param item the item put into the database
 * @return true if the record was put, false otherwise
*/
func (t *Client) PutItem(ctx context.Context, pk, sk Attribute, item interface{}, opts ...PutOption) error {
	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		log.Println("Failed to Marshal item" + err.Error())
		return err
	}

	for k, v := range t.NewKeys(pk, sk) {
		av[k] = v
	}

	input := &dynamodb.PutItemInput{
		TableName: &t.TableName,
		Item:      av,
	}
	// Apply option functions
	if len(opts) > 0 {
		for _, opt := range opts {
			opt(input)
		}
	}

	_, err = t.client.PutItem(ctx, input)
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
