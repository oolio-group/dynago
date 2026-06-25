package dynago

import (
	"context"
	//"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type UpdateOption func(*dynamodb.UpdateItemInput) error

func WithReturnValues(returnValues string) UpdateOption {
	return func(input *dynamodb.UpdateItemInput) error {
		input.ReturnValues = dynamodbTypes.ReturnValue(returnValues)
		return nil
	}
}

func WithConditionExpression(conditionExpression string) UpdateOption {
	return func(input *dynamodb.UpdateItemInput) error {
		input.ConditionExpression = &conditionExpression
		return nil
	}
}

func WithReturnConsumedCapacity(returnConsumedCapacity string) UpdateOption {
	return func(input *dynamodb.UpdateItemInput) error {
		input.ReturnConsumedCapacity = dynamodbTypes.ReturnConsumedCapacity(returnConsumedCapacity)
		return nil
	}
}

func WithReturnItemCollectionMetrics(returnItemCollectionMetrics string) UpdateOption {
	return func(input *dynamodb.UpdateItemInput) error {
		input.ReturnItemCollectionMetrics = dynamodbTypes.ReturnItemCollectionMetrics(returnItemCollectionMetrics)
		return nil
	}
}

func WithReturnValuesOnConditionCheckFailure(returnValuesOnConditionCheckFailure string) UpdateOption {
	return func(input *dynamodb.UpdateItemInput) error {
		input.ReturnValuesOnConditionCheckFailure = dynamodbTypes.ReturnValuesOnConditionCheckFailure(returnValuesOnConditionCheckFailure)
		return nil
	}
}

// UpdateItem updates a db record from dynamodb given a partition key and sort key
// @param item the item put into the database
// @return true if the record was updated, false otherwise
func (t *Client) UpdateItem(
	ctx context.Context,
	pk Attribute,
	sk Attribute,
	updateExpression *string,
	expressionAttributeValues map[string]dynamodbTypes.AttributeValue,
	opts ...UpdateOption,
) (*dynamodb.UpdateItemOutput, error) {
	input := &dynamodb.UpdateItemInput{
		TableName:        &t.TableName,
		Key:              t.NewKeys(pk, sk),
		UpdateExpression: updateExpression,
	}

	if len(expressionAttributeValues) > 0 {
		input.ExpressionAttributeValues = expressionAttributeValues
	}

	// Apply option functions
	if len(opts) > 0 {
		for _, opt := range opts {
			opt(input)
		}
	}

	ret, err := t.client.UpdateItem(ctx, input)
	if err != nil {
		log.Println("Failed to Update item" + err.Error())
		return nil, err
	}

	return ret, nil
}
