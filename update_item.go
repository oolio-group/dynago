package dynago

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type UpdateOption func(*dynamodb.UpdateItemInput) error

func WithOptimisticLockForUpdate(key string, currentVersion uint) UpdateOption {
	return func(input *dynamodb.UpdateItemInput) error {
		condition := "#version = :oldVersion"
		input.ConditionExpression = &condition
		if input.ExpressionAttributeNames == nil {
			input.ExpressionAttributeNames = map[string]string{}
		}
		if input.ExpressionAttributeValues == nil {
			input.ExpressionAttributeValues = map[string]Attribute{}
		}
		input.ExpressionAttributeNames["#version"] = key
		input.ExpressionAttributeValues[":oldVersion"] = NumberValue(int64(currentVersion))

		if input.UpdateExpression != nil {
			versionUpdate := fmt.Sprintf("%s, %s = :newVersion", *input.UpdateExpression, key)
			input.UpdateExpression = &versionUpdate
		} else {
			versionUpdate := fmt.Sprintf("SET %s = :newVersion", key)
			input.UpdateExpression = &versionUpdate
		}
		input.ExpressionAttributeValues[":newVersion"] = NumberValue(int64(currentVersion + 1))
		return nil
	}
}

func WithConditionalUpdate(conditionExpr string) UpdateOption {
	return func(input *dynamodb.UpdateItemInput) error {
		input.ConditionExpression = &conditionExpr
		return nil
	}
}

func WithReturnValues(returnValue types.ReturnValue) UpdateOption {
	return func(input *dynamodb.UpdateItemInput) error {
		input.ReturnValues = returnValue
		return nil
	}
}

func (t *Client) UpdateItem(ctx context.Context, pk, sk Attribute, updates map[string]Attribute, opts ...UpdateOption) error {
	var setExpressions []string
	expressionAttributeNames := make(map[string]string)
	expressionAttributeValues := make(map[string]Attribute)

	for key, value := range updates {
		attrName := fmt.Sprintf("#%s", key)
		attrValue := fmt.Sprintf(":%s", key)
		
		setExpressions = append(setExpressions, fmt.Sprintf("%s = %s", attrName, attrValue))
		expressionAttributeNames[attrName] = key
		expressionAttributeValues[attrValue] = value
	}

	updateExpression := fmt.Sprintf("SET %s", strings.Join(setExpressions, ", "))

	input := &dynamodb.UpdateItemInput{
		TableName:                 &t.TableName,
		Key:                       t.NewKeys(pk, sk),
		UpdateExpression:          &updateExpression,
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
	}

	if len(opts) > 0 {
		for _, opt := range opts {
			err := opt(input)
			if err != nil {
				return err
			}
		}
	}

	_, err := t.client.UpdateItem(ctx, input)
	if err != nil {
		log.Println("Failed to update item: " + err.Error())
		return err
	}

	return nil
}

type TransactUpdateItemsInput struct {
	PartitionKeyValue Attribute
	SortKeyValue      Attribute
	Updates           map[string]Attribute
	Options           []UpdateOption
}
