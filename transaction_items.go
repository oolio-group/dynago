package dynago

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (t *Client) WithDeleteItem(pk string, sk string) types.TransactWriteItem {
	return types.TransactWriteItem{
		Delete: &types.Delete{
			TableName: &t.TableName,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: pk},
				"sk": &types.AttributeValueMemberS{Value: sk},
			},
		},
	}

}

func (t *Client) WithPutItem(pk string, sk string, item interface{}) types.TransactWriteItem {
	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		log.Println("Failed to Marshal item" + err.Error())
		return types.TransactWriteItem{}
	}
	keys := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: pk},
		"sk": &types.AttributeValueMemberS{Value: sk},
	}
	for k, v := range keys {
		av[k] = v
	}
	return types.TransactWriteItem{
		Put: &types.Put{
			TableName: &t.TableName,
			Item:      av,
		},
	}

}

// TransactItems is a synchronous for writing or deletion operation performed in dynamodb grouped together

func (t *Client) TransactItems(ctx context.Context, input ...types.TransactWriteItem) error {
	_, err := t.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: input,
	})
	return err
}

func (t *Client) WithUpdateItem(pk string, sk string, updates map[string]Attribute, opts ...UpdateOption) types.TransactWriteItem {
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
		Key:                       map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
			"sk": &types.AttributeValueMemberS{Value: sk},
		},
		UpdateExpression:          &updateExpression,
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
	}

	for _, opt := range opts {
		err := opt(input)
		if err != nil {
			return types.TransactWriteItem{}
		}
	}

	return types.TransactWriteItem{
		Update: &types.Update{
			TableName:                 input.TableName,
			Key:                       input.Key,
			UpdateExpression:          input.UpdateExpression,
			ConditionExpression:       input.ConditionExpression,
			ExpressionAttributeNames:  input.ExpressionAttributeNames,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
		},
	}
}
