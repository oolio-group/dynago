package dynago

import (
	"context"
	"log"

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

func (t *Client) TransactItems(ctx context.Context, input []types.TransactWriteItem) error {
	_, err := t.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: input,
	})
	return err
}
