package dynago

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Attribute = types.AttributeValue

func StringValue(v string) *types.AttributeValueMemberS {
	return &types.AttributeValueMemberS{Value: v}
}

func NumberValue(v int64) *types.AttributeValueMemberN {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

func BoolValue(v bool) *types.AttributeValueMemberBOOL {
	return &types.AttributeValueMemberBOOL{Value: v}
}

type WriteAPI interface {
	// Create or update given item in DynamoDB. Must implemenmt DynamoRecord interface.
	// DynamoRecord.GetKeys will be called to get values for parition and sort keys.
	PutItem(ctx context.Context, pk Attribute, sk Attribute, item interface{}) error
}

type TransactionAPI interface {
	TransactPutItems(ctx context.Context, items []*TransactPutItemsInput) error
}

type ReadAPI interface {
	GetItem(ctx context.Context, pk Attribute, sk Attribute, out interface{}) (error, bool)
	BatchGetItems(ctx context.Context, input []AttributeRecord, out interface{}) error
}

type QueryAPI interface {
	// Perform a DynamoDB query with the key condition expression in first argument.
	//
	// If key condition contains template params eg: pk = :pk for values, second argument should provide values
	Query(ctx context.Context, condition string, params map[string]Attribute, out interface{}, opts ...QueryOptions) (map[string]Attribute, error)
}

type DynamoClient interface {
	ReadAPI
	WriteAPI
	QueryAPI
	TransactionAPI
}
