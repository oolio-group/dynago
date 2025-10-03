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
	PutItem(ctx context.Context, pk, sk Attribute, item interface{}, opt ...PutOption) error
	DeleteItem(ctx context.Context, pk, sk string) error
	BatchDeleteItems(ctx context.Context, input []AttributeRecord) []AttributeRecord
}

type TransactionAPI interface {
	TransactPutItems(ctx context.Context, items []*TransactPutItemsInput) error
	TransactItems(ctx context.Context, input ...types.TransactWriteItem) error
}

type ReadAPI interface {
	GetItem(ctx context.Context, pk, sk Attribute, out interface{}, opts ...GetItemOptions) (error, bool)
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
