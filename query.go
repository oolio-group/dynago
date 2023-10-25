package dynago

import (
	"context"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type QueryInput = dynamodb.QueryInput

// Fuction Struct for providing option input prams
type QueryOptions func(q *dynamodb.QueryInput)

// WithFields func to be passed as optional param function to select specfic fielld from the db enity
func WithFields(fields []string) QueryOptions {
	exp := aws.String(strings.Join(fields, ", "))
	return func(q *dynamodb.QueryInput) {
		q.ProjectionExpression = exp
	}
}

func WithFilter(filterCon string) QueryOptions {
	return func(q *dynamodb.QueryInput) {
		q.FilterExpression = aws.String(filterCon)
	}
}

func WithIndex(i string) QueryOptions {
	index := aws.String(i)
	return func(q *dynamodb.QueryInput) {
		q.IndexName = index
	}
}

// FIXME: WithDescOrder instead? SortByAsc(true) doesn't make sense as dynamodb sorts by asc (default)
func SortByAsc(v bool) QueryOptions {
	val := aws.Bool(v)
	return func(q *dynamodb.QueryInput) {
		q.ScanIndexForward = val
	}
}

func WithLimit(v int32) QueryOptions {
	val := aws.Int32(v)
	return func(q *dynamodb.QueryInput) {
		q.Limit = val
	}
}

func WithCursorKey(key map[string]Attribute) QueryOptions {
	return func(q *dynamodb.QueryInput) {
		q.ExclusiveStartKey = key
	}
}

// TODO: improve with generics and avoid the `out` argument
func (t *Client) Query(
	ctx context.Context,
	condition string, values map[string]Attribute, out interface{}, opts ...QueryOptions,
) (cursor map[string]Attribute, err error) {
	input := &dynamodb.QueryInput{
		TableName:                 &t.TableName,
		KeyConditionExpression:    aws.String(condition),
		ExpressionAttributeValues: values,
	}

	// when optional function param is provided
	if len(opts) > 0 {
		for _, opt := range opts {
			opt(input)
		}
	}

	// TODO: pre allocate 100 capacity? AttributeValue is an iterface; allocated capacity might be too small
	results := []map[string]Attribute{}
	var limit int32
	if input.Limit != nil {
		limit = *input.Limit
	}

	// dynamodb paginates by default when query result exceeds 1MB. ie pages of 1MB data
	for {
		resp, err := t.client.Query(ctx, input)
		if err != nil {
			log.Printf("dynamodb query %s failed; %s \n", condition, err)
			return nil, err
		}

		if resp.Items != nil {
			results = append(results, resp.Items...)
		}

		// Dynamodb will resume query scanning from this key
		input.ExclusiveStartKey = resp.LastEvaluatedKey

		if input.Limit != nil {
			// Stop paginating if we have retrieved what we want if a Limit option is used
			if len(results) >= int(limit) {
				break
			}
			// New limit = total fetched
			input.Limit = aws.Int32(limit - int32(len(results)))
		}

		// The only way to know when you have reached the end of the result set is when LastEvaluatedKey is empty.
		if resp.LastEvaluatedKey == nil || resp.Items == nil {
			break
		}
	}

	err = attributevalue.UnmarshalListOfMaps(results, &out)
	if err != nil {
		log.Println("dynamodb unmarshal failed" + err.Error())
		return nil, err
	}
	return input.ExclusiveStartKey, err
}
