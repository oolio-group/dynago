package dynago

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// WithConsistentRead enables strongly consistent read for Query operations
// See documentation https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
// Note that the cost for strongly consistent reads are double of eventual consistent reads
func WithConsistentRead() QueryOptions {
	return func(q *dynamodb.QueryInput) {
		q.ConsistentRead = aws.Bool(true)
	}
}

// WithConsistentReadItem enables strongly consistent read for GetItem operations
// See documentation https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
// Note that the cost for strongly consistent reads are double of eventual consistent reads
func WithConsistentReadItem() GetItemOptions {
	return func(g *dynamodb.GetItemInput) {
		g.ConsistentRead = aws.Bool(true)
	}
}