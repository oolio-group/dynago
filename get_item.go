package dynago

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

/*
Used to get a db record from dynamodb given a partition key and sort key
@param partitionKey the partition key of the record
@param sortKey the sort key of the record
@param result the result of the query written to given memory reference
@return error, true if the record was found, false otherwise
*/
func (t *Client) GetItem(ctx context.Context, pk Attribute, sk Attribute, out interface{}) (err error, found bool) {
	input := &dynamodb.GetItemInput{
		TableName: &t.TableName,
		Key:       t.NewKeys(pk, sk),
	}

	resp, err := t.client.GetItem(ctx, input)
	if err != nil {
		// fixme: remove logs or log based on log level
		log.Println("failed to get record from database. Error:" + err.Error())
		return err, false
	}

	if resp.Item == nil {
		log.Printf("record not found %v %v\n", pk, sk)
		return nil, false
	}

	err = attributevalue.UnmarshalMap(resp.Item, &out)
	if err != nil {
		log.Println("unmarshal failed" + err.Error())
		return err, true
	}

	return nil, true
}
