package dynago

type Index struct {
	IndexName        string
	PartitionKeyName string
	SortKeyName      string
}

// Generate DynamoDB item key map for the given value
// Name of the keys were registered during the NewDynamoTable call
func (t *Client) NewKeys(pk Attribute, sk Attribute) map[string]Attribute {
	return map[string]Attribute{
		t.Keys["pk"]: pk,
		t.Keys["sk"]: sk,
	}
}
