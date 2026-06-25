package tests

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/oolio-group/dynago"
)

func TestUpdateItem(t *testing.T) {
	ddbClient := prepareTable(t)

	partitionKey := "org_123#" + rand.Text() // avoid interference with other tests that may be running in parallel

	testCases := []struct {
		name                          string
		givePk                        dynago.Attribute
		giveSk                        dynago.Attribute
		giveUpdateExpr                *string
		giveExpressionAttributeNames  map[string]*string
		giveExpressionAttributeValues map[string]ddbtypes.AttributeValue
		giveOptions                   []dynago.UpdateOption
		wantAttributes                map[string]ddbtypes.AttributeValue
		wantErrStr                    string
	}{
		{
			name:           "inserting new item and requesting ALL_OLD attributes returns no attributes",
			givePk:         dynago.StringValue(partitionKey),
			giveSk:         dynago.StringValue("2026-jan"),
			giveUpdateExpr: aws.String("SET Income = :v"),
			giveExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":v": &ddbtypes.AttributeValueMemberN{Value: "1000"},
			},
			giveOptions: []dynago.UpdateOption{
				dynago.WithReturnValues("ALL_OLD"),
			},
			wantAttributes: map[string]ddbtypes.AttributeValue{},
		},
		{
			name:           "inserting new item and requesting ALL_NEW attributes returns all attributes",
			givePk:         dynago.StringValue(partitionKey),
			giveSk:         dynago.StringValue("2026-feb"),
			giveUpdateExpr: aws.String("SET Income = :v"),
			giveExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":v": &ddbtypes.AttributeValueMemberN{Value: "1000"},
			},
			giveOptions: []dynago.UpdateOption{
				dynago.WithReturnValues("ALL_NEW"),
			},
			wantAttributes: map[string]ddbtypes.AttributeValue{
				"Income": &ddbtypes.AttributeValueMemberN{Value: "1000"},
				"pk":     &ddbtypes.AttributeValueMemberS{Value: partitionKey},
				"sk":     &ddbtypes.AttributeValueMemberS{Value: "2026-feb"},
			},
		},
		{
			name:           "updating existing item and requesting ALL_OLD attributes returns all attributes",
			givePk:         dynago.StringValue(partitionKey),
			giveSk:         dynago.StringValue("2026-jan"),
			giveUpdateExpr: aws.String("SET Income = :v"),
			giveExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":v": &ddbtypes.AttributeValueMemberN{Value: "2000"},
			},
			giveOptions: []dynago.UpdateOption{
				dynago.WithReturnValues("ALL_OLD"),
			},
			wantAttributes: map[string]ddbtypes.AttributeValue{
				"Income": &ddbtypes.AttributeValueMemberN{Value: "1000"}, // old value is returned
				"pk":     &ddbtypes.AttributeValueMemberS{Value: partitionKey},
				"sk":     &ddbtypes.AttributeValueMemberS{Value: "2026-jan"},
			},
		},
		{
			name:           "updating existing item and requesting ALL_NEW attributes returns new attributes",
			givePk:         dynago.StringValue(partitionKey),
			giveSk:         dynago.StringValue("2026-jan"),
			giveUpdateExpr: aws.String("SET Income = :v"),
			giveExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":v": &ddbtypes.AttributeValueMemberN{Value: "3000"},
			},
			giveOptions: []dynago.UpdateOption{
				dynago.WithReturnValues("ALL_NEW"),
			},
			wantAttributes: map[string]ddbtypes.AttributeValue{
				"Income": &ddbtypes.AttributeValueMemberN{Value: "3000"}, // new value is returned
				"pk":     &ddbtypes.AttributeValueMemberS{Value: partitionKey},
				"sk":     &ddbtypes.AttributeValueMemberS{Value: "2026-jan"},
			},
		},
		{
			name:           "incrementing non-existing item with ALL_NEW returns new attributes",
			givePk:         dynago.StringValue(partitionKey),
			giveSk:         dynago.StringValue("2026-mar"),
			giveUpdateExpr: aws.String("ADD Income :increment"),
			giveExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":increment": &ddbtypes.AttributeValueMemberN{Value: "8"},
			},
			giveOptions: []dynago.UpdateOption{
				dynago.WithReturnValues("ALL_NEW"),
			},
			wantAttributes: map[string]ddbtypes.AttributeValue{
				"Income": &ddbtypes.AttributeValueMemberN{Value: "8"},
				"pk":     &ddbtypes.AttributeValueMemberS{Value: partitionKey},
				"sk":     &ddbtypes.AttributeValueMemberS{Value: "2026-mar"},
			},
		},
		{
			name:           "incrementing existing item with ALL_NEW returns new attributes",
			givePk:         dynago.StringValue(partitionKey),
			giveSk:         dynago.StringValue("2026-mar"),
			giveUpdateExpr: aws.String("ADD Income :increment"),
			giveExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":increment": &ddbtypes.AttributeValueMemberN{Value: "8"},
			},
			giveOptions: []dynago.UpdateOption{
				dynago.WithReturnValues("ALL_NEW"),
			},
			wantAttributes: map[string]ddbtypes.AttributeValue{
				"Income": &ddbtypes.AttributeValueMemberN{Value: "16"},
				"pk":     &ddbtypes.AttributeValueMemberS{Value: partitionKey},
				"sk":     &ddbtypes.AttributeValueMemberS{Value: "2026-mar"},
			},
		},
		{
			name:           "increment missing item with condition expression causes an error",
			givePk:         dynago.StringValue(partitionKey),
			giveSk:         dynago.StringValue("2026-may"),
			giveUpdateExpr: aws.String("ADD Income :increment"),
			giveExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
				":increment": &ddbtypes.AttributeValueMemberN{Value: "8"},
			},
			giveOptions: []dynago.UpdateOption{
				dynago.WithConditionExpression("attribute_exists(pk) AND attribute_exists(sk)"), // want failure is the item does not exist
				dynago.WithReturnValues("ALL_NEW"),
			},
			wantAttributes: nil,
			wantErrStr:     "ConditionalCheckFailedException: The conditional request failed",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotResponse, gotErr := ddbClient.UpdateItem(context.TODO(), tc.givePk, tc.giveSk, tc.giveUpdateExpr, tc.giveExpressionAttributeValues, tc.giveOptions...)

			if tc.wantErrStr != "" {
				if gotErr == nil {
					t.Fatalf("expected error but got nil")
				}

				if !strings.Contains(gotErr.Error(), tc.wantErrStr) {
					t.Fatalf("error message does not contain expected substring: got %q, want %q", gotErr.Error(), tc.wantErrStr)
				}
			} else {
				if gotErr != nil {
					t.Fatalf("unexpected error: %v", gotErr)
				}

				if len(gotResponse.Attributes) != len(tc.wantAttributes) {
					t.Fatalf("number of attributes does not match: got %d, want %d", len(gotResponse.Attributes), len(tc.wantAttributes))
				}

				for key, value := range tc.wantAttributes {
					gotResponseValue := gotResponse.Attributes[key]
					if gotResponseValue == nil {
						t.Errorf("attribute %q not found in response", key)
					}

					switch v := gotResponseValue.(type) {
					case *ddbtypes.AttributeValueMemberN: // number
						if v.Value != value.(*ddbtypes.AttributeValueMemberN).Value {
							t.Errorf("attribute %q does not match: got %q, want %q", key, v.Value, value.(*ddbtypes.AttributeValueMemberN).Value)
						}
					case *ddbtypes.AttributeValueMemberS: // string
						if v.Value != value.(*ddbtypes.AttributeValueMemberS).Value {
							t.Errorf("attribute %q does not match: got %q, want %q", key, v.Value, value.(*ddbtypes.AttributeValueMemberS).Value)
						}
					default:
						t.Errorf("unsupported attribute value type for key %q: %T", key, gotResponseValue)
					}
				}
			}
		})
	}
}
