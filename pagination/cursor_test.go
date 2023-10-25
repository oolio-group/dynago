package pagination_test

import (
	"github.com/oolio-group/dynago"
	"github.com/oolio-group/dynago/pagination"
	"reflect"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
)

type TestKey struct {
	Pk         string `dynamodbav:"pk"`
	Sk         string `dynamodbav:"sk"`
	Timestamp  int64  `dynamodbav:"timestamp"`
	Timeseries string `dynamodbav:"timePk"`
}

func TestEncode(t *testing.T) {
	cases := []struct {
		input       map[string]dynago.Attribute
		expected    string
		expectedErr bool
	}{
		{
			input: map[string]dynago.Attribute{
				"pk":        dynago.StringValue("some#value"),
				"sk":        dynago.StringValue("another_value"),
				"timePk":    dynago.StringValue("not_a-number"),
				"timestamp": dynago.NumberValue(999999999),
			},
			expected: "eyJQayI6InNvbWUjdmFsdWUiLCJTayI6ImFub3RoZXJfdmFsdWUiLCJUaW1lc3RhbXAiOjk5OTk5OTk5OSwiVGltZXNlcmllcyI6Im5vdF9hLW51bWJlciJ9Cg==",
		},
		{
			input: map[string]dynago.Attribute{
				"pk":        dynago.StringValue("some#value"),
				"sk":        dynago.StringValue("another_value"),
				"timestamp": dynago.StringValue("this will be omitted"),
			},
			expected:    "",
			expectedErr: true,
		},
		{
			input:    map[string]dynago.Attribute{},
			expected: "eyJQayI6IiIsIlNrIjoiIiwiVGltZXN0YW1wIjowLCJUaW1lc2VyaWVzIjoiIn0K",
		},
		{
			input:    nil,
			expected: "",
		},
	}

	for idx, tc := range cases {
		t.Run(strconv.Itoa(idx), func(t *testing.T) {
			got, err := pagination.Encode[TestKey](tc.input)
			if err != nil && !tc.expectedErr {
				t.Fatal(err)
			}
			if tc.expectedErr {
				return
			}
			if got != tc.expected {
				t.Errorf("expected %s got %s", tc.expected, got)
			}
		})
	}
}

func TestDecode(t *testing.T) {
	cases := []struct {
		input       string
		expected    TestKey
		expectedErr bool
	}{
		{
			expected: TestKey{
				Pk:         "some#value",
				Sk:         "another_value",
				Timeseries: "not_a-number",
				Timestamp:  999999999,
			},
			input: "eyJQayI6InNvbWUjdmFsdWUiLCJTayI6ImFub3RoZXJfdmFsdWUiLCJUaW1lc3RhbXAiOjk5OTk5OTk5OSwiVGltZXNlcmllcyI6Im5vdF9hLW51bWJlciJ9Cg==",
		},
		{
			input:    "",
			expected: TestKey{},
		},
	}

	for idx, tc := range cases {
		t.Run(strconv.Itoa(idx), func(t *testing.T) {
			attr, err := pagination.Decode[TestKey](tc.input)
			if err != nil && !tc.expectedErr {
				t.Fatal(err)
			}
			if tc.expectedErr {
				return
			}
			var got TestKey
			attributevalue.UnmarshalMap(attr, &got)
			if !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("expected %v got %v", tc.expected, got)
			}
		})
	}
}

func TestEncodeDecode(t *testing.T) {
	cases := []map[string]dynago.Attribute{
		{},
		{
			"pk":        dynago.StringValue("some#value"),
			"sk":        dynago.StringValue("another_value"),
			"timePk":    dynago.StringValue("not_a-number"),
			"timestamp": dynago.NumberValue(999999999),
		},
		{
			"sk":        dynago.StringValue("another_value"),
			"timePk":    dynago.StringValue("not_a-number"),
			"timestamp": dynago.NumberValue(999999999),
		},
		{
			"pk": dynago.StringValue("some#value"),
			"sk": dynago.StringValue("another_value"),
		},
	}

	for idx, tc := range cases {
		t.Run(strconv.Itoa(idx), func(t *testing.T) {
			enc, err := pagination.Encode[TestKey](tc)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf(enc)

			dec, err := pagination.Decode[TestKey](enc)
			if err != nil {
				t.Fatal(err)
			}

			var got TestKey
			var expected TestKey
			attributevalue.UnmarshalMap(tc, &expected)
			attributevalue.UnmarshalMap(dec, &got)
			if !reflect.DeepEqual(got, expected) {
				t.Errorf("expected %v got %v", expected, got)
			}
		})
	}
}
