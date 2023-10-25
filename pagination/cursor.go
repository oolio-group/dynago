package pagination

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	dynamodb "github.com/oolio-group/dynago/v1"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
)

func Decode[Key any](encoded string) (map[string]dynamodb.Attribute, error) {
	if encoded == "" {
		return nil, nil
	}

	var dec Key
	if err := decodeFromBase64(&dec, encoded); err != nil {
		return nil, err
	}
	out, err := attributevalue.MarshalMap(&dec)
	return out, err
}

func Encode[Key any](attr map[string]dynamodb.Attribute) (string, error) {
	if attr == nil {
		return "", nil
	}

	var k Key
	err := attributevalue.UnmarshalMap(attr, &k)
	if err != nil {
		return "", err
	}

	enc, err := encodeToBase64(&k)
	if err != nil {
		return "", err
	}

	return enc, nil
}

func encodeToBase64(v interface{}) (string, error) {
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	err := json.NewEncoder(encoder).Encode(v)
	if err != nil {
		return "", err
	}
	encoder.Close()
	return buf.String(), nil
}

func decodeFromBase64(v interface{}, enc string) error {
	return json.NewDecoder(base64.NewDecoder(base64.StdEncoding, strings.NewReader(enc))).Decode(v)
}
