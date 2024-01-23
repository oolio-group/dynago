package dynago_test

import (
	"testing"

	"github.com/oolio-group/dynago"
)

func TestClient(t *testing.T) {
	// test if dynago.Client implements dynago.DynamoClient interface
	var client dynago.DynamoClient = &dynago.Client{}
	_, ok := client.(*dynago.Client)
	if !ok {
		t.Errorf("client does not implement DynamoClient interface")
	}
}
