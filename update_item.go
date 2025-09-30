package dynago

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type UpdateOption func(*dynamodb.UpdateItemInput) error

// WithConditionalUpdate enables conditional updates by setting a condition expression
func WithConditionalUpdate(conditionExpression string, values map[string]Attribute, names map[string]string) UpdateOption {
	return func(input *dynamodb.UpdateItemInput) error {
		input.ConditionExpression = &conditionExpression
		if input.ExpressionAttributeValues == nil {
			input.ExpressionAttributeValues = map[string]Attribute{}
		}
		for k, v := range values {
			input.ExpressionAttributeValues[k] = v
		}
		if names != nil {
			if input.ExpressionAttributeNames == nil {
				input.ExpressionAttributeNames = map[string]string{}
			}
			for k, v := range names {
				input.ExpressionAttributeNames[k] = v
			}
		}
		return nil
	}
}

// WithOptimisticLockForUpdate enables concurrency control by using an optimistic lock for updates
// Similar to PutItem's WithOptimisticLock but for UpdateItem operations
func WithOptimisticLockForUpdate(key string, currentVersion uint) UpdateOption {
	return func(input *dynamodb.UpdateItemInput) error {
		// Check if version attribute doesn't exist or matches the old version
		condition := "attribute_not_exists(#version) or #version = :oldVersion"
		input.ConditionExpression = &condition
		
		if input.ExpressionAttributeNames == nil {
			input.ExpressionAttributeNames = map[string]string{}
		}
		if input.ExpressionAttributeValues == nil {
			input.ExpressionAttributeValues = map[string]Attribute{}
		}
		
		input.ExpressionAttributeNames["#version"] = key
		input.ExpressionAttributeValues[":oldVersion"] = NumberValue(int64(currentVersion))
		input.ExpressionAttributeValues[":newVersion"] = NumberValue(int64(currentVersion + 1))
		
		// Add version increment to update expression
		versionUpdate := "#version = :newVersion"
		
		if input.UpdateExpression == nil || *input.UpdateExpression == "" {
			// If no existing expression, create a new SET expression
			expr := fmt.Sprintf("SET %s", versionUpdate)
			input.UpdateExpression = &expr
		} else {
			existingExpr := *input.UpdateExpression
			if strings.Contains(strings.ToUpper(existingExpr), "SET") {
				// Add to existing SET clause
				newExpr := strings.Replace(existingExpr, "SET ", fmt.Sprintf("SET %s, ", versionUpdate), 1)
				input.UpdateExpression = &newExpr
			} else {
				// Prepend SET clause to other operations
				newExpr := fmt.Sprintf("SET %s %s", versionUpdate, existingExpr)
				input.UpdateExpression = &newExpr
			}
		}
		
		return nil
	}
}

// WithUpdateExpression allows setting custom update expressions (e.g., "ADD balance :val")
func WithUpdateExpression(expression string, values map[string]Attribute, names map[string]string) UpdateOption {
	return func(input *dynamodb.UpdateItemInput) error {
		input.UpdateExpression = &expression
		
		if input.ExpressionAttributeValues == nil {
			input.ExpressionAttributeValues = map[string]Attribute{}
		}
		for k, v := range values {
			input.ExpressionAttributeValues[k] = v
		}
		
		if names != nil {
			if input.ExpressionAttributeNames == nil {
				input.ExpressionAttributeNames = map[string]string{}
			}
			for k, v := range names {
				input.ExpressionAttributeNames[k] = v
			}
		}
		
		return nil
	}
}

// UpdateItem updates specified fields on a DynamoDB record
// fields parameter should be a struct or map with fields to update
// If fields is nil, only custom expressions from options will be applied
func (t *Client) UpdateItem(ctx context.Context, pk, sk Attribute, fields interface{}, opts ...UpdateOption) error {
	var updateExpr string
	var attrValues map[string]Attribute
	var attrNames map[string]string
	var err error
	
	// Generate update expression from fields if provided
	if fields != nil {
		updateExpr, attrValues, attrNames, err = t.generateUpdateExpression(fields)
		if err != nil {
			return fmt.Errorf("failed to generate update expression: %w", err)
		}
	} else {
		// Initialize empty maps if no fields provided
		attrValues = make(map[string]Attribute)
		attrNames = make(map[string]string)
	}
	
	input := &dynamodb.UpdateItemInput{
		TableName:                 &t.TableName,
		Key:                       t.NewKeys(pk, sk),
		ExpressionAttributeValues: attrValues,
		ExpressionAttributeNames:  attrNames,
	}
	
	// Set update expression if we have one from fields
	if updateExpr != "" {
		input.UpdateExpression = &updateExpr
	}
	
	// Apply option functions
	for _, opt := range opts {
		if err := opt(input); err != nil {
			return fmt.Errorf("failed to apply update option: %w", err)
		}
	}
	
	// Check if we have any update expression after applying options
	if input.UpdateExpression == nil || *input.UpdateExpression == "" {
		return fmt.Errorf("no update expression provided")
	}
	
	_, err = t.client.UpdateItem(ctx, input)
	if err != nil {
		log.Printf("Failed to update item: %s", err.Error())
		return err
	}
	
	return nil
}

// generateUpdateExpression creates an update expression from a struct or map
func (t *Client) generateUpdateExpression(fields interface{}) (string, map[string]Attribute, map[string]string, error) {
	if fields == nil {
		return "", nil, nil, fmt.Errorf("fields cannot be nil")
	}
	
	// Marshal the fields to get attribute values
	av, err := attributevalue.MarshalMap(fields)
	if err != nil {
		return "", nil, nil, fmt.Errorf("failed to marshal fields: %w", err)
	}
	
	if len(av) == 0 {
		return "", nil, nil, fmt.Errorf("no fields to update")
	}
	
	var setParts []string
	attrValues := make(map[string]Attribute)
	attrNames := make(map[string]string)
	
	// Filter out partition and sort keys from updates
	pkName := t.Keys["pk"]
	skName := t.Keys["sk"]
	
	for fieldName, attrValue := range av {
		// Skip partition and sort keys
		if fieldName == pkName || fieldName == skName {
			continue
		}
		
		// Create attribute name and value placeholders
		nameKey := fmt.Sprintf("#%s", fieldName)
		valueKey := fmt.Sprintf(":%s", fieldName)
		
		attrNames[nameKey] = fieldName
		attrValues[valueKey] = attrValue
		setParts = append(setParts, fmt.Sprintf("%s = %s", nameKey, valueKey))
	}
	
	if len(setParts) == 0 {
		return "", nil, nil, fmt.Errorf("no valid fields to update (only primary keys provided)")
	}
	
	updateExpr := fmt.Sprintf("SET %s", strings.Join(setParts, ", "))
	
	return updateExpr, attrValues, attrNames, nil
}