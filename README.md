# go-dynamodb

## Usage with default credentials on machine (Recommended for connecting to AWS)

```go
import (
  "github.com/oolio-group/dynago"
)

table, err := dynago.NewClient(ctx, dynago.ClientOptions{
  TableName:        "test-table",
  Region:           os.Getenv("AWS_REGION"),
  PartitionKeyName: "pk",
  SortKeyName:      "sk",
})
```

## Usage with local dynamodb

### Run dynago.locally

```sh
docker run -p  8000:8000 amazon/dynamodb-local
```

```go
import (
  "github.com/oolio-group/dynago"
)

table, err := dynago.NewClient(ctx, dynamite.ClientOptions{
  TableName: "test",
  Endpoint: &dynago.EndpointResolver{
    EndpointURL:     "http://localhost:8000",
    AccessKeyID:     "dummy",
    SecretAccessKey: "dummy",
  },
  PartitionKeyName: "pk",
  SortKeyName:      "sk",
  Region:           "us-east-1",
})
```

### Get item

```go
var jon Person
err, found := table.GetItem(ctx, dynago.StringValue("pk_jon"), dynago.StringValue("sk_jon"), &jon)
if err != nil {
  // connection or query error
  return err
}
if !found {
  // item does not exist
}

fmt.Println(jon)
```

### Batch Get Items

```go
var ids = [3]string{"1", "2", "3"}
var users []User
var items = make([]dynago.AttributeRecord, 0, len(ids))
for _, id := range ids {
  items = append(items, map[string]dynago.AttributeValue{
    "pk": dynago.StringValue("user#" + id),
    "sk": dynago.StringValue("user#" + id),
  })
}
err := table.BatchGetItems(ctx, items, &users)
if err != nil {
  // connection or query error
  return err
}
fmt.Println(users)
```

### Put Item

```go
event := Event{ Id: "one", Timestamp: time.Now().Unix() }
err := table.PutItem(
  context.TODO(),
  dynago.StringValue("event#"+event.id),
  dynago.NumberValue(event.Timestamp),
  &event
)
```

#### Optimistic locking with version number

> Optimistic locking is a strategy to ensure that the client-side item that you are updating (or deleting) is the same as the item in Amazon DynamoDB.
If you use this strategy, your database writes are protected from being overwritten by the writes of others, and vice versa.

Use the `WithOptimisticLock` option when calling `PutItem` method.

This works well and is recommended when using the event sourcing pattern where you need to update aggregate snapshots.
You can make use of event broker retry mechanism or retry libraries to simplify retry

**Example**

```go
type LedgerAccount struct {
	ID      string
	Balance int
	Version uint
}

func AddBalance(ctx context.Context, acc LedgerAccount, amount int) (err error) {
	var try int
	for try <= maxTries {
		var acc LedgerAccount
		err, _ := table.GetItem(ctx, pk, pk, &acc)
		if err != nil {
			return err
		}

		// Add amount to current account balance
		acc.Balance += amount

		// If another go routine updates account using AddBalance we want to avoid overwriting using an old balance
		err = table.PutItem(ctx, pk, pk, acc, dynago.WithOptimisticLock("Version", acc.Version))
		if err == nil {
			return nil
		}
		// Retry if there is an error with latest item value from DynamoDB
		try += 1
	}

	return err
}
```

### Query

```go
var peeps []Person
_, found := table.Query(ctx,"pk = :pk_val", map[string]dynago.Attribute{
  ":pk_val": dynago.StringValue("merchant#id"),
}, &peeps)
if err != nil {
  // connection or query error
  return err
}

fmt.Println(peeps)
```

### Query with options

**Fetch 10 items from gsi1 index sorted in descending order (using sk)**

```go
table.Query(ctx, "pk = :pk_val", map[string]dynago.Attribute{
  ":pk_val": dynago.StringValue("merchant#id"),
}, &out, dynago.WithIndex("gsi1"), dynago.SortByAsc(false), dynago.WithLimit(10))
```

### Query with pagination

**Fetch 10 items per page**

```go
// get first page
cursor, err := table.Query(ctx, "pk = :pk_val", map[string]dynago.Attribute{
  ":pk_val": dynago.StringValue("merchant#id"),
}, &out, dynago.WithLimit(10))

// get next page
if cursor != nil {
  cursor, err := table.Query(ctx, "pk = :pk_val", map[string]dynago.Attribute{
    ":pk_val": dynago.StringValue("merchant#id"),
  }, &out, dynago.WithLimit(10), dynago.WithCursorKey(cursor))
}
```

## Running Tets

By default, tests are run in offline mode. Using https://github.com/ory/dockertest, ephermal amazon/dynago.local containers are created for tests.

### Requirements

Docker must be installed and `docker` must be on `$PATH`

```sh
yarn test # runs go test ./...
```
