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
docker run -p docker run -p 8000:8000 amazon/dynago.local
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
