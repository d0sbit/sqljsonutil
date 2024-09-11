# sqljsonutil

Utilities for using JSON with SQL in Go.

## RowsWriter

A RowsWriter makes it simple to convert from sql.Rows to JSON output. A common use case is for an HTTP response.

RowsWriter is designed to be:

* **Easy to Use:** If all you need is to send back the result of a SQL query, you can do this with a single line of code.
* **Efficient:** Rows are streamed back one record at a time and JSON encoding is done using simple string conversion and reusing buffers - many of the common mistakes that waste memory and CPU are avoided.
* ***Flexible:*** Configurable and composable, gives you a fair amount of flexibility for such a specific utility.  See examples below.

## Examples

Here are some common use cases:

### Writer Output

```go
rows, err := db.Query(...)
//...
defer rows.Close()
rw := sqljsonutil.NewRowsWriter(os.Stdout)
rw.WriteCommaRows(rows)
```

Output:
```
{"widget_id":"abc123","name":"First One"}
,{"widget_id":"def456","name":"Next One"}
```


### HTTP Response

```go
func (h *SomeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    
    //...

    rows, err := db.Query(...)
    //...
    defer rows.Close()
    err = sqljsonutil.NewRowsWriter(w).WriteResponse(w, rows)

    //...

}
```

HTTP Response:
```
Content-Type: application/json

[
{"widget_id":"abc123","name":"First One"}
,{"widget_id":"def456","name":"Next One"}
]
```

### Response Prefix/Suffix

### Filtering Fields

### Embedded JSON

### Custom Conversion Logic

