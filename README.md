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

You can use any `io.Writer` implementation for output.

```go
rows, err := db.Query(...)
//...
defer rows.Close()
rw := sqljsonutil.NewRowsWriter(os.Stdout, rows)
err = rw.WriteCommaRows()
```

Output:
```
{"widget_id":"abc123","name":"First One"}
,{"widget_id":"def456","name":"Next One"}
```


### HTTP Response

Some additional utility is provided for sending HTTP responses:

```go
func (h *SomeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    
    //...

    rows, err := db.Query(...)
    //...
    defer rows.Close()
    err = sqljsonutil.NewRowsWriter(w, rows).WriteResponse()

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

You can also write a prefix and suffix to wrap the default HTTP as you like:

```go
func (h *SomeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    
    //...

    rows, err := db.Query(...)
    //...
    defer rows.Close()

    fmt.Fprint(w, `{"result":`)
    err = sqljsonutil.NewRowsWriter(w, rows).WriteResponse()
    //...
    fmt.Fprint(w, `,"another_field":"here"}`)

}
```

HTTP Response: (formatting added for clarity):
```
Content-Type: text/plain; charset=utf-8

{
    "result": [
        {"widget_id":"abc123","name":"First One"},
        {"widget_id":"def456","name":"Next One"}
    ],
    "another_field":"here"
}
```

### Streaming

You can use `WriteCommaRow` to write out each row separate and do work in between each record.  Note that this and other calls are designed to stream data one record at a time (unlike approaches that juse use json.Marshal on all rows at once, potentially using a lot of memory and delaying immediate output).

```go
func (h *SomeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    
    //...

    rows, err := db.Query(...)
    //...
    defer rows.Close()

    fmt.Fprint(w, `[`)
    rw := sqljsonutil.NewRowsWriter(w, rows)

    for rows.Next() {
        err := rw.WriteCommaRow()
        if err != nil {
            return err
        }
        // TODO: you can do more work here if needed
    }
    if err := rows.Err(); err != nil {
        //...
    }
    fmt.Fprint(w, `]`)

}
```

### Custom JSON Output

You can control how fields are converted to JSON by setting `JSONValueFunc`.  An example use case is to emit certain fields which contain JSON in them already as-is without string escaping:

```go
func (h *SomeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    
    //...

    rows, err := db.Query(...)
    //...
    defer rows.Close()
    rw := sqljsonutil.NewRowsWriter(w, rows)
    rw.JSONValueFunc = func(w io.Writer, colName string, colIndex int, value interface{}) (ok, skip bool, err error) {
        if colName == "data" {
            var buf bytes.Buffer
            buf.WriteString("null")
            switch v := value.(type) {
            case *sql.NullString:
                if v.Valid && json.Valid([]byte(v.String)) { // compiler should optimize this cast away in Go 1.22+
                    buf.Reset()
                    buf.WriteString(v.String)
                }
            default:
                return false, false, fmt.Errorf("unknown 'data' column type: %#T", value)
            }
            w.Write(buf.Bytes())
            return true, false, nil
        }
        return
    }

    err = rw.WriteResponse()
    //...

}
```

And then output has the values of the `data` field unescaped (e.g. the first data field in this case contained the string `{"description":"This is abc123, the first one."}`, which was output literally here instead of adding backslashes).

```
Content-Type: application/json

[
{"widget_id":"abc123","data":{"description":"This is abc123, the first one."}}
,{"widget_id":"def456","data":{"description":"This is def456, the next one."}}
]
```


### Skipping Fields

You can also use `JSONValueFunc` to skip outputting fields you don't want based on custom logic.  This will only write out `data` fields as JSON if they contain the string `first`, and otherwise skip outputting the `data` field:

```go
func (h *SomeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    
    //...

    rows, err := db.Query(...)
    //...
    defer rows.Close()
    rw := sqljsonutil.NewRowsWriter(w, rows)
    rw.JSONValueFunc = func(w io.Writer, colName string, colIndex int, value interface{}) (ok, skip bool, err error) {
        if colName == "data" {
            var buf bytes.Buffer
            buf.WriteString("null")
            switch v := value.(type) {
            case *sql.NullString:
                // only output data field values that contain the word "first"
                if v.Valid && strings.Contains(v.String, "first") && json.Valid([]byte(v.String)) { // compiler should optimize this cast away in Go 1.22+
                    buf.Reset()
                    buf.WriteString(v.String)
                    break
                }
                skip = true
                return
            default:
                return false, false, fmt.Errorf("unknown 'data' column type: %#T", value)
            }
            w.Write(buf.Bytes())
            return true, false, nil
        }
        return
    }

    err = rw.WriteResponse()
    //...

}
```

Output:
```
Content-Type: application/json

[
{"widget_id":"abc123","data":{"description":"This is abc123, the first one."}}
,{"widget_id":"def456"}
]
```


### Custom SQL Scanning

TODO: This still needs to be implemented.  Feel free to open an issue (or better yet, a pull request :) if you run into needing this.
