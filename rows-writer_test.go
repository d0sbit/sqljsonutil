package sqljsonutil

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestIntro(t *testing.T) {
	const line = `docker run -d --rm --name sqljsonutil_rows_writer_test_mysql -eMYSQL_ROOT_PASSWORD=notasecurepassword -eMYSQL_DATABASE=sqljsonutil_test -p3456:3306 mysql:8.4.2`
	t.Logf("If you haven't already you'll want to launch mysql in a docker container, like so: %s", line)
}

// mustDbSetup sets up your db or bust, caller must close
func mustDbSetup(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("mysql", "root:notasecurepassword@tcp(127.0.0.1:3456)/sqljsonutil_test?charset=utf8mb4,utf8")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS widgets(widget_id VARCHAR(64), name VARCHAR(255))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("DELETE FROM widgets")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO widgets (widget_id,name) VALUES ('abc123','First One'), ('def456', 'Next One')")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS widgets_data (widget_id VARCHAR(64), data MEDIUMTEXT)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("DELETE FROM widgets_data")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO widgets_data (widget_id,data) VALUES ('abc123','{\"description\":\"This is abc123, the first one.\"}'), ('def456', '{\"description\":\"This is def456, the next one.\"}')")
	if err != nil {
		t.Fatal(err)
	}

	return db
}

func TestWrite(t *testing.T) {

	db := mustDbSetup(t)
	defer db.Close()

	t.Run("WriterOutput", func(t *testing.T) {

		rows, err := db.Query("SELECT * FROM widgets")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		rw := NewRowsWriter(os.Stdout, rows)
		err = rw.WriteCommaRows()
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("WriteResponse", func(t *testing.T) {

		rows, err := db.Query("SELECT * FROM widgets")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		rec := httptest.NewRecorder()

		rw := NewRowsWriter(rec, rows)
		err = rw.WriteResponse()
		if err != nil {
			t.Fatal(err)
		}

		res := rec.Result()
		resText, err := httputil.DumpResponse(res, true)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("RESPONSE: %s", resText)

	})

	t.Run("ResponsePrefixSuffix", func(t *testing.T) {

		rows, err := db.Query("SELECT * FROM widgets")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		rec := httptest.NewRecorder()

		fmt.Fprint(rec, `{"result":`)

		rw := NewRowsWriter(rec, rows)
		err = rw.WriteResponse()
		if err != nil {
			t.Fatal(err)
		}

		fmt.Fprint(rec, `,"another_field":"here"}`)

		res := rec.Result()
		resText, err := httputil.DumpResponse(res, true)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("RESPONSE: %s", resText)

	})

	t.Run("Streaming", func(t *testing.T) {

		rows, err := db.Query("SELECT * FROM widgets")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		rec := httptest.NewRecorder()

		fmt.Fprint(rec, `[`)

		rw := NewRowsWriter(rec, rows)
		for rows.Next() {
			err := rw.WriteCommaRow()
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}

		fmt.Fprint(rec, `]`)

		res := rec.Result()
		resText, err := httputil.DumpResponse(res, true)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("RESPONSE: %s", resText)

	})

	t.Run("CustomJSONOutput", func(t *testing.T) {

		rows, err := db.Query("SELECT * FROM widgets_data")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		rec := httptest.NewRecorder()

		rw := NewRowsWriter(rec, rows)
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
		if err != nil {
			t.Fatal(err)
		}

		res := rec.Result()

		resText, err := httputil.DumpResponse(res, true)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("RESPONSE: %s", resText)
	})

	t.Run("SkippingFields", func(t *testing.T) {

		rows, err := db.Query("SELECT * FROM widgets_data")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		rec := httptest.NewRecorder()

		rw := NewRowsWriter(rec, rows)
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
		if err != nil {
			t.Fatal(err)
		}

		res := rec.Result()

		resText, err := httputil.DumpResponse(res, true)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("RESPONSE: %s", resText)
	})
}
