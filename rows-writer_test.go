package sqljsonutil

import (
	"database/sql"
	"net/http/httptest"
	"net/http/httputil"
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

	return db
}

func TestWrite(t *testing.T) {

	db := mustDbSetup(t)
	defer db.Close()

	rec := httptest.NewRecorder()

	rows, err := db.Query("SELECT * FROM widgets")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	t.Run("WriteResponse", func(t *testing.T) {
		rw := NewRowsWriter(rec)
		err = rw.WriteResponse(rec, rows)
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
