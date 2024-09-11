package sqljsonutil

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// FIXME:
// look for field-specific hacks and _json prefix and either remove or make them options on the writer

// RowsWriter takes care of writing a sql.Rows to a stream as JSON using the same field
// names that came from the SQL result set.  Each line is output as a JSON object {...} with
// commas separating each field.  Output a [ before and ] after to make a valid JSON array.
type RowsWriter struct {
	Writer io.Writer

	colNames          []string
	scanArgs          []interface{}
	rowOutBuf         bytes.Buffer
	rowOutEnc         *json.Encoder
	valOutBuf         bytes.Buffer
	valOutBytes       []byte
	jsonFieldSuffixes []string
}

// NewRowsWriter is the same as: return &RowsWriter{Writer: w}
func NewRowsWriter(w io.Writer) *RowsWriter {
	return &RowsWriter{Writer: w}
}

func stringNeedsJSONEsc(s string) bool {
	for _, c := range s {
		if c < 0x20 || c > 0x7f || c == '"' || c == '\\' {
			return true
		}
	}
	return false
}

func (rw *RowsWriter) trimnl() {
	for l := rw.rowOutBuf.Len() - 1; l >= 0 && rw.rowOutBuf.Bytes()[l] == '\n'; l-- {
		rw.rowOutBuf.Truncate(l)
	}
}

func (rw *RowsWriter) writeRawJSONValue(v interface{}) error {

	defer rw.trimnl()

	rw.valOutBuf.Reset()
	rowOut := &rw.rowOutBuf

	// log.Printf("v = %#v", v)

	// for specific cases we can do a lot faster than json.Encoder
	switch vt := v.(type) {

	case string:

		rowOut.WriteString(vt)
		return nil

	case *string:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		rowOut.WriteString(*vt)
		return nil

	case *sql.NullString:
		if vt == nil || !vt.Valid {
			rowOut.WriteString("null")
			return nil
		}
		rowOut.WriteString(vt.String)
		return nil

	case *[]byte:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		vts := unsafeString(*vt)
		rowOut.WriteString(vts)
		return nil

	case *sql.RawBytes:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		vts := unsafeString(*vt)
		if vts == "" {
			rowOut.WriteString("[]")
		}
		rowOut.WriteString(vts)
		return nil

	}

	panic(fmt.Errorf("unknown type for writeRawJSONValue %T: %#v", v, v))

}

func (rw *RowsWriter) writeValue(v interface{}) error {

	defer rw.trimnl()

	rw.valOutBuf.Reset()
	vob := rw.valOutBytes[:0]
	defer func() {
		if vob != nil {
			rw.valOutBytes = vob
		}
	}()
	rowOut := &rw.rowOutBuf

	// log.Printf("v = %#v", v)

	// for specific cases we can do a lot faster than json.Encoder
	switch vt := v.(type) {

	case string:
		if stringNeedsJSONEsc(vt) {
			return rw.rowOutEnc.Encode(vt)
		}
		rowOut.WriteByte('"')
		rowOut.WriteString(vt)
		rowOut.WriteByte('"')
		return nil

	case *string:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		if stringNeedsJSONEsc(*vt) {
			return rw.rowOutEnc.Encode(*vt)
		}
		rowOut.WriteByte('"')
		rowOut.WriteString(*vt)
		rowOut.WriteByte('"')
		return nil

	case *sql.NullString:
		if vt == nil || !vt.Valid {
			rowOut.WriteString("null")
			return nil
		}
		if stringNeedsJSONEsc(vt.String) {
			return rw.rowOutEnc.Encode(vt.String)
		}
		rowOut.WriteByte('"')
		rowOut.WriteString(vt.String)
		rowOut.WriteByte('"')
		return nil

	case *[]byte:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		vts := unsafeString(*vt)
		if stringNeedsJSONEsc(vts) {
			return rw.rowOutEnc.Encode(vts)
		}
		rowOut.WriteByte('"')
		rowOut.WriteString(vts)
		rowOut.WriteByte('"')
		return nil

	case *sql.RawBytes:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		vts := unsafeString(*vt)
		if stringNeedsJSONEsc(vts) {
			return rw.rowOutEnc.Encode(vts)
		}
		rowOut.WriteByte('"')
		rowOut.WriteString(vts)
		rowOut.WriteByte('"')
		return nil

	case *int:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		vob = strconv.AppendInt(vob, int64(*vt), 10)
		rowOut.Write(vob)
		return nil

	case *int32:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		vob = strconv.AppendInt(vob, int64(*vt), 10)
		rowOut.Write(vob)
		return nil
	case *int8:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		vob = strconv.AppendInt(vob, int64(*vt), 10)
		rowOut.Write(vob)
		return nil

	case *sql.NullInt32:
		if vt == nil || !vt.Valid {
			rowOut.WriteString("null")
			return nil
		}
		vob = strconv.AppendInt(vob, int64(vt.Int32), 10)
		rowOut.Write(vob)
		return nil

	case *int64:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		vob = strconv.AppendInt(vob, int64(*vt), 10)
		rowOut.Write(vob)
		return nil

	case *sql.NullInt64:
		if vt == nil || !vt.Valid {
			rowOut.WriteString("null")
			return nil
		}
		vob = strconv.AppendInt(vob, int64(vt.Int64), 10)
		rowOut.Write(vob)
		return nil

	case *uint:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		vob = strconv.AppendUint(vob, uint64(*vt), 10)
		rowOut.Write(vob)
		return nil

	case *uint32:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		vob = strconv.AppendUint(vob, uint64(*vt), 10)
		rowOut.Write(vob)
		return nil

	case *uint64:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		vob = strconv.AppendUint(vob, uint64(*vt), 10)
		rowOut.Write(vob)
		return nil

	case *bool:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		vob = strconv.AppendBool(vob, *vt)
		rowOut.Write(vob)
		return nil

	case *sql.NullBool:
		if vt == nil || !vt.Valid {
			rowOut.WriteString("null")
			return nil
		}
		vob = strconv.AppendBool(vob, vt.Bool)
		rowOut.Write(vob)
		return nil

	case *float32:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		vob = strconv.AppendFloat(vob, float64(*vt), 'f', -1, 32)
		rowOut.Write(vob)
		return nil

	case *float64:
		if vt == nil {
			rowOut.WriteString("null")
			return nil
		}
		vob = strconv.AppendFloat(vob, float64(*vt), 'f', -1, 64)
		rowOut.Write(vob)
		return nil

	case *sql.NullFloat64:
		if vt == nil || !vt.Valid {
			rowOut.WriteString("null")
			return nil
		}
		vob = strconv.AppendFloat(vob, float64(vt.Float64), 'f', -1, 64)
		rowOut.Write(vob)
		return nil

	case *sql.NullTime:
		if vt == nil || !vt.Valid {
			rowOut.WriteString("null")
			return nil
		}
		vob = vt.Time.AppendFormat(vob, time.RFC3339Nano)
		rowOut.WriteByte('"')
		rowOut.Write(vob)
		rowOut.WriteByte('"')
		return nil

		// case *mysql.NullTime:
		// 	if vt == nil || !vt.Valid {
		// 		rowOut.WriteString("null")
		// 		return nil
		// 	}
		// 	vob = vt.Time.AppendFormat(vob, time.RFC3339Nano)
		// 	rowOut.WriteByte('"')
		// 	rowOut.Write(vob)
		// 	rowOut.WriteByte('"')
		// 	return nil

		// TODO:
		// *time.Time ?

	}

	return fmt.Errorf("unknown type for writeValue %T: %#v", v, v)

	// // anything that falls through we just use json.Encoder
	// err := rw.rowOutEnc.Encode(v)
	// rw.trimnl()
	// return err

}

// WriteResponse writes rows as a full response of a JSON array and objects for each row.
// It will iterate through rows until the end of the result set.
func (rw *RowsWriter) WriteResponse(w http.ResponseWriter, rows *sql.Rows) error {
	if w.Header().Get("Content-Type") == "" { // set content type the first time
		w.Header().Set("Content-Type", "application/json")
	}
	fmt.Fprintln(w, "[")

	for rows.Next() {
		err := rw.WriteCommaRow(rows)
		if err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	fmt.Fprintln(w, "]")

	return nil
}

// // WriteResponseObj writes the first sql row response object.
// // It will ignore any other rows returned
// func (rw *RowsWriter) WriteResponseObj(w http.ResponseWriter, rows *sql.Rows) error {

// 	w.Header().Set("Content-Type", "application/json")

// 	for rows.Next() {

// 		err := rw.WriteRow(rows)
// 		if err != nil {
// 			return err
// 		}
// 		break
// 	}
// 	if err := rows.Err(); err != nil {
// 		return err
// 	}

// 	return nil

// }

// WriteCommaRow is like WriteRow but will prepend a comma before every row except the first.
// Suitable for writing out multiple rows in an JSON array.
// The same rows object must be passed each time, i.e. do not reuse an instance of this object for
// multiple result sets.
func (rw *RowsWriter) WriteCommaRow(rows *sql.Rows) error {

	err := rw.scanRowArgs(rows, true)
	if err != nil {
		return err
	}

	rw.rowOutBuf.WriteByte('{')

	err = rw.writeRowFields()
	if err != nil {
		return err
	}

	rw.rowOutBuf.WriteString("}\n")

	_, err = rw.rowOutBuf.WriteTo(rw.Writer)
	return err
}

// WriteRow will call rows.Scan with the appropriate arguments and write the result as a JSON object.
// The same rows object must be passed each time, i.e. do not reuse an instance of this object for
// multiple result sets.
func (rw *RowsWriter) WriteRow(rows *sql.Rows) error {

	err := rw.scanRowArgs(rows, false)
	if err != nil {
		return err
	}

	rw.rowOutBuf.WriteByte('{')

	err = rw.writeRowFields()
	if err != nil {
		return err
	}

	rw.rowOutBuf.WriteString("}\n")

	_, err = rw.rowOutBuf.WriteTo(rw.Writer)
	return err
}

// WriteCommaRows calls WriteRow in a loop and adds a comma in between each.
// Surround with `[`...`]` to form valid JSON.
func (rw *RowsWriter) WriteCommaRows(rows *sql.Rows) error {

	for rows.Next() {
		err := rw.WriteCommaRow(rows)
		if err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

// writeRowFields will write the object fields to rowOutBuf without flushing it
func (rw *RowsWriter) writeRowFields() error {

	// output each column as JSON object entry, fast paths for specific cases
	doneFirstCol := false
colloop:
	for i := range rw.colNames {

		if doneFirstCol {
			rw.rowOutBuf.WriteByte(',')
		}
		doneFirstCol = true

		rw.writeValue(rw.colNames[i])
		rw.rowOutBuf.WriteByte(':')

		// json fields are output raw
		if rw.jsonFieldSuffixes == nil && strings.HasSuffix(rw.colNames[i], "_json") {
			rw.writeRawJSONValue(rw.scanArgs[i])
			continue colloop
		} else if rw.jsonFieldSuffixes != nil {
			for _, suf := range rw.jsonFieldSuffixes {
				if strings.HasSuffix(rw.colNames[i], suf) {
					rw.writeRawJSONValue(rw.scanArgs[i])
					continue colloop
				}
			}
		}
		// otherwise use writeValue
		rw.writeValue(rw.scanArgs[i])

		// if strings.HasSuffix(rw.colNames[i], "_json") {
		// 	rw.writeRawJSONValue(rw.scanArgs[i])
		// } else {
		// 	// otherwise use writeValue
		// 	rw.writeValue(rw.scanArgs[i])
		// }

	}

	return nil
}

func (rw *RowsWriter) scanRowArgs(rows *sql.Rows, comma bool) error {

	// the first time we set up the stuff we need for scanning each row
	if rw.colNames == nil {

		colNames, err := rows.Columns()
		if err != nil {
			return err
		}
		rw.colNames = colNames

		colTypes, err := rows.ColumnTypes()
		if err != nil {
			return err
		}
		// rw.colTypes = colTypes

		scanArgs := make([]interface{}, len(colTypes))
		for i, ct := range colTypes {

			//log.Printf("coltype: %v scanArg: %v", ct, scanArgs[i])

			// HACK: so this is unfortunate but the MySQL driver does not send back "UNSIGNED" for unsigned ints.
			// This is a problem for specific fields that use the full range of a uint64. Thus we just
			// hack it by name.

			switch colNames[i] {
			case "asin_hash", "sku_hash", "created_at": //, "change_time":
				// use strings, since uint64 is not representable in JSON
				scanArgs[i] = new(string)
			// case "change_time":
			// 	scanArgs[i] = new(Timestamp)
			default:

				scanType := ct.ScanType()
				// if colNames[i] == "updated_at" {
				// 	log.Printf("ct.DatabaseTypeName: %q scanType.String(): %q", ct.DatabaseTypeName(), scanType.String())
				// }

				// 20230608 - TIMESTAMP sql type is not supported
				// Error: sql: Scan error on column index 1, name "change_time": unsupported Scan, storing driver.Value type []uint8 into type *time.Time
				// 2023/06/08 15:43:59 coltype: &{change_time true false true false 0 TIMESTAMP 0 0 0x17dc000} scanArg: <nil>
				//scanType: sql.NullTime

				if (ct.DatabaseTypeName() == "TIMESTAMP") && scanType.String() == "sql.NullTime" {
					scanArgs[i] = new(string)
				} else if strings.HasSuffix(colNames[i], "_id") || ((ct.DatabaseTypeName() == "DATETIME") && scanType.String() == "sql.NullTime") {
					// anything that ends with "_id" we assume is a uint64 that needs to be made a string
					scanArgs[i] = new(sql.NullString)
					// } else if scanType.ConvertibleTo(reflect.TypeOf(sql.NullTime{})) {
					// use *sql.NullTime, since the mysql driver is returning a *mysql.NullTime - so lame
					// scanArgs[i] = &sql.NullTime{}
				} else {
					// allocate and get pointer using whatever the database has
					scanArgs[i] = reflect.New(scanType).Interface()
				}

			}

			// log.Printf("col %q: %v; %v", colNames[i], ct.ScanType(), ct.DatabaseTypeName())

		}
		rw.scanArgs = scanArgs

		rw.rowOutBuf.Grow(1024)
		rw.rowOutEnc = json.NewEncoder(&rw.rowOutBuf)

	} else {

		// reset row buffer and write a comma to separate from prior row
		rw.rowOutBuf.Reset()
		if comma {
			rw.rowOutBuf.WriteByte(',')
		}
	}

	// scan row data
	err := rows.Scan(rw.scanArgs...)
	if err != nil {

		// log.Printf("error scanning args: %v", err)
		return err
	}

	return nil
}

// TODO: not sure if this belongs on RowsWriter, wait until a more specific case where it's needed arises
// // ScanRowMap calls rows.Scan with the appropriate types and returns the data as a map.
// // Individual fields can then be output with OutputFields and related functions.
// func (rw *RowsWriter) ScanRowMap(rows *sql.Rows) (map[string]interface{}, error) {

// 	err := rw.scanRowArgs(rows)
// 	if err != nil {
// 		return nil, err
// 	}

// 	if rw.rowMap == nil {
// 		rw.rowMap = make(map[string]interface{}, len(rw.scanArgs))
// 	}

// 	for i, cn := range rw.colNames {
// 		rw.rowMap[cn] = rw.scanArgs[i]
// 	}

// 	return rw.rowMap, nil
// }

func (rw *RowsWriter) WriteFields(fieldNames ...string) error {

	var err error

	rw.rowOutBuf.Reset()

	firstDone := false
	for i, cn := range rw.colNames {
		// log.Printf("col name: %#v", cn)
		for _, fn := range fieldNames {
			if cn == fn {
				if firstDone {
					rw.rowOutBuf.WriteByte(',')
				}
				firstDone = true
				// log.Printf("WRITING VALUE: %#v", rw.scanArgs[i])
				err = rw.writeValue(cn)
				if err != nil {
					return err
				}
				rw.rowOutBuf.WriteByte(':')
				err = rw.writeValue(rw.scanArgs[i])
				if err != nil {
					return err
				}
				break
			}
		}
	}

	_, err = rw.rowOutBuf.WriteTo(rw.Writer)
	return err

}

// unsafeString gives a string that points to the bytes of b.
// Only use this temporarily in a controlled area, do not assign
// this as a value, only use it for comparison.
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
