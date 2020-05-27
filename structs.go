package tada

import (
	"fmt"
	"reflect"

	"github.com/ptiger10/tablediff"
)

// if requireSameType, all columns must be of same type; otherwise, each column is converted to []interface{}
func readNestedInterfaceByCols(columns [][]interface{}) ([]interface{}, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("reading [][]interface{}: must have at least one column")
	}
	l := len(columns[0])
	ret := make([]interface{}, len(columns))
	colTypes := make([]reflect.Type, len(columns))
	for k := range columns {
		colType := reflect.TypeOf(columns[k][0])
		colTypes[k] = colType
		ret[k] = make([]interface{}, len(columns))
	}

	for k := range columns {
		if len(columns[k]) != l {
			return nil, fmt.Errorf("reading [][]interface{} by columns: column %d: all columns must have same length as column 0 (%d != %d)",
				k, len(columns[k]), l)
		}
		ret[k] = columns[k]

	}
	return ret, nil
}

func transposeNestedNulls(isNull [][]bool) ([][]bool, error) {
	if len(isNull) == 0 {
		return nil, nil
	}
	ret := make([][]bool, len(isNull[0]))
	for k := range isNull[0] {
		ret[k] = make([]bool, len(isNull))
	}
	for i := range isNull {
		if len(isNull[i]) != len(isNull[0]) {
			return nil, fmt.Errorf("transposing [][]bool: row %d: all rows must have same length as row 0 (%d != %d)",
				i, len(isNull[i]), len(isNull[0]))
		}
		for k := range isNull[i] {
			ret[k][i] = isNull[i][k]
		}
	}
	return ret, nil
}

// PrettyDiff reads two slices of structs into DataFrames, prints each as a stringified csv table,
// and returns whether they are equal. If not, returns the differences between the two.
func PrettyDiff(got, want interface{}) (bool, *tablediff.Differences, error) {
	df1, err := NewStructReader(got).Read()
	if err != nil {
		return false, nil, fmt.Errorf("pretty diffing two structs: reading got: %v", err)
	}
	df2, err := NewStructReader(want).Read()
	if err != nil {
		return false, nil, fmt.Errorf("pretty diffing two structs: reading want: %v", err)
	}
	gotRecords := NewRecordWriter()
	wantRecords := NewRecordWriter()
	// ducking error because writing to [][]string known to not cause errors
	df1.WriteTo(gotRecords)
	df2.WriteTo(wantRecords)
	diffs, eq := tablediff.Diff(gotRecords.Records(), wantRecords.Records())
	return eq, diffs, nil
}
