package tada

import (
	"fmt"
	"math/rand"
	"reflect"
	"unicode"

	"github.com/ptiger10/tablediff"
)

// all columns must be of same type.
// return null values for use in StructTransposer
func readNestedInterfaceByRowsInferType(rows [][]interface{}) (ret []interface{}, isNull [][]bool, err error) {
	interfaceType := reflect.TypeOf([]interface{}{})

	if len(rows) == 0 {
		return nil, nil, fmt.Errorf("reading [][]interface{}: must have at least one row")
	}

	firstRow := rows[0]
	ret = make([]interface{}, len(firstRow))
	isNull = make([][]bool, len(firstRow))
	for k := range firstRow {
		isNull[k] = make([]bool, len(rows))
	}
	colTypes := make([]reflect.Type, len(firstRow))

	for i := range rows {
		// different number of columns than in row 0?
		if len(rows[i]) != len(colTypes) {
			return nil, nil, fmt.Errorf("reading [][]interface{} by rows: row %d: all rows must have same length as row 0 (%d != %d)",
				i, len(rows[i]), len(colTypes))
		}
		for k := range rows[i] {
			if isNullInterface(rows[i][k]) {
				// is null value? set to zero type unless no colType has been set
				isNull[k][i] = true
				if colTypes[k] != nil {
					// colType has been set? set to zero type. otherwise ignore
					src := reflect.Zero(colTypes[k])
					dst := reflect.ValueOf(ret[k]).Index(i)
					dst.Set(src)
				}

				// not null value?
			} else {
				// col type not already set? create slice and backfill with zero types.
				if colTypes[k] == nil {
					colType := reflect.TypeOf(rows[i][k])
					colTypes[k] = colType
					ret[k] = reflect.MakeSlice(reflect.SliceOf(colType), len(rows), len(rows)).Interface()
					for backfill := 0; backfill < i; backfill++ {
						src := reflect.Zero(colType)
						dst := reflect.ValueOf(ret[k]).Index(backfill)
						dst.Set(src)
					}
				}
				// value same type as prior values? set value
				if reflect.TypeOf(rows[i][k]) == colTypes[k] {
					src := reflect.ValueOf(rows[i][k])
					dst := reflect.ValueOf(ret[k]).Index(i)
					dst.Set(src)
					// value different type? convert entire slice to []interface{}
					// replace zero-values with nil
				} else {
					if colTypes[k] != interfaceType {
						colTypes[k] = interfaceType
						newContainer := make([]interface{}, len(rows))
						// backfill prior values, replacing null values with nil
						for backfill := 0; backfill < i; backfill++ {
							if isNull[k][backfill] {
								newContainer[backfill] = nil
							} else {
								newContainer[backfill] = reflect.ValueOf(ret[k]).Index(backfill).Interface()
							}
						}
						ret[k] = newContainer
					}
					// set current value
					ret[k].([]interface{})[i] = rows[i][k]
				}

			}
		}
	}
	// col type never set?
	for k := range colTypes {
		if colTypes[k] == nil {
			// first row is nil? set as []interface{}
			if firstRow[k] == nil {
				ret[k] = make([]interface{}, len(rows))
				// first row has type? set values as same type as first row
			} else {
				ret[k] = reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(firstRow[k])), len(rows), len(rows)).Interface()
			}
		}
	}
	return
}

// each column is converted to []interface{}
func readNestedInterfaceByRows(rows [][]interface{}) ([]interface{}, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("reading [][]interface{}: must have at least one row")
	}
	// must deduce output type per column
	sampleRow := rows[0]
	ret := make([]interface{}, len(sampleRow))

	colTypes := make([]reflect.Type, len(sampleRow))
	for k := range sampleRow {
		colType := reflect.TypeOf(sampleRow[k])
		colTypes[k] = colType
		ret[k] = make([]interface{}, len(rows))
	}
	for i := range rows {
		// different number of columns than in row 0?
		if len(rows[i]) != len(colTypes) {
			return nil, fmt.Errorf("reading [][]interface{} by rows: row %d: all rows must have same length as row 0 (%d != %d)",
				i, len(rows[i]), len(colTypes))
		}
		for k := range rows[i] {

			ret[k].([]interface{})[i] = rows[i][k]

		}
	}
	return ret, nil
}

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

// Transpose reads the values of an untyped, row-oriented struct representation of a DataFrame
// into a typed, column-oriented struct representation of a DataFrame.
// If all non-null values in a column have the same type, then the column will be a slice of that type.
// If any of the non-null values in a column have different types, then the column will be []interface{}.
// If all values are considered null by tada, then the column will be a slice of the type in the first row
// (when all values are null and the first row is nil, the column will be []interface{}).
// If an error is returned, values are still written to structPointer up until the point the error occurred.
func (st StructTransposer) Transpose(structPointer interface{}) error {
	transfer, isNull, err := readNestedInterfaceByRowsInferType(st)
	if err != nil {
		return fmt.Errorf("transposing to struct: %v", err)
	}
	if reflect.TypeOf(structPointer).Kind() != reflect.Ptr {
		return fmt.Errorf("transposing to struct: structPointer must be pointer to struct, not %s", reflect.TypeOf(structPointer).Kind())
	}
	if reflect.TypeOf(structPointer).Elem().Kind() != reflect.Struct {
		return fmt.Errorf("transposing to struct: structPointer must be pointer to struct, not to %s", reflect.TypeOf(structPointer).Elem().Kind())
	}
	v := reflect.ValueOf(structPointer).Elem()
	var offset int
	var nullField string
	var hasNullTag bool
	nullTag := "isNull"
	for k := 0; k < v.NumField(); k++ {
		field := reflect.TypeOf(structPointer).Elem().Field(k)
		// is unexported field?
		if unicode.IsLower([]rune(field.Name)[0]) {
			offset--
			continue
		}
		// has null tag?
		if field.Tag.Get("tada") == nullTag {
			offset--
			if field.Type.String() != "[][]bool" {
				return fmt.Errorf("transposing to struct: field with tag %v must be type [][]bool, not %s", nullTag, field.Type.String())
			}
			hasNullTag = true
			nullField = field.Name
			continue
		}
		container := k + offset
		// df does not have enough containers?
		if container >= len(transfer) {
			return fmt.Errorf("transposing to struct: writing to exported field %s [%d]: insufficient number of columns [%d]",
				field.Name, container, len(transfer))
		}
		if reflect.TypeOf(transfer[container]) != field.Type {
			return fmt.Errorf("transposing to struct: writing to exported field %s [%d]: column has wrong type (%s != %s)",
				field.Name, container,
				reflect.TypeOf(transfer[container]), field.Type)
		}
		src := reflect.ValueOf(reflect.ValueOf(transfer[container]).Interface())
		dst := v.FieldByName(field.Name)
		dst.Set(src)
	}
	// receiving structPointer has null tag?
	if hasNullTag {
		if len(isNull) > 0 {
			src := reflect.ValueOf(isNull).Interface()
			dst := v.FieldByName(nullField)
			dst.Set(reflect.ValueOf(src))
		}
	}

	return nil

}

// Shuffle randomly shuffles the row order in Rows, using a randomizer seeded with seed.
func (st StructTransposer) Shuffle(seed int64) {
	rand.Seed(seed)
	rand.Shuffle(
		len(st),
		func(i, j int) {
			st[i], st[j] = st[j], st[i]
		})
	return
}

// PrettyDiff reads two structs into DataFrames, prints each as a stringified csv table,
// and returns whether they are equal. If not, returns the differences between the two.
func PrettyDiff(got, want interface{}) (bool, *tablediff.Differences, error) {
	df1, err := ReadStruct(got)
	if err != nil {
		return false, nil, fmt.Errorf("pretty diffing two structs: reading got: %v", err)
	}
	df2, err := ReadStruct((want))
	if err != nil {
		return false, nil, fmt.Errorf("pretty diffing two structs: reading want: %v", err)
	}
	gotRecords := df1.CSVRecords(WriteOptionExcludeLabels())
	wantRecords := df2.CSVRecords(WriteOptionExcludeLabels())
	diffs, eq := tablediff.Diff(gotRecords, wantRecords)
	return eq, diffs, nil
}
