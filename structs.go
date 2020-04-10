package tada

import (
	"fmt"
	"math/rand"
	"reflect"
	"unicode"
)

// if requireSameType, all columns must be of same type; otherwise, each column is converted to []interface{}
func readNestedInterfaceByRows(rows [][]interface{}, requireSameType bool) (ret []interface{}, isNull [][]bool, err error) {
	if len(rows) == 0 {
		return nil, nil, fmt.Errorf("reading [][]interface{}: must have at least one row")
	}
	// must deduce output type per column
	sampleRow := rows[0]
	ret = make([]interface{}, len(sampleRow))

	if requireSameType {
		isNull = make([][]bool, len(sampleRow))
		for k := range sampleRow {
			isNull[k] = make([]bool, len(rows))
		}
	} else {
		isNull = nil
	}

	colTypes := make([]reflect.Type, len(sampleRow))
	for k := range sampleRow {
		colType := reflect.TypeOf(sampleRow[k])
		colTypes[k] = colType
		if requireSameType {
			ret[k] = reflect.MakeSlice(reflect.SliceOf(colType), len(rows), len(rows)).Interface()
		} else {
			ret[k] = make([]interface{}, len(rows))
		}
	}
	for i := range rows {
		// different number of columns than in row 0?
		if len(rows[i]) != len(colTypes) {
			return nil, nil, fmt.Errorf("reading [][]interface{} by rows: row %d: all rows must have same length as row 0 (%d != %d)",
				i, len(rows[i]), len(colTypes))
		}
		for k := range rows[i] {
			// all columns are not same type as row 0?
			if requireSameType {
				// value is null value?
				dst := reflect.ValueOf(ret[k]).Index(i)
				var src reflect.Value
				// is null value? set to zero type
				if null, _ := isNullInterface(rows[i][k]); null {
					isNull[k][i] = true
					src = reflect.Zero(colTypes[k])
				}
				if !isNull[k][i] {
					if reflect.TypeOf(rows[i][k]) != colTypes[k] {
						return nil, nil, fmt.Errorf("reading [][]interface{} by rows: [%d][%d]: all types must be the same as in row 0 (%v != %v)",
							i, k, reflect.TypeOf(rows[i][k]).String(), colTypes[k].String())
					}
					src = reflect.ValueOf(rows[i][k])
				}
				dst.Set(src)
			} else {
				ret[k].([]interface{})[i] = rows[i][k]
			}

		}
	}
	return ret, isNull, nil
}

// if requireSameType, all columns must be of same type; otherwise, each column is converted to []interface{}
func readNestedInterfaceByCols(columns [][]interface{}, requireSameType bool) ([]interface{}, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("reading [][]interface{}: must have at least one column")
	}
	l := len(columns[0])
	ret := make([]interface{}, len(columns))
	colTypes := make([]reflect.Type, len(columns))
	for k := range columns {
		colType := reflect.TypeOf(columns[k][0])
		colTypes[k] = colType
		if requireSameType {
			ret[k] = reflect.MakeSlice(reflect.SliceOf(colType), len(columns), len(columns)).Interface()
		} else {
			ret[k] = make([]interface{}, len(columns))
		}
	}

	for k := range columns {
		if len(columns[k]) != l {
			return nil, fmt.Errorf("reading [][]interface{} by columns: column %d: all columns must have same length as column 0 (%d != %d)",
				k, len(columns[k]), l)
		}
		if requireSameType {
			for i := 0; i < l; i++ {
				if reflect.TypeOf(columns[k][i]) != colTypes[k] {
					return nil, fmt.Errorf("reading [][]interface{} by rows: [%d][%d]: all types must be the same as in row 0 (%v != %v)",
						i, k, reflect.TypeOf(columns[k][i]).String(), colTypes[k].String())
				}
				dst := reflect.ValueOf(ret[k]).Index(i)
				src := reflect.ValueOf(columns[k][i])
				dst.Set(src)
			}
		} else {
			ret[k] = columns[k]
		}
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

// Transpose reads the values of a row-oriented struct representation of a DataFrame
// into a column-oriented struct representation of a DataFrame.
// If an error is returned, values are still written to structPointer up until the point the error occurred.
func (st *StructTransposer) Transpose(structPointer interface{}) error {
	transfer, isNull, err := readNestedInterfaceByRows(st.Rows, true)
	if err != nil {
		return fmt.Errorf("transposing struct: %v", err)
	}
	if reflect.TypeOf(structPointer).Kind() != reflect.Ptr {
		return fmt.Errorf("transposing struct: structPointer must be pointer to struct, not %s", reflect.TypeOf(structPointer).Kind())
	}
	if reflect.TypeOf(structPointer).Elem().Kind() != reflect.Struct {
		return fmt.Errorf("transposing struct: structPointer must be pointer to struct, not to %s", reflect.TypeOf(structPointer).Elem().Kind())
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
				return fmt.Errorf("writing to struct: field with tag %v must be type [][]bool, not %s", nullTag, field.Type.String())
			}
			hasNullTag = true
			nullField = field.Name
			continue
		}
		container := k + offset
		// df does not have enough containers?
		if container >= len(transfer) {
			return fmt.Errorf("transposing struct: insufficient columns to write to exported field %s (column count: %d)",
				field.Name, container)
		}
		if reflect.TypeOf(transfer[container]) != field.Type {
			return fmt.Errorf("transposing struct: position %d, StructTransposer has wrong type for field %s (%s != %s)",
				container, field.Name, reflect.TypeOf(transfer[container]), field.Type)
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
func (st *StructTransposer) Shuffle(seed int64) {
	rand.Seed(seed)
	rand.Shuffle(
		len(st.Rows),
		func(i, j int) {
			st.Rows[i], st.Rows[j] = st.Rows[j], st.Rows[i]
		})
	return
}
