package tada

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/civil"
)

func newValueContainer(slice interface{}, isNull []bool, name string, opts ...string) *valueContainer {
	var id string
	if len(opts) > 0 {
		id = opts[0]
	} else {
		id = makeID()
	}

	return &valueContainer{
		slice:  slice,
		isNull: isNull,
		name:   name,
		id:     id,
	}
}

func errorWarning(err error) {
	if optionWarnings {
		log.Println("Warning:", err)
	}
}

func (s *Series) resetWithError(err error) {
	errorWarning(err)

	s.values = nil
	s.labels = nil
	s.err = err
}

func (df *DataFrame) resetWithError(err error) {
	errorWarning(err)

	df.values = nil
	df.labels = nil
	df.name = ""
	df.err = err
	df.colLevelNames = nil
}

func seriesWithError(err error) *Series {
	errorWarning(err)

	return &Series{
		err: err,
	}
}

func dataFrameWithError(err error) *DataFrame {
	errorWarning(err)

	return &DataFrame{
		err: err,
	}
}

func groupedSeriesWithError(err error) *GroupedSeries {
	errorWarning(err)

	return &GroupedSeries{
		err: err,
	}
}

func groupedDataFrameWithError(err error) *GroupedDataFrame {
	errorWarning(err)

	return &GroupedDataFrame{
		err: err,
	}
}

func isSlice(input interface{}) bool {
	return reflect.TypeOf(input).Kind() == reflect.Slice
}

func unpackIDsByPosition(containers []*valueContainer, receivers ...*string) error {
	if len(receivers) > len(containers) {
		return fmt.Errorf("unpacking container ids by index position:"+
			"len(receivers) out of range [%d] with length %d",
			len(receivers), len(containers))
	}
	for i := range containers {
		if i >= len(receivers) {
			return nil
		}
		*receivers[i] = containers[i].id
	}
	return nil
}

func unpackIDsByName(containers []*valueContainer, receivers map[string]*string) error {
	for k, v := range receivers {
		i, err := indexOfContainer(k, containers)
		if err != nil {
			return fmt.Errorf("unpacking container ids by container name: %v", err)
		}
		*v = containers[i].id
	}
	return nil
}

func makeValueContainerFromInterface(slice interface{}, name string) (*valueContainer, error) {
	isNull, err := setNullsFromInterface(slice)
	if err != nil {
		return nil, err
	}
	return newValueContainer(slice, isNull, name), nil
}

func makeID() string {
	return tadaID + strconv.Itoa(int(clock.now().UnixNano()))
}

func makeValueContainersFromInterfaces(slices []interface{}, usePrefix bool) ([]*valueContainer, error) {
	var namePrefix string
	if usePrefix {
		namePrefix = optionPrefix
	}
	ret := make([]*valueContainer, len(slices))
	for i, slice := range slices {
		vc, err := makeValueContainerFromInterface(slice, namePrefix+fmt.Sprint(i))
		if err != nil {
			return nil, fmt.Errorf("slice[%d]: %v", i, err)
		}
		ret[i] = vc
	}
	return ret, nil
}

func ensureEqualLengths(containers []*valueContainer, length int) error {
	for k := range containers {
		if containers[k].len() != length {
			return fmt.Errorf("slice[%d] does not match required length (%d != %d)",
				k, containers[k].len(), length)
		}
	}
	return nil
}

// makeDefaultLabels returns a valueContainer with a
// sequential series of numbers (inclusive of min, exclusive of max), a companion isNull slice, and a name.
func makeDefaultLabels(min, max int, prefixAsterisk bool) *valueContainer {
	labels := make([]int, max-min)
	isNull := make([]bool, len(labels))
	for i := range labels {
		labels[i] = min + i
		isNull[i] = false
	}
	name := "0"
	if prefixAsterisk {
		name = "*0"
	}
	return newValueContainer(labels, isNull, name)
}

// makeIntRange returns a sequential series of numbers (inclusive of min, exclusive of max)
func makeIntRange(min, max int) []int {
	ret := make([]int, max-min)
	for i := range ret {
		ret[i] = min + i
	}
	return ret
}

// search for a name only once.
// if it is found multiple times, return only the first
func findMatchingKeysBetweenTwoContainers(container1 []*valueContainer, container2 []*valueContainer) ([]int, []int, error) {
	var leftKeys, rightKeys []int
	searched := make(map[string]bool)
	// add every level name to the map in order to skip duplicates
	for j := range container1 {
		key := container1[j].name
		// if level name already in map, skip
		if _, ok := searched[key]; ok {
			continue
			// if level name not already in map, add to map
		} else {
			searched[key] = true
		}
		for k := range container2 {
			// compare to every name in labels2
			if key == container2[k].name {
				leftKeys = append(leftKeys, j)
				rightKeys = append(rightKeys, k)
				break
			}
		}
	}
	if leftKeys == nil {
		return nil, nil, fmt.Errorf("no matching keys between containers")
	}
	return leftKeys, rightKeys, nil
}

// nameOfContainer returns the name of the container at index position n.
// If n is out of range, returns error message as string.
func nameOfContainer(containers []*valueContainer, n int) string {
	if n >= len(containers) {
		return fmt.Sprintf("index out of range [%d] with length %d", n, len(containers))
	}
	return containers[n].name
}

// indexOfContainer returns the position of the first level within cols with a name matching name, or an error if no level matches
// case-sensitive
func indexOfContainer(name string, containers []*valueContainer) (int, error) {
	for j := range containers {
		if strings.HasPrefix(name, tadaID) {
			if containers[j].id == name {
				return j, nil
			}
		} else {
			if containers[j].name == name {
				return j, nil
			}
		}
	}
	return 0, fmt.Errorf("name (%v) not found", name)
}

func (vc *valueContainer) indexOfRows(value interface{}) []int {
	vals := vc.string().slice
	stringifiedValue := fmt.Sprint(value)
	ret := make([]int, len(vals))
	var counter int
	for i := range vals {
		ret[counter] = i
		if !vc.isNull[i] && vals[i] == stringifiedValue {
			counter++
		}
	}
	return ret[:counter]
}

func withColumn(cols []*valueContainer, name string, input interface{}, requiredLen int) ([]*valueContainer, error) {
	switch reflect.TypeOf(input).Kind() {
	// input is string? rename label level
	case reflect.String:
		lvl, err := indexOfContainer(name, cols)
		if err != nil {
			return nil, fmt.Errorf("cannot rename container: %v", err)
		}
		cols[lvl].name = input.(string)
	case reflect.Slice:
		// duck error because it is known to be a slice
		isNull, _ := setNullsFromInterface(input)
		if l := reflect.ValueOf(input).Len(); l != requiredLen {
			return nil, fmt.Errorf(
				"cannot replace slice in container %s: length of input (%d) does not match existing length (%d)",
				name, l, requiredLen)
		}
		// input is supported slice? append or overwrite
		lvl, err := indexOfContainer(name, cols)
		if err != nil {
			// name does not already exist: append new label level
			cols = append(cols, newValueContainer(input, isNull, name))
		} else {
			// name already exists: overwrite existing label level
			cols[lvl].slice = input
			cols[lvl].isNull = isNull
			cols[lvl].resetCache()
		}

	default:
		return nil, fmt.Errorf("unsupported input kind (%v)", reflect.TypeOf(input).Kind())
	}
	return cols, nil
}

// -- MATRIX MANIPULATION

// expects every item in slices to be a slice, and for len(slices) to equal len(isNull) and len(names)
// if isNull is nil, sets null values from slices
func copyInterfaceIntoValueContainers(slices []interface{}, isNull [][]bool, names []string) []*valueContainer {
	ret := make([]*valueContainer, len(names))
	if isNull == nil {
		isNull = make([][]bool, len(slices))
		for k := range slices {
			// ducks error because nulls are set from existing values
			isNull[k], _ = setNullsFromInterface(slices[k])
		}
	}
	for k := range slices {
		ret[k] = newValueContainer(slices[k], isNull[k], names[k])
	}
	return ret
}

// convert strings to interface. if isNull is nil, sets null values from slices
func copyStringsIntoValueContainers(slices [][]string, isNull [][]bool, names []string) []*valueContainer {
	slicesInterface := make([]interface{}, len(slices))
	for k := range slices {
		slicesInterface[k] = slices[k]
	}
	return copyInterfaceIntoValueContainers(slicesInterface, isNull, names)
}

// convert Floats to interface. if isNull is nil, sets null values from slices
func copyFloatsIntoValueContainers(slices [][]float64, isNull [][]bool, names []string) []*valueContainer {
	slicesInterface := make([]interface{}, len(slices))
	for k := range slices {
		slicesInterface[k] = slices[k]
	}
	return copyInterfaceIntoValueContainers(slicesInterface, isNull, names)
}

// columns as major dimension
func makeByteMatrix(numCols, numRows int) [][][]byte {
	ret := make([][][]byte, numCols)
	for k := 0; k < numCols; k++ {
		ret[k] = make([][]byte, numRows)
	}
	return ret
}

// columns as major dimension
func makeStringMatrix(numCols, numRows int) [][]string {
	ret := make([][]string, numCols)
	for k := 0; k < numCols; k++ {
		ret[k] = make([]string, numRows)
	}
	return ret
}

// columns as major dimension
func makeFloatMatrix(numCols, numRows int) [][]float64 {
	ret := make([][]float64, numCols)
	for k := 0; k < numCols; k++ {
		ret[k] = make([]float64, numRows)
	}
	return ret
}

// columns as major dimension
func makeBoolMatrix(numCols, numRows int) [][]bool {
	ret := make([][]bool, numCols)
	for k := 0; k < numCols; k++ {
		ret[k] = make([]bool, numRows)
	}
	return ret
}

// returns []int that are shared by all slices
func intersection(slices [][]int, maxLen int) []int {
	// only one slice? intersection is that slice
	if len(slices) == 1 {
		return slices[0]
	}

	// all slices are the max length? intersection should be all the index values in sequential order
	allMaxLength := true
	for k := range slices {
		if len(slices[k]) != maxLen {
			allMaxLength = false
		}
	}
	if allMaxLength {
		return makeIntRange(0, maxLen)
	}

	orderedKeys := make([]int, maxLen*len(slices))
	var counter int
	set := make(map[int]int, maxLen)
	for _, slice := range slices {
		for _, i := range slice {
			if _, ok := set[i]; !ok {
				set[i] = 1
				orderedKeys[counter] = i
				counter++
			} else {
				set[i]++
			}
		}
	}
	ret := make([]int, maxLen)
	qualifyingCounter := 0
	for _, key := range orderedKeys[:counter] {
		// this means that the value appeared in every slice
		if set[key] == len(slices) {
			ret[qualifyingCounter] = key
			qualifyingCounter++
		}
	}
	return ret[:qualifyingCounter]
}

func union(slices [][]int) []int {
	var ret []int
	set := make(map[int]bool)
	for _, slice := range slices {
		for i := range slice {
			if _, ok := set[slice[i]]; !ok {
				set[slice[i]] = true
				ret = append(ret, slice[i])
			}
		}
	}
	sort.Ints(ret)
	return ret
}

func difference(slice1 []int, slice2 []int) []int {
	var ret []int
	// mark as true if contained in slice1
	set := make(map[int]bool)
	for _, i := range slice1 {
		set[i] = true
	}
	// mark as false if contained in slice2 as well
	for _, i := range slice2 {
		if _, ok := set[i]; ok {
			set[i] = false
		}
	}
	orderedKeys := make([]int, 0)
	for k := range set {
		orderedKeys = append(orderedKeys, k)
	}
	sort.Ints(orderedKeys)
	for _, k := range orderedKeys {
		if set[k] {
			ret = append(ret, k)
		}
	}
	return ret
}

func (df *DataFrame) toCSVByRows(includeLabels bool) ([][]string, error) {
	if df.values == nil {
		return nil, fmt.Errorf("cannot export empty dataframe")
	}
	// make final container with rows as major dimension
	ret := make([][]string, df.numColLevels()+df.Len())
	for i := range ret {
		var newCols int
		if includeLabels {
			newCols = df.NumLevels() + df.NumColumns()
		} else {
			newCols = df.NumColumns()
		}
		ret[i] = make([]string, newCols)
	}
	if includeLabels {
		for j := range df.labels {
			// write label headers, index at first header row
			ret[df.numColLevels()-1][j] = df.labels[j].name
			v := df.labels[j].string().slice
			// write label values, offset by header rows
			for i := range v {
				ret[i+df.numColLevels()][j] = v[i]
			}
		}
	}
	// if there are multiple column headers, those rows will be blank above the index header
	for k := range df.values {
		var offset int
		if includeLabels {
			offset = df.NumLevels()
		}
		// if number of col levels is only one, return the name as a single-item slice
		multiColHeaders := splitNameIntoLevels(df.values[k].name)
		for l := 0; l < df.numColLevels(); l++ {
			// write multi column headers, offset by label levels
			ret[l][k+offset] = multiColHeaders[l]
		}
		v := df.values[k].string().slice
		// write label values, offset by header rows and label levels
		for i := range v {
			ret[i+df.numColLevels()][k+offset] = v[i]
		}
	}
	return ret, nil
}

func setJoinConfig(options []JoinOption) *joinConfig {
	// default config
	config := &joinConfig{
		how: "left",
	}
	for _, option := range options {
		option(config)
	}
	return config
}

func containersToDF(containers []*valueContainer, numHeaders int, numLabels int, name string) *DataFrame {
	labels := containers[:numLabels]
	if numLabels == 0 {
		labels = []*valueContainer{makeDefaultLabels(0, containers[0].len(), true)}
	}
	columns := containers[numLabels:]
	var colLevelNames []string
	colLevelNames, columns = setColLevelNames(numHeaders, columns)
	df := &DataFrame{
		labels:        labels,
		values:        columns,
		colLevelNames: colLevelNames,
		name:          name,
	}
	return df
}

func readRecords(records [][]string, byColumns bool, numHeaders int) ([]*valueContainer, error) {
	xl := len(records[0])
	for k := range records {
		if len(records[k]) != xl {
			return nil, fmt.Errorf("num items in row %d [%d] does not match row 0 [%d]", k, len(records[k]), xl)
		}
	}
	if !byColumns {
		records = transposeRecords(records)
	}
	headers := popNRecords(records, numHeaders)
	ret := make([]*valueContainer, len(records))
	for k := range records {
		// duck error because slice is guaranteed
		isNull, _ := setNullsFromInterface(records[k])
		ret[k] = newValueContainer(
			records[k],
			isNull,
			joinLevelsIntoName(headers[k]),
		)
	}
	return ret, nil
}

// major dimension: columns
func popNRecords(records [][]string, n int) [][]string {
	ret := make([][]string, len(records))
	for k := range records {
		ret[k] = records[k][:n]
		records[k] = records[k][n:]
	}
	return ret
}

func transposeRecords(records [][]string) [][]string {
	numCols := len(records[0])
	numRows := len(records)
	ret := make([][]string, numCols)
	for i := range ret {
		ret[i] = make([]string, numRows)
	}
	for i := 0; i < numCols; i++ {
		for j := 0; j < numRows; j++ {
			ret[i][j] = records[j][i]
		}
	}
	return ret
}

func readInterfaceRecords(records [][]interface{}, byColumns bool, numHeaders int) ([]*valueContainer, error) {
	xl := len(records[0])
	for k := range records {
		if len(records[k]) != xl {
			return nil, fmt.Errorf("num items in row %d [%d] does not match row 0 [%d]", k, len(records[k]), xl)
		}
	}
	if !byColumns {
		records = transposeInterfaceRecords(records)
	}
	headers := popNInterfaceRecords(records, numHeaders)
	ret := make([]*valueContainer, len(records))
	for k := range records {
		// duck error because slice is guaranteed
		isNull, _ := setNullsFromInterface(records[k])
		stringifiedHeaders := make([]string, len(headers[k]))
		for i := range headers[k] {
			stringifiedHeaders[i] = fmt.Sprint(headers[k][i])
		}
		ret[k] = newValueContainer(
			records[k],
			isNull,
			joinLevelsIntoName(stringifiedHeaders),
		)
	}
	return ret, nil
}

// major dimension: columns
func popNInterfaceRecords(records [][]interface{}, n int) [][]interface{} {
	ret := make([][]interface{}, len(records))
	for k := range records {
		ret[k] = records[k][:n]
		records[k] = records[k][n:]
	}
	return ret
}

func transposeInterfaceRecords(records [][]interface{}) [][]interface{} {
	numCols := len(records[0])
	numRows := len(records)
	ret := make([][]interface{}, numCols)
	for i := range ret {
		ret[i] = make([]interface{}, numRows)
	}
	for k := 0; k < numCols; k++ {
		for i := 0; i < numRows; i++ {
			ret[k][i] = records[i][k]
		}
	}
	return ret
}

// (colLevelNames, columnValues)
func setColLevelNames(numHeaderRows int, columns []*valueContainer) ([]string, []*valueContainer) {
	if numHeaderRows <= 0 {
		// if no header rows, change the names of the columns to be 0, 1...
		for k := range columns {
			columns[k].name = fmt.Sprintf("%v", k)
		}
		return []string{"*0"}, columns
	}
	// if header rows, set the col level names to *0, *1...
	ret := make([]string, numHeaderRows)
	for l := range ret {
		ret[l] = fmt.Sprintf("*%d", l)
	}
	return ret, columns
}

func writeStructSlice(containers []*valueContainer, slice interface{}, noUnmatchedCols bool) ([][]bool, error) {
	if reflect.TypeOf(slice).Kind() != reflect.Ptr ||
		reflect.TypeOf(slice).Elem().Kind() != reflect.Slice ||
		reflect.TypeOf(slice).Elem().Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("writing to slice of structs: unsupported input type (%v), must be *[]struct", reflect.TypeOf(slice))
	}
	if len(containers) == 0 {
		return nil, fmt.Errorf("writing to slice of structs: dataframe must have at least one container (label level or column)")
	}
	// m: fields exported by the struct and the DataFrame {structFieldIndex: containerIndex}
	m := make(map[int]int)
	t := reflect.TypeOf(slice)
	protoStruct := t.Elem().Elem()
	for i := 0; i < protoStruct.NumField(); i++ {
		var name string
		if js := protoStruct.Field(i).Tag.Get("json"); js != "" {
			name = js
		} else {
			name = protoStruct.Field(i).Name
		}
		k, err := indexOfContainer(name, containers)
		if err == nil {
			m[i] = k
		}
	}

	if noUnmatchedCols {
		if len(containers) > len(m) {
			return nil, fmt.Errorf("writing to slice of structs: DataFrame has unmatched containers")
		}
	}

	numRows := containers[0].len()

	v := reflect.ValueOf(slice)
	v.Elem().Set(reflect.MakeSlice(reflect.SliceOf(protoStruct), numRows, numRows))

	for i := 0; i < numRows; i++ {
		s := reflect.New(protoStruct)
		for key, value := range m {
			dst := s.Elem().Field(key)
			dst.Set(reflect.ValueOf(containers[value].slice).Index(i))
		}
		v.Elem().Index(i).Set(s.Elem())
	}
	// set nulls
	nulls := make([][]bool, protoStruct.NumField())
	fieldOrder := make([]int, 0, 5)
	for k := range m {
		fieldOrder = append(fieldOrder, k)
	}
	sort.Ints(fieldOrder)

	var counter int
	for i := 0; i < protoStruct.NumField(); i++ {
		if counter < len(fieldOrder) && i == fieldOrder[counter] {
			k := m[i]
			nulls[i] = containers[k].isNull
			counter++
		} else {
			// if field is not exported by DataFrame, set all nulls to false
			nulls[i] = make([]bool, numRows)
		}
	}

	// transpose nulls
	nulls, _ = transposeNestedNulls(nulls) // ducks error because constructing nulls is controlled

	return nulls, nil

}

// each struct becomes a different row
// each field becomes a different column
func readStructSlice(slice interface{}, isNull [][]bool) ([]*valueContainer, error) {
	if !isSlice(slice) || reflect.TypeOf(slice).Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("unsupported input type (%v), must be []struct", reflect.TypeOf(slice))
	}
	v := reflect.ValueOf(slice)
	if v.Len() == 0 {
		return nil, fmt.Errorf("slice must contain at least one struct")
	}
	protoStruct := reflect.TypeOf(slice).Elem()
	numCols := protoStruct.NumField()
	if numCols == 0 {
		return nil, fmt.Errorf("struct must contain at least one field")
	}
	retValues := make([]interface{}, numCols)
	retNames := make([]string, numCols)
	for k := 0; k < numCols; k++ {
		field := protoStruct.Field(k)
		colType := field.Type
		colValues := reflect.MakeSlice(reflect.SliceOf(colType), v.Len(), v.Len())
		// setting container name
		var name string
		if js := field.Tag.Get("json"); js != "" {
			name = js
		} else {
			name = field.Name
		}
		retNames[k] = name
		for i := 0; i < v.Len(); i++ {
			dst := colValues.Index(i)
			src := v.Index(i).Field(k)
			dst.Set(src)
		}
		retValues[k] = colValues.Interface()
	}
	// transfer to final container
	ret := make([]*valueContainer, numCols)

	// set null values
	if isNull == nil {
		isNull = make([][]bool, len(ret))
		for k := range ret {
			isNull[k], _ = setNullsFromInterface(retValues[k])
		}
	} else {
		if len(ret) != len(isNull[0]) {
			return nil, fmt.Errorf("setting null values: number of columns in [][]bool (%d) does not match number of exported fields (%d)",
				len(ret), len(isNull[0]))
		}
		if v.Len() != len(isNull) {
			return nil, fmt.Errorf("setting null values: number of rows in [][]bool (%d) does not match number of structs in slice (%d)",
				v.Len(), len(isNull))
		}
		var err error
		isNull, err = transposeNestedNulls(isNull)
		if err != nil {
			return nil, fmt.Errorf("reading slice of structs: setting null values: %v", err)
		}
	}
	for k := range ret {
		ret[k] = newValueContainer(retValues[k], isNull[k], retNames[k])
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

func inferType(input string) DType {
	if _, err := strconv.ParseFloat(input, 64); err == nil {
		return Float64
	}
	if t, null := convertStringToDateTime(input); !null {
		if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0 {
			return Date
		}
		if t.Year() == 0 && t.Month() == 1 && t.Day() == 1 {
			return Time
		}
		return DateTime
	}
	return String
}

// cast valueContainers in place
func castToInferredTypes(containers []*valueContainer) {
	for k := range containers {
		dtype := containers[k].inferType()
		containers[k].cast(dtype)
	}
	return
}

// expects vc.slice to be []string
func (vc *valueContainer) inferType() DType {
	s := vc.slice.([]string)
	sampleSize := 10
	if len(s) < sampleSize {
		sampleSize = len(s)
	}
	inferredTypes := make(map[DType]int)
	sample := s[:sampleSize]
	for i := range sample {
		dtype := inferType(sample[i])
		inferredTypes[dtype]++
	}
	var highestCount int
	var dtype DType
	for key, v := range inferredTypes {
		// tie resolves randomly
		if v > highestCount {
			dtype = key
			highestCount = v
		}
	}
	return dtype
}

// major dimension of output is rows
func mockContainersFromDTypes(names []string, dtypes []DType, numMockRows int) []*valueContainer {
	ret := make([]*valueContainer, len(dtypes))
	for k := range ret {
		s := make([]string, numMockRows)
		isNull := make([]bool, numMockRows)
		for i := range s {
			s[i], isNull[i] = mockString(dtypes[k], .1)
		}
		ret[k] = newValueContainer(s, isNull, names[k])
		ret[k].cast(dtypes[k])
	}
	return ret
}

func mockString(dtype DType, nullPct float64) (string, bool) {
	var options []string
	switch dtype {
	// overwrite the options based on the dtype
	case Float64:
		options = []string{".1", ".25", ".5", ".75", ".9"}
	case String:
		options = []string{"foo", "bar", "baz", "qux", "quuz"}
	case DateTime:
		options = []string{
			"2020-01-01T00:00:00Z00:00", "2020-01-01T12:00:00Z00:00", "2020-01-01T12:30:00Z00:00",
			"2020-01-02T00:00:00Z00:00", "2020-01-01T12:30:00Z00:00"}
	case Date:
		options = []string{"2019-12-31", "2020-01-01", "2020-01-02", "2020-02-01", "2020-02-02"}
	case Time:
		options = []string{"10:00am", "11:00am", "1:00pm", "2:00pm", "3:30pm"}
	}
	rand.Seed(clock.now().UnixNano())
	f := rand.Float64()
	if f < nullPct {
		return optionsNullPrinter, true
	}
	randomIndex := rand.Intn(len(options))
	return options[randomIndex], false
}

// modifies vc in place
func (vc *valueContainer) fillnull(lambda NullFiller) {
	vc.resetCache()
	v := reflect.ValueOf(vc.slice)
	zeroVal := reflect.Zero(v.Type().Elem())
	if lambda.FillForward {
		lastValid := zeroVal
		for i := 0; i < len(vc.isNull); i++ {
			if !vc.isNull[i] {
				lastValid = v.Index(i)
			} else {
				v.Index(i).Set(lastValid)
				vc.isNull[i] = false
			}
		}
		return
	}
	if lambda.FillBackward {
		lastValid := zeroVal
		for i := len(vc.isNull) - 1; i >= 0; i-- {
			if !vc.isNull[i] {
				lastValid = v.Index(i)
			} else {
				v.Index(i).Set(lastValid)
				vc.isNull[i] = false
			}
		}
		return
	}
	if lambda.FillZero {
		for i := 0; i < len(vc.isNull); i++ {
			if vc.isNull[i] {
				v.Index(i).Set(zeroVal)
				vc.isNull[i] = false
			}
		}
		return
	}
	// // default: coerce to float and fill with 0
	vals := vc.float64().slice
	for i := 0; i < len(vc.isNull); i++ {
		if vc.isNull[i] {
			vals[i] = lambda.FillFloat
			vc.isNull[i] = false
		}
	}
	vc.slice = vals
	return
}

func (vc *valueContainer) valid() []int {
	index := make([]int, len(vc.isNull))
	// increment counter for every valid row
	var counter int
	for i := range vc.isNull {
		if !vc.isNull[i] {
			index[counter] = i
			counter++
		}
	}
	return index[:counter]
}

// index positions of any null value
func (vc *valueContainer) null() []int {
	index := make([]int, len(vc.isNull))
	// increment counter for every null row
	var counter int
	for i := range vc.isNull {
		if vc.isNull[i] {
			index[counter] = i
			counter++
		}
	}
	return index[:counter]
}

func subsetContainerRows(containers []*valueContainer, index []int) error {
	for k := range containers {
		err := containers[k].subsetRows(index)
		if err != nil {
			return err
		}
	}
	return nil
}

// expects index to be in range
func subsetInterfaceSlice(slice interface{}, index []int) interface{} {
	switch slice.(type) {
	case []float64:
		v := slice.([]float64)
		retVals := make([]float64, len(index))
		for incrementor, i := range index {
			retVals[incrementor] = v[i]
		}
		return retVals
	case []string:
		v := slice.([]string)
		retVals := make([]string, len(index))
		for incrementor, i := range index {
			retVals[incrementor] = v[i]
		}
		return retVals
	case []time.Time:
		v := slice.([]time.Time)
		retVals := make([]time.Time, len(index))
		for incrementor, i := range index {
			retVals[incrementor] = v[i]
		}
		return retVals
	case []int:
		v := slice.([]int)
		retVals := make([]int, len(index))
		for incrementor, i := range index {
			retVals[incrementor] = v[i]
		}
		return retVals
	default:
		v := reflect.ValueOf(slice)
		retVals := reflect.MakeSlice(v.Type(), len(index), len(index))
		// []int{1, 5}
		// incrementor: [0, 1], i: [1,5]
		for incrementor, i := range index {
			dst := retVals.Index(incrementor)
			src := v.Index(i)
			dst.Set(src)
		}
		return retVals.Interface()
	}

}

func subsetNulls(nulls []bool, index []int) []bool {
	retNulls := make([]bool, len(index))
	for incrementor, i := range index {
		retNulls[incrementor] = nulls[i]
	}
	return retNulls
}

// subsetRows modifies vc in place to contain ony the rows specified by index.
// If any position is out of range, returns an error
func (vc *valueContainer) subsetRows(index []int) error {
	l := reflect.ValueOf(vc.slice).Len()
	if len(index) == l {
		if reflect.DeepEqual(index, makeIntRange(0, l)) {
			return nil
		}
	}
	for _, i := range index {
		if i >= l {
			return fmt.Errorf("index out of range [%d] with length %d", i, l)
		}
	}

	vc.slice = subsetInterfaceSlice(vc.slice, index)
	vc.isNull = subsetNulls(vc.isNull, index)
	vc.resetCache()
	return nil
}

// subsetContainers returns a new set of valueContainers containing only the columns specified by index.
// Has different behavior from subsetRows becauseto avoid needing ot copy containers again for lookup and groupby
// If any position is out of range, returns error
func subsetContainers(containers []*valueContainer, index []int) ([]*valueContainer, error) {
	retLabels := make([]*valueContainer, len(index))
	for indexPosition, indexValue := range index {
		if indexValue >= len(containers) {
			return nil, fmt.Errorf("index out of range [%d] with length %d", indexValue, len(containers))
		}
		retLabels[indexPosition] = containers[indexValue]
	}
	return retLabels, nil
}

// head returns the first number of rows specified by n
func (vc *valueContainer) head(n int) *valueContainer {
	v := reflect.ValueOf(vc.slice)
	var retIsNull []bool
	retVals := v.Slice(0, n)
	retIsNull = vc.isNull[:n]

	return newValueContainer(retVals.Interface(), retIsNull, vc.name, vc.id)
}

// tail returns the last number of rows specified by n
func (vc *valueContainer) tail(n int) *valueContainer {
	v := reflect.ValueOf(vc.slice)
	var retIsNull []bool
	retVals := v.Slice(len(vc.isNull)-n, len(vc.isNull))
	retIsNull = vc.isNull[len(vc.isNull)-n : len(vc.isNull)]

	return newValueContainer(retVals.Interface(), retIsNull, vc.name, vc.id)
}

// rangeSlice returns the rows starting with first and ending with last (exclusive)
func (vc *valueContainer) rangeSlice(first, last int) *valueContainer {
	v := reflect.ValueOf(vc.slice)
	var retIsNull []bool
	retVals := v.Slice(first, last)
	retIsNull = vc.isNull[first:last]

	return newValueContainer(retVals.Interface(), retIsNull, vc.name, vc.id)
}

func (vc *valueContainer) shift(n int) *valueContainer {
	v := reflect.ValueOf(vc.slice)
	vals := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
	isNull := make([]bool, v.Len())
	for i := 0; i < v.Len(); i++ {
		position := i - n
		if position < 0 || position >= v.Len() {
			isNull[i] = true
		} else {
			vals.Index(i).Set(v.Index(position))
			isNull[i] = vc.isNull[position]
		}
	}
	return newValueContainer(vals.Interface(), isNull, vc.name, vc.id)
}

// append values to the end of a valueContainer
// convert to string as lowest common denominator if types are not the same
func (vc *valueContainer) append(other *valueContainer) *valueContainer {
	var retSlice interface{}
	if reflect.TypeOf(vc.slice) == reflect.TypeOf(other.slice) {
		retSlice = reflect.AppendSlice(
			reflect.ValueOf(vc.slice), reflect.ValueOf(other.slice)).Interface()
	} else {
		retSlice = append(vc.string().slice, other.string().slice...)
	}

	retIsNull := append(vc.isNull, other.isNull...)
	return newValueContainer(retSlice, retIsNull, vc.name, vc.id)
}

func (vc *valueContainer) iterRow(index int) Element {
	return Element{
		Val:    reflect.ValueOf(vc.slice).Index(index).Interface(),
		IsNull: vc.isNull[index]}
}

// call filter.validate() first
func (vc *valueContainer) filter(filter FilterFn) []int {
	var index []int
	v := reflect.ValueOf(vc.slice)
	for i := 0; i < v.Len(); i++ {
		val := v.Index(i).Interface()
		if filter(val) && !vc.isNull[i] {
			index = append(index, i)
		}
	}
	return index
}

func (vc *valueContainer) set(newSlice interface{}, newNulls []bool, index []int) error {
	v := reflect.ValueOf(vc.slice)
	newV := reflect.ValueOf(newSlice)
	if v.Type() != newV.Type() {
		return fmt.Errorf("setting new values: type must match existing type (%s != %s)",
			v.Type().String(), newV.Type().String())
	}
	for incrementor, i := range index {
		src := newV.Index(incrementor)
		dst := v.Index(i)
		dst.Set(src)
		vc.isNull[i] = newNulls[incrementor]
	}
	return nil
}

func (vc *valueContainer) apply(lambda ApplyFn, index []int) error {
	// create caches to reset container on error
	cache := make([]bool, len(vc.isNull))
	copy(cache, vc.isNull)
	containerLen := vc.len()
	requiredLen := containerLen
	if index != nil {
		requiredLen = len(index)
		for _, i := range index {
			if i >= containerLen {
				return fmt.Errorf("index out of range [%d] with length %d", i, requiredLen)
			}
		}
	}

	var isNull []bool
	var ret interface{}
	if index == nil {
		ret = lambda(vc.slice, vc.isNull)
	} else {
		isNull = subsetNulls(vc.isNull, index)
		ret = lambda(subsetInterfaceSlice(vc.slice, index), isNull)
	}
	err := isSupportedSlice(ret)
	if err != nil {
		vc.isNull = cache
		return fmt.Errorf("constructing new values: %v", err)
	}
	l := reflect.ValueOf(ret).Len()
	if l != requiredLen {
		vc.isNull = cache
		return fmt.Errorf("constructing new values: new slice is not same length as original slice (%d != %d)",
			l, requiredLen)
	}
	if l == vc.len() {
		vc.slice = ret
	} else {
		err := vc.set(ret, isNull, index)
		if err != nil {
			return err
		}
	}
	vc.resetCache()
	return nil
}

func (vc *valueContainer) len() int {
	return reflect.ValueOf(vc.slice).Len()
}

func (vc *valueContainer) sort(dtype DType, ascending bool, index []int) []int {
	var srt sort.Interface
	nulls := make([]int, vc.len())
	notNulls := make([]int, vc.len())
	var sortedIsNull []bool
	var sortedIndex []int
	switch dtype {
	case Float64:
		d := vc.float64()
		d.index = index
		srt = d
		if !ascending {
			srt = sort.Reverse(srt)
		}
		sort.Stable(srt)
		sortedIsNull = d.isNull
		sortedIndex = d.index

	case String:
		d := vc.string()
		d.index = index
		srt = d
		if !ascending {
			srt = sort.Reverse(srt)
		}
		sort.Stable(srt)
		sortedIsNull = d.isNull
		sortedIndex = d.index

	case DateTime, Date, Time:
		d := vc.dateTime()
		d.index = index
		srt = d
		if !ascending {
			srt = sort.Reverse(srt)
		}
		sort.Stable(srt)
		sortedIsNull = d.isNull
		sortedIndex = d.index
	}
	// iterate over each sorted row and check whether it is null or not
	var nullCounter, validCounter int
	for i := range sortedIsNull {
		if sortedIsNull[i] {
			nulls[nullCounter] = sortedIndex[i]
			nullCounter++
		} else {
			notNulls[validCounter] = sortedIndex[i]
			validCounter++
		}
	}
	// move all null values to the bottom
	return append(notNulls[:validCounter], nulls[:nullCounter]...)
}

func sortContainers(containers []*valueContainer, sorters []Sorter) ([]int, error) {
	// initialize original index
	length := reflect.ValueOf(containers[0].slice).Len()
	originalIndex := makeIntRange(0, length)
	for i := len(sorters) - 1; i >= 0; i-- {
		index, err := indexOfContainer(sorters[i].Name, containers)
		if err != nil {
			return nil, fmt.Errorf("position %v: %v", len(sorters)-1-i, err)
		}
		// must copy the values to be sorted to avoid prematurely overwriting underlying data
		vals := containers[index].copy()
		vals.subsetRows(originalIndex)
		ascending := !sorters[i].Descending
		// pass in prior originalIndex to create new originalIndex
		originalIndex = vals.sort(sorters[i].DType, ascending, originalIndex)
	}
	// rearranging the original data by referencing these original row positions (in sequential order) will sort the series
	return originalIndex, nil
}

// indexOfContainers converts a slice of label or column names to index positions.
// If any name is not in the set of containers, returns an error
func indexOfContainers(names []string, containers []*valueContainer) ([]int, error) {
	ret := make([]int, len(names))
	for i, name := range names {
		lvl, err := indexOfContainer(name, containers)
		if err != nil {
			return nil, err
		}
		ret[i] = lvl
	}
	return ret, nil
}

// concatenateLabelsToStringsBytes reduces all container rows to a single slice of concatenated strings, one per row
func concatenateLabelsToStringsBytes(labels []*valueContainer) []string {
	for j := range labels {
		labels[j].setCache()
	}
	// is only label?
	if len(labels) == 1 {
		numRows := labels[0].len()
		ret := make([]string, numRows)
		for i := 0; i < numRows; i++ {
			ret[i] = labels[0].cache[i]
		}
		return ret
	}
	b := strings.Builder{}
	numRows := labels[0].len()
	ret := make([]string, numRows)

	for i := 0; i < numRows; i++ {
		b.Reset()
		for j := range labels {
			b.WriteString(labels[j].cache[i])
			if j != len(labels)-1 {
				b.WriteString(optionLevelSeparator)
			}
		}
		ret[i] = b.String()
	}
	// return a single slice of strings
	return ret
}

// reduceContainers reduces the containers referenced in the index
// to 1) a new []*valueContainer with slices with one unique combination of labels per row (same type as original labels),
// 2) an [][]int that maps each new row back to the rows in the original containers with the matching label combo
// and 3) a []string of the unique label combinations in order
func reduceContainers(containers []*valueContainer) (
	newContainers []*valueContainer,
	originalRowIndices [][]int,
	orderedKeys []string) {
	// coerce all label levels to string for use as map keys
	stringifiedLabels := concatenateLabelsToStringsBytes(containers)
	// create receiver for unique labels of same type as original levels
	newContainers = make([]*valueContainer, len(containers))
	for j := range containers {
		newContainers[j] = newValueContainer(
			reflect.MakeSlice(reflect.TypeOf(containers[j].slice), 0, 0).Interface(),
			make([]bool, 0),
			containers[j].name,
			containers[j].id,
		)
	}
	// create receiver for the original row indexes for each unique label combo
	uniqueLabelRows := make(map[string][]int)
	// create receiver for the unique label combos in order
	orderedKeys = make([]string, 0)
	// iterate over rows in original containers
	for i, key := range stringifiedLabels {
		// check if label combo already exists in set
		if _, ok := uniqueLabelRows[key]; !ok {
			// if label combo does not exist:
			// add int position of the original row to the map
			uniqueLabelRows[key] = []int{i}

			// write label values to new containers, in order of appearance
			for j := range containers {
				src := reflect.ValueOf(containers[j].slice).Index(i)
				dst := reflect.ValueOf(newContainers[j].slice)
				newContainers[j].slice = reflect.Append(dst, src).Interface()
				newContainers[j].isNull = append(newContainers[j].isNull, containers[j].isNull[i])
			}
			// add key to list to maintain order in which unique label combos appear
			orderedKeys = append(orderedKeys, key)

		} else {
			// if so: add int position of this row
			uniqueLabelRows[key] = append(uniqueLabelRows[key], i)
		}
	}
	// transfer row indexes in order of new unique label combos
	originalRowIndices = make([][]int, len(orderedKeys))
	for i, key := range orderedKeys {
		originalRowIndices[i] = uniqueLabelRows[key]
	}
	return
}

// returns 1) new grouped labels as []*valueContainer, and
// 2) a map[int]int that maps each original row index to its row index in the new containers
func reduceContainersForPromote(containers []*valueContainer) (
	newContainers []*valueContainer, oldToNewRowMapping map[int]int) {
	// coerce all label levels to string for use as map keys
	stringifiedLabels := concatenateLabelsToStringsBytes(containers)
	// create receiver for unique labels of same type as original levels
	newContainers = make([]*valueContainer, len(containers))
	for j := range containers {
		newContainers[j] = newValueContainer(
			reflect.MakeSlice(reflect.TypeOf(containers[j].slice), 0, 0).Interface(),
			make([]bool, 0),
			containers[j].name,
			containers[j].id,
		)
	}
	// create receiver for the original row indexes for each unique label combo
	uniqueLabelRows := make(map[string][]int)
	// create receiver for the unique label combos in order
	var numKeys int
	// key: original row index, value: new row index for the same unique label combo found at original row index
	// there will be one key for each row in the original data
	oldToNewRowMapping = make(map[int]int, len(stringifiedLabels))
	// helper map for oldToNewRowMapping - key: unique label combo, value: new row index for that unique label combo
	orderedKeyIndex := make(map[string]int)
	// iterate over rows in original containers
	for i, key := range stringifiedLabels {
		// check if label combo already exists in set
		if _, ok := uniqueLabelRows[key]; !ok {
			// if label combo does not exist:
			// add int position of the original row to the map
			uniqueLabelRows[key] = []int{i}

			// write label values to new containers, in order of appearance
			for j := range containers {
				src := reflect.ValueOf(containers[j].slice).Index(i)
				dst := reflect.ValueOf(newContainers[j].slice)
				newContainers[j].slice = reflect.Append(dst, src).Interface()
				newContainers[j].isNull = append(newContainers[j].isNull, containers[j].isNull[i])
			}
			// count the number of existing ordered keys to identify the new row index of this unique label combo
			orderedKeyIndex[key] = numKeys
			// add key to list to maintain order in which unique label combos appear
			numKeys++

		}
		// relate each row index in the old containers to a row in the new containers with deduplicated labels
		oldToNewRowMapping[i] = orderedKeyIndex[key]
	}
	return
}

// similar to reduceContainers, but only returns map of unique label combos and the row index where they first appear
func reduceContainersForLookup(containers []*valueContainer) map[string]int {
	ret := make(map[string]int)
	stringifiedLabels := concatenateLabelsToStringsBytes(containers)
	for i, key := range stringifiedLabels {
		if _, ok := ret[key]; !ok {
			ret[key] = i
		}
	}
	return ret
}

func splitNameIntoLevels(name string) []string {
	return strings.Split(name, optionLevelSeparator)
}

func joinLevelsIntoName(levels []string) string {
	return strings.Join(levels, optionLevelSeparator)
}

// if labels1[i] is in labels2, ret[i] = the row position in labels2 that first matches labels[i].
// if no match, ret[i] = -1
func matchLabelPositions(labels1 []string, labels2 map[string]int) []int {
	ret := make([]int, len(labels1))
	for i, key := range labels1 {
		if val, ok := labels2[key]; ok {
			ret[i] = val
		} else {
			ret[i] = -1
		}
	}
	return ret
}

func (s *Series) combineMath(other *Series, ignoreNulls bool, fn func(v1 float64, v2 float64) float64) *Series {
	retFloat := make([]float64, s.Len())
	retIsNull := make([]bool, s.Len())
	originalFloat := s.values.float64().slice
	originalNulls := s.values.isNull
	lookupVals, _ := s.Lookup(other)
	otherFloat := lookupVals.values.float64().slice
	otherNulls := lookupVals.values.isNull

	for i := range originalFloat {
		// handle null lookup
		if (otherNulls[i] || originalNulls[i]) && !ignoreNulls {
			retFloat[i] = 0
			retIsNull[i] = true
			continue
		}
		if otherNulls[i] {
			retFloat[i] = originalFloat[i]
			retIsNull[i] = originalNulls[i]
			continue
		} else if originalNulls[i] {
			retFloat[i] = otherFloat[i]
			retIsNull[i] = otherNulls[i]
			continue
		}
		// actual combination logic
		combinedFloat := fn(originalFloat[i], otherFloat[i])
		// handle division by 0
		if math.IsNaN(combinedFloat) || math.IsInf(combinedFloat, 0) {
			retIsNull[i] = true
		} else {
			retFloat[i] = combinedFloat
			retIsNull[i] = originalNulls[i]
		}
	}
	// copy the labels to avoid sharing data with derivative Series
	return &Series{
		values: newValueContainer(retFloat, retIsNull, s.values.name),
		labels: copyContainers(s.labels)}
}

func lookup(how string,
	values1 *valueContainer, labels1 []*valueContainer, leftOn []int,
	values2 *valueContainer, labels2 []*valueContainer, rightOn []int) (*Series, error) {
	switch how {
	case "left":
		return lookupWithAnchor(values1.name, labels1, leftOn, values2, labels2, rightOn), nil
	case "right":
		return lookupWithAnchor(values2.name, labels2, rightOn, values1, labels1, leftOn), nil
	case "inner":
		s := lookupWithAnchor(values1.name, labels1, leftOn, values2, labels2, rightOn)
		s = s.DropNull()
		return s, nil
	default:
		return nil, fmt.Errorf("how: must be left, right, or inner")
	}
}

func lookupDataFrame(how string,
	name string, colLevelNames []string,
	values1 []*valueContainer, labels1 []*valueContainer, leftOn []int,
	values2 []*valueContainer, labels2 []*valueContainer, rightOn []int,
	excludeLeft []string, excludeRight []string) (*DataFrame, error) {
	mergedLabelsCols1 := append(labels1, values1...)
	mergedLabelsCols2 := append(labels2, values2...)
	switch how {
	case "left":
		return lookupDataFrameWithAnchor(name, colLevelNames, labels1,
			mergedLabelsCols1, leftOn,
			mergedLabelsCols2, rightOn,
			values2, excludeRight), nil
	case "right":
		return lookupDataFrameWithAnchor(name, colLevelNames, labels2,
			mergedLabelsCols2, rightOn,
			mergedLabelsCols1, leftOn,
			values1, excludeLeft), nil
	case "inner":
		df := lookupDataFrameWithAnchor(name, colLevelNames, labels1,
			mergedLabelsCols1, leftOn,
			mergedLabelsCols2, rightOn,
			values2, excludeRight)
		df = df.DropNull()
		return df, nil
	default:
		return nil, fmt.Errorf("how: must be left, right, or inner")
	}
}

// lookupWithAnchor subsets sourceLabels by leftOn and lookupLabels by rightOn,
// and finds aligned rows between the containers.
// for every aligned row, looks up the value in lookupValues.
// returns a Series that is anchored on sourceLabels and is named name.
func lookupWithAnchor(
	name string, sourceLabels []*valueContainer, leftOn []int,
	lookupValues *valueContainer, lookupLabels []*valueContainer, rightOn []int) *Series {

	subsetLeft, _ := subsetContainers(sourceLabels, leftOn)
	subsetRight, _ := subsetContainers(lookupLabels, rightOn)
	if reflect.DeepEqual(subsetLeft, subsetRight) {
		return &Series{
			values: lookupValues.copy(),
			labels: copyContainers(sourceLabels),
		}
	}

	toLookup := concatenateLabelsToStringsBytes(subsetLeft)
	lookupSource := reduceContainersForLookup(subsetRight)
	matches := matchLabelPositions(toLookup, lookupSource)
	reflectLookup := reflect.ValueOf(lookupValues.slice)
	isNull := make([]bool, len(matches))
	// return type is set to same type as within lookupSource
	vals := reflect.MakeSlice(reflectLookup.Type(), len(matches), len(matches))
	for i, matchedIndex := range matches {
		// positive match: copy value from values2
		dst := vals.Index(i)
		if matchedIndex != -1 {
			src := reflectLookup.Index(matchedIndex)
			dst.Set(src)
			isNull[i] = lookupValues.isNull[matchedIndex]
			// no match: set to zero value
		} else {
			isNull[i] = true
		}
	}
	return &Series{
		values: newValueContainer(vals.Interface(), isNull, name),
		labels: copyContainers(sourceLabels),
	}
}

// lookupDataFrameWithAnchor subsets sourceContainers by leftOn and lookupContainers by rightOn,
// and finds aligned rows between the containers.
// for every aligned row, looks up the value in every column in lookupColumns (excluding colNames within exclude).
// returns a dataframe that is anchored on originalLabels, preserves the column names from lookupColumns,
// preserves the original column level names, and is named name.
func lookupDataFrameWithAnchor(
	name string, colLevelNames []string, originalLabels []*valueContainer,
	sourceContainers []*valueContainer, leftOn []int,
	lookupContainers []*valueContainer, rightOn []int,
	lookupColumns []*valueContainer, exclude []string) *DataFrame {

	subsetLeft, _ := subsetContainers(sourceContainers, leftOn)
	subsetRight, _ := subsetContainers(lookupContainers, rightOn)
	if reflect.DeepEqual(subsetLeft, subsetRight) {
		return &DataFrame{
			values:        copyContainers(lookupColumns),
			labels:        copyContainers(sourceContainers),
			name:          name,
			colLevelNames: colLevelNames,
		}
	}
	toLookup := concatenateLabelsToStringsBytes(subsetLeft)
	lookupSource := reduceContainersForLookup(subsetRight)
	// list of aligned rows
	matches := matchLabelPositions(toLookup, lookupSource)
	// slice of slices
	var retVals []*valueContainer
	for k := range lookupColumns {
		var skip bool
		for _, colToExclude := range exclude {
			// skip any column whose name is also used in the lookup
			if lookupColumns[k].name == colToExclude {
				skip = true
			}
		}
		if skip {
			continue
		}
		v := reflect.ValueOf(lookupColumns[k].slice)
		isNull := make([]bool, len(matches))
		// return type is set to same type as within lookupSource
		vals := reflect.MakeSlice(v.Type(), len(matches), len(matches))
		for i, matchedIndex := range matches {
			// positive match: copy value from values2
			dst := vals.Index(i)
			if matchedIndex != -1 {
				src := v.Index(matchedIndex)
				dst.Set(src)
				isNull[i] = lookupColumns[k].isNull[matchedIndex]
				// no match
			} else {
				isNull[i] = true
			}
		}
		retVals = append(
			retVals,
			newValueContainer(vals.Interface(), isNull, lookupColumns[k].name),
		)
	}
	// copy labels to avoid sharing data accidentally
	return &DataFrame{
		values:        retVals,
		labels:        copyContainers(originalLabels),
		name:          name,
		colLevelNames: colLevelNames,
	}
}

func (vc *valueContainer) dropRow(index int) error {
	v := reflect.ValueOf(vc.slice)
	l := v.Len()
	if index >= l {
		return fmt.Errorf("index out of range [%d] with length %d", index, l)
	}
	retIsNull := append(vc.isNull[:index], vc.isNull[index+1:]...)
	retVals := reflect.MakeSlice(v.Type(), 0, 0)
	retVals = reflect.AppendSlice(v.Slice(0, index), v.Slice(index+1, l))

	vc.slice = retVals.Interface()
	vc.isNull = retIsNull
	vc.resetCache()
	return nil
}

func dropFromContainers(name string, containers []*valueContainer) ([]*valueContainer, error) {
	toExclude, err := indexOfContainer(name, containers)
	if err != nil {
		return nil, err
	}
	if len(containers) == 1 {
		return nil, fmt.Errorf("cannot drop only container")
	}
	return append(containers[:toExclude], containers[toExclude+1:]...), nil
}

func excludeFromIndex(indexLength int, item int) []int {
	var ret []int
	for i := 0; i < indexLength; i++ {
		if i != item {
			ret = append(ret, i)
		}
	}
	return ret
}

func copyNulls(isNull []bool) []bool {
	ret := make([]bool, len(isNull))
	copy(ret, isNull)
	return ret
}

func copyContainers(containers []*valueContainer) []*valueContainer {
	ret := make([]*valueContainer, len(containers))
	for k := range containers {
		ret[k] = containers[k].copy()
	}
	return ret
}

func copyInterface(i interface{}) interface{} {
	v := reflect.ValueOf(i)
	l := v.Len()
	switch i.(type) {
	case []float64:
		vals := make([]float64, l)
		copy(vals, i.([]float64))
		return vals
	case []string:
		vals := make([]string, l)
		copy(vals, i.([]string))
		return vals
	case []time.Time:
		vals := make([]time.Time, l)
		copy(vals, i.([]time.Time))
		return vals
	case []int:
		vals := make([]int, l)
		copy(vals, i.([]int))
		return vals
	default:
		vals := reflect.MakeSlice(v.Type(), l, l)
		for i := 0; i < v.Len(); i++ {
			ptr := vals.Index(i)
			ptr.Set(v.Index(i))
		}
		return vals.Interface()
	}
}

func (vc *valueContainer) copy() *valueContainer {
	return &valueContainer{
		slice:  copyInterface(vc.slice),
		isNull: copyNulls(vc.isNull),
		name:   vc.name,
		id:     vc.id,
		cache:  copyCache(vc.cache),
	}
}
func copyCache(input []string) []string {
	if input == nil {
		return nil
	}
	ret := make([]string, len(input))
	copy(ret, input)
	return ret
}

func isNullFloat(v float64) bool {
	if optionNaNIsNull {
		if math.IsNaN(v) {
			return true
		}
	}
	return false
}

func isSupportedSlice(slice interface{}) error {
	if k := reflect.TypeOf(slice).Kind(); k != reflect.Slice {
		return fmt.Errorf("unsupported kind (%v), must be slice", k)
	}
	if reflect.ValueOf(slice).Len() == 0 {
		return fmt.Errorf("empty slice: cannot be empty")
	}
	return nil
}

func setNullsFromInterface(input interface{}) ([]bool, error) {
	if input == nil {
		return []bool{}, nil
	}
	var ret []bool
	err := isSupportedSlice(input)
	if err != nil {
		return nil, fmt.Errorf("setting null values from interface{}: %v", err)
	}
	v := reflect.ValueOf(input)
	// map or nested slice
	// if len is empty -> null (this includes [][]byte, instead of treating them as strings)
	t := v.Index(0).Kind()
	if t == reflect.Map || t == reflect.Slice {
		l := v.Len()
		ret = make([]bool, l)
		for i := range ret {
			ret[i] = (v.Index(i).Len() == 0)
		}
		return ret, nil
	}

	switch input.(type) {
	case []float64:
		vals := input.([]float64)
		ret = make([]bool, len(vals))
		for i := range ret {
			ret[i] = isNullFloat(vals[i])
		}

	case []string:
		vals := input.([]string)
		ret = make([]bool, len(vals))
		for i := range ret {
			if isNullString(vals[i]) {
				ret[i] = true
			} else {
				ret[i] = false
			}
		}
	case []time.Time:
		vals := input.([]time.Time)
		ret = make([]bool, len(vals))
		for i := range ret {
			if (time.Time{}) == vals[i] {
				ret[i] = true
			} else {
				ret[i] = false
			}
		}

	case []interface{}:
		vals := input.([]interface{})
		ret = make([]bool, len(vals))
		for i := range vals {
			null := isNullInterface(vals[i])
			if null {
				ret[i] = true
			} else {
				ret[i] = false
			}
		}
	case []civil.Date:
		vals := input.([]civil.Date)
		ret = make([]bool, len(vals))
		for i := range ret {
			if !vals[i].IsValid() {
				ret[i] = true
			} else {
				ret[i] = false
			}
		}
	case []civil.Time:
		vals := input.([]civil.Time)
		ret = make([]bool, len(vals))
		for i := range ret {
			if !vals[i].IsValid() {
				ret[i] = true
			} else {
				ret[i] = false
			}
		}
	default:
		// all other types are considered non-null
		l := reflect.ValueOf(input).Len()
		ret = make([]bool, l)
		for i := range ret {
			ret[i] = false
		}
	}
	return ret, nil
}

func isNullInterface(i interface{}) bool {
	if i == nil {
		return true
	}
	if v := reflect.ValueOf(i); v.Kind() == reflect.Slice {
		if v.Len() == 0 {
			return true
		}
		return false
	}
	switch i.(type) {
	case float64:
		f := i.(float64)
		if isNullFloat(f) {
			return true
		}
	case string:
		s := i.(string)
		if isNullString(s) {
			return true
		}
	case time.Time:
		t := i.(time.Time)
		if (time.Time{}) == t {
			return true
		}
	case civil.Date:
		t := i.(civil.Date)
		if !t.IsValid() {
			return true
		}
	case civil.Time:
		t := i.(civil.Time)
		if !t.IsValid() {
			return true
		}
	}
	return false
}

func isNullString(s string) bool {
	if _, ok := optionNullStrings.Read()[s]; !ok {
		return false
	}
	return true
}

// math

// sum sums the non-null values at the index positions in vals. If all values are null, the final result is null.
// Compatible with Grouped calculations as well as Series
func sum(vals []float64, isNull []bool, index []int) (float64, bool) {
	var sum float64
	var atLeastOneValid bool
	for _, i := range index {
		if !isNull[i] {
			sum += vals[i]
			atLeastOneValid = true
		}
	}
	if !atLeastOneValid {
		return 0, true
	}
	return sum, false
}

// mean calculates the mean of the non-null values at the index positions in vals.
// If all values are null, the final result is null.
// Compatible with Grouped calculations as well as Series
func mean(vals []float64, isNull []bool, index []int) (float64, bool) {
	var sum float64
	var counter float64
	var atLeastOneValid bool
	for _, i := range index {
		if !isNull[i] {
			sum += vals[i]
			counter++
			atLeastOneValid = true
		}
	}
	if !atLeastOneValid {
		return 0, true
	}
	return sum / counter, false
}

// median calculates the median of the non-null values at the index positions in vals.
// If all values are null, the final result is null.
// Compatible with Grouped calculations as well as Series
func median(vals []float64, isNull []bool, index []int) (float64, bool) {
	data := make([]float64, 0)
	for _, i := range index {
		if !isNull[i] {
			data = append(data, vals[i])
		}
	}
	sort.Float64s(data)
	if len(data) == 0 {
		return 0, true
	}
	// rounds down if there are even number of elements
	mNumber := len(data) / 2

	// odd number of elements
	if len(data)%2 != 0 {
		return data[mNumber], false
	}
	// even number of elements
	return (data[mNumber-1] + data[mNumber]) / 2, false
}

// std calculates the standard deviation of the non-null values at the index positions in vals.
// If all values are null, the final result is null.
// Compatible with Grouped calculations as well as Series
func std(vals []float64, isNull []bool, index []int) (float64, bool) {
	mean, _ := mean(vals, isNull, index)
	var variance, counter float64
	var atLeastOneValid bool
	for _, i := range index {
		if !isNull[i] {
			variance += math.Pow((vals[i] - mean), 2)
			counter++
			atLeastOneValid = true
		}
	}
	if !atLeastOneValid {
		return 0, true
	}
	return math.Pow(variance/counter, 0.5), false
}

// count counts the non-null values at the index positions in vals.
// Compatible with Grouped calculations as well as Series
func count(vals interface{}, isNull []bool, index []int) (int, bool) {
	var counter int
	var atLeastOneValid bool
	for _, i := range index {
		if !isNull[i] {
			counter++
			atLeastOneValid = true
		}
	}
	if !atLeastOneValid {
		return 0, true
	}
	return counter, false
}

func nunique(vals interface{}, isNull []bool, index []int) (int, bool) {
	m := make(map[string]bool)
	var atLeastOneValid bool
	v := reflect.ValueOf(vals)
	for _, i := range index {
		if !isNull[i] {
			key := v.Index(i).Interface()
			stringifiedKey := fmt.Sprint(key)
			if _, ok := m[stringifiedKey]; !ok {
				m[stringifiedKey] = true
			}
			atLeastOneValid = true
		}
	}
	if !atLeastOneValid {
		return 0, true
	}
	return len(m), false
}

// min returns the min of the non-null values at the index positions in vals.
// Compatible with Grouped calculations as well as Series
func min(vals []float64, isNull []bool, index []int) (float64, bool) {
	min := math.Inf(0)
	var atLeastOneValid bool
	for _, i := range index {
		if !isNull[i] {
			if vals[i] < min {
				min = vals[i]
			}
			atLeastOneValid = true
		}
	}
	if !atLeastOneValid {
		return 0, true
	}
	return min, false
}

// max returns the max of the non-null values at the index positions in vals.
// Compatible with Grouped calculations as well as Series
func max(vals []float64, isNull []bool, index []int) (float64, bool) {
	max := math.Inf(-1)
	var atLeastOneValid bool
	for _, i := range index {
		if !isNull[i] {
			if vals[i] > max {
				max = vals[i]
			}
			atLeastOneValid = true
		}
	}
	if !atLeastOneValid {
		return 0, true
	}
	return max, false
}

// earliest returns the earliest of the non-null values at the index positions in vals.
// Compatible with Grouped calculations as well as Series
func earliest(vals []time.Time, isNull []bool, index []int) (time.Time, bool) {
	min := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	var atLeastOneValid bool
	for _, i := range index {
		if !isNull[i] {
			if vals[i].Before(min) {
				min = vals[i]
			}
			atLeastOneValid = true
		}
	}
	if !atLeastOneValid {
		return time.Time{}, true
	}
	return min, false
}

// latest returns the latest of the non-null values at the index positions in vals.
// Compatible with Grouped calculations as well as Series
func latest(vals []time.Time, isNull []bool, index []int) (time.Time, bool) {
	max := time.Time{}
	var atLeastOneValid bool
	for _, i := range index {
		if !isNull[i] {
			if vals[i].After(max) {
				max = vals[i]
			}
			atLeastOneValid = true
		}
	}
	if !atLeastOneValid {
		return time.Time{}, true
	}
	return max, false
}

// cumsum is an aligned function, meaning it aligns with the original rows
func cumsum(vals []float64, isNull []bool, index []int) []float64 {
	ret := make([]float64, len(index))
	var cumsum float64
	for incrementor, i := range index {
		if !isNull[i] {
			cumsum += vals[i]
		}
		ret[incrementor] = cumsum
	}
	return ret
}

func (filter FilterFn) validate() error {
	if filter == nil {
		return fmt.Errorf("no filter function provided")
	}
	return nil
}

func (lambda ApplyFn) validate() error {
	if lambda == nil {
		return fmt.Errorf("no apply function provided")
	}
	return nil
}

func (lambda ReduceFn) validate() error {
	if lambda == nil {
		return fmt.Errorf("no reduce function provided")
	}
	return nil
}

// left-exclusive, right-inclusive by default
// expects vals and isNull to be same length
func cut(vals []float64, isNull []bool,
	bins []float64, leftInclusive bool, rightExclusive bool,
	includeLess, includeMore bool, labels []string) ([]string, error) {
	if len(bins) == 0 {
		return nil, fmt.Errorf("must supply at least one bin edge")
	}
	originalBinCount := len(bins)
	var useDefaultLabels bool
	// create default labels
	if len(labels) == 0 {
		useDefaultLabels = true
		labels = make([]string, len(bins)-1)
		// do not iterate over the last edge to avoid range error
		for i := 0; i < len(bins)-1; i++ {
			labels[i] = fmt.Sprintf("%v-%v", bins[i], bins[i+1])
		}
	}
	if includeLess {
		if useDefaultLabels {
			str := "<=%v"
			if leftInclusive {
				str = "<%v"
			}
			labels = append([]string{fmt.Sprintf(str, bins[0])}, labels...)
		}
		bins = append([]float64{math.Inf(-1)}, bins...)
	}
	if includeMore {
		if useDefaultLabels {
			str := ">%v"
			if rightExclusive {
				str = ">=%v"
			}
			labels = append(labels, fmt.Sprintf(str, bins[len(bins)-1]))
		}
		bins = append(bins, math.Inf(1))
	}
	// validate correct number of bins
	var andLessEdge, andMoreEdge int
	if includeLess {
		andLessEdge = 1
	}
	if includeMore {
		andMoreEdge = 1
	}
	if len(bins)-1 != len(labels) {
		return nil, fmt.Errorf("number of bin edges (+ includeLess + includeMore), "+
			"must be one more than number of supplied labels: (%d + %d + %d) != (%d + 1)",
			originalBinCount, andLessEdge, andMoreEdge, len(labels))
	}
	// write final results
	ret := make([]string, len(vals))
	for i, val := range vals {
		if isNull[i] {
			ret[i] = optionsNullPrinter
			continue
		}
		// check for value within every bin interval
		// do not iterate over the last edge to avoid range error
		// check if value is within bin
		var found bool
		for binEdge := 0; binEdge < len(bins)-1; binEdge++ {
			if leftInclusive && rightExclusive {
				if val >= bins[binEdge] && val < bins[binEdge+1] {
					ret[i] = labels[binEdge]
					found = true
					break
				}
			} else if !leftInclusive && !rightExclusive {
				// default: leftExclusive and rightInclusive
				if val > bins[binEdge] && val <= bins[binEdge+1] {
					ret[i] = labels[binEdge]
					found = true
					break
				}
			} else {
				// avoid this scenario
				return nil, fmt.Errorf("internal error: bad cut() conditions")
			}
		}
		if !found {
			ret[i] = optionsNullPrinter
		}
	}
	return ret, nil
}

// by default: leftExclusive, rightInclusive
func (vc *valueContainer) cut(bins []float64, includeLess, includeMore bool, labels []string) ([]string, error) {
	leftInclusive := true
	rightExclusive := true
	return cut(vc.float64().slice, vc.isNull, bins, !leftInclusive, !rightExclusive, includeLess, includeMore, labels)
}

func (vc *floatValueContainer) rank() []float64 {
	ret := make([]float64, len(vc.slice))
	sort.Stable(vc)
	var offset float64
	// iterate over sorted values and write results back to []float aligned with original
	// the incrementor here is the naive ranking, but must be adjsuted for nulls and duplicates
	for i := range vc.slice {
		originalPosition := vc.index[i]
		// ranking is 1-based, index is 0-based
		rank := float64(i) + 1

		// handle null
		if vc.isNull[i] {
			ret[originalPosition] = -999
			offset--
			continue
		}
		// no duplicates prior to first value
		if i == 0 {
			ret[originalPosition] = rank
			continue
		}
		// handle duplicates
		if vc.slice[i] == vc.slice[i-1] {
			priorOriginal := vc.index[i-1]
			// if duplicate, look up original position immediately prior and increment the offset
			ret[originalPosition] = ret[priorOriginal]
			offset--
		} else {
			// reduce the rank by the offset amount
			ret[originalPosition] = rank + offset
		}
	}
	return ret
}

// null is returned as -999
func rank(vals []float64, isNull []bool, index []int) []float64 {

	// copy all existing values at index positions
	newVals := make([]float64, len(index))
	newIsNull := make([]bool, len(index))
	for i := range index {
		newVals[i] = vals[i]
		newIsNull[i] = isNull[i]
	}
	// sort floats
	floats := &floatValueContainer{slice: newVals, index: makeIntRange(0, len(index)), isNull: newIsNull}
	return floats.rank()
}

func (vc *floatValueContainer) percentile() []float64 {
	ret := make([]float64, len(vc.slice))
	var validCount int
	for i := range vc.isNull {
		if !vc.isNull[i] {
			validCount++
		}
	}
	rank := vc.rank()

	var counter int
	for i := range rank {
		originalPosition := vc.index[i]
		percentile := float64(counter) / float64(validCount)
		// handle null
		if vc.isNull[i] {
			ret[originalPosition] = -999
			continue
		}
		// no duplicates in first row
		if i == 0 {
			ret[originalPosition] = percentile
			counter++
			continue
		}
		// handle repeat ranks (ie duplicate rows)
		if vc.slice[i] == vc.slice[i-1] {
			priorOriginal := vc.index[i-1]
			ret[originalPosition] = ret[priorOriginal]
		} else {
			ret[originalPosition] = percentile
		}
		counter++
	}
	return ret
}

// exclusive definition: what % of all values are below this value
// -999 for null values
func (vc *valueContainer) pcut(bins []float64, config *Binner) ([]string, error) {
	for i, edge := range bins {
		if edge < 0 || edge > 1 {
			return nil, fmt.Errorf("all bin edges must be between 0 and 1 (%v at edge %d", edge, i)
		}
	}
	if config == nil {
		config = &Binner{}
	}
	// copy to avoid sorting the underlying values
	floats := vc.copy().float64()
	floats.index = makeIntRange(0, vc.len())
	pctile := floats.percentile()
	leftInclusive := true
	rightExclusive := true
	return cut(pctile, vc.isNull, bins, leftInclusive, rightExclusive, config.AndLess, config.AndMore, config.Labels)
}

func withinWindow(root time.Time, other time.Time, d time.Duration) bool {
	// window = [root, root+d)
	// if the other time is before root time, it is not within the window
	if other.Before(root) {
		return false
	}
	// if the other time is on or after the root + duration, it is not within the window
	if other.Equal(root.Add(d)) || other.After(root.Add(d)) {
		return false
	}
	return true
}

func resample(t time.Time, by Resampler) time.Time {
	if by.ByYear {
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, by.Location)
	} else if by.ByMonth {
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, by.Location)
	} else if by.ByDay {
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, by.Location)
	} else if by.ByWeek {
		day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, by.Location)
		daysSinceStartOfWeek := day.Weekday() - by.StartOfWeek
		if daysSinceStartOfWeek >= 0 {
			// subtract days back to beginning of week
			return day.AddDate(0, 0, int(daysSinceStartOfWeek)*-1)
		}
		// add days to get to start of new week, then subtract a full week
		return day.AddDate(0, 0, (int(daysSinceStartOfWeek)*-1)-7)
	} else {
		return t.Truncate(by.ByDuration)
	}
}

func (vc *valueContainer) resample(by Resampler) {
	var isCivilDate, isCivilTime bool
	switch vc.slice.(type) {
	case []civil.Date:
		isCivilDate = true
	case []civil.Time:
		isCivilTime = true
	}
	vals := vc.dateTime().slice
	truncatedVals := make([]time.Time, len(vals))
	if by.Location == nil {
		by.Location = time.UTC
	}
	for i := range vals {
		t := vals[i].In(by.Location)
		truncatedVals[i] = resample(t, by)
	}
	if isCivilDate {
		retVals := make([]civil.Date, len(vals))
		for i := range truncatedVals {
			retVals[i] = civil.DateOf(truncatedVals[i])
		}
		vc.slice = retVals
	} else if isCivilTime {
		retVals := make([]civil.Time, len(vals))
		for i := range vals {
			retVals[i] = civil.TimeOf(truncatedVals[i])
		}
		vc.slice = retVals
	} else {
		vc.slice = truncatedVals
	}

	vc.resetCache()
	return
}

func (vc *valueContainer) valueCounts() map[string]int {
	v := vc.string().slice
	m := make(map[string]int)
	for i := range v {
		// skip nulls
		if vc.isNull[i] {
			continue
		}
		if _, ok := m[v[i]]; !ok {
			m[v[i]] = 1
		} else {
			m[v[i]]++
		}
	}
	return m
}

func deduplicateContainerNames(containers []*valueContainer) {
	m := make(map[string]int)
	for k := range containers {
		name := containers[k].name
		if n, ok := m[name]; !ok {
			m[name] = 1
		} else {
			containers[k].name = fmt.Sprintf("%v_%v", containers[k].name, n)
			m[name]++
		}
	}
}

// returns the first row position each value appears
func (vc *valueContainer) uniqueIndex() []int {
	m := make(map[string]bool)
	ret := make([]int, 0)
	vc.setCache()
	for i, value := range vc.cache {
		if _, ok := m[string(value)]; !ok {
			m[string(value)] = true
			ret = append(ret, i)
		}
	}
	return ret
}

// returns the first row position each combination of values appears (accounting for all container values)
func multiUniqueIndex(containers []*valueContainer) []int {
	stringifiedRows := concatenateLabelsToStringsBytes(containers)
	m := make(map[string]bool)
	ret := make([]int, 0)
	for i, value := range stringifiedRows {
		if _, ok := m[value]; !ok {
			m[value] = true
			ret = append(ret, i)
		}
	}
	return ret
}

func (vc *valueContainer) dtype() reflect.Type {
	return reflect.TypeOf(vc.slice)
}

// -- equality checks

// EqualSeries returns whether two Series are identical or not.
func EqualSeries(a, b *Series) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if (a == nil) && (b == nil) {
		return true
	}
	if !reflect.DeepEqual(a.labels, b.labels) {
		return false
	}
	if !reflect.DeepEqual(a.values, b.values) {
		return false
	}
	if a.sharedData != b.sharedData {
		return false
	}
	if (a.err == nil) != (b.err == nil) {
		return false
	}
	if a.err != nil {
		if a.err.Error() != b.err.Error() {
			return false
		}
	}
	return true
}

func seriesIsDistinct(a, b *Series) bool {
	if reflect.ValueOf(a.labels).Pointer() == reflect.ValueOf(b.labels).Pointer() {
		return false
	}
	// compare individual label pointer addresses
	for j := range a.labels {
		if j < len(b.labels) {
			if a.labels[j] == b.labels[j] {
				return false
			}
		}
	}
	if a.values == b.values {
		return false
	}
	return true
}

func equalGroupedSeries(a, b *GroupedSeries) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if (a == nil) && (b == nil) {
		return true
	}
	if !reflect.DeepEqual(a.orderedKeys, b.orderedKeys) {
		return false
	}
	if !reflect.DeepEqual(a.rowIndices, b.rowIndices) {
		return false
	}
	if !reflect.DeepEqual(a.labels, b.labels) {
		return false
	}
	if (a.series == nil) != (b.series == nil) {
		return false
	}
	if a.series != nil {
		if !EqualSeries(a.series, b.series) {
			return false
		}
	}
	if (a.err == nil) != (b.err == nil) {
		return false
	}
	if a.err != nil {
		if a.err.Error() != b.err.Error() {
			return false
		}
	}
	return true
}

// EqualDataFrames returns whether two dataframes are identical or not.
func EqualDataFrames(a, b *DataFrame) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if (a == nil) && (b == nil) {
		return true
	}
	if !reflect.DeepEqual(a.labels, b.labels) {
		return false
	}
	if !reflect.DeepEqual(a.values, b.values) {
		return false
	}
	if !reflect.DeepEqual(a.colLevelNames, b.colLevelNames) {
		return false
	}
	if a.name != b.name {
		return false
	}
	if (a.err == nil) != (b.err == nil) {
		return false
	}
	if a.err != nil {
		if a.err.Error() != b.err.Error() {
			return false
		}
	}
	return true
}

func equalGroupedDataFrames(a, b *GroupedDataFrame) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if (a == nil) && (b == nil) {
		return true
	}
	if !reflect.DeepEqual(a.orderedKeys, b.orderedKeys) {
		return false
	}
	if !reflect.DeepEqual(a.rowIndices, b.rowIndices) {
		return false
	}
	if !reflect.DeepEqual(a.labels, b.labels) {
		return false
	}
	if (a.df == nil) != (b.df == nil) {
		return false
	}
	if a.df != nil {
		if !EqualDataFrames(a.df, b.df) {
			return false
		}
	}
	if (a.err == nil) != (b.err == nil) {
		return false
	}
	if a.err != nil {
		if a.err.Error() != b.err.Error() {
			return false
		}
	}
	return true
}

func dataFrameIsDistinct(a, b *DataFrame) bool {
	if reflect.ValueOf(a.labels).Pointer() == reflect.ValueOf(b.labels).Pointer() {
		return false
	}
	if reflect.ValueOf(a.values).Pointer() == reflect.ValueOf(b.values).Pointer() {
		return false
	}
	if reflect.ValueOf(a.colLevelNames).Pointer() == reflect.ValueOf(b.colLevelNames).Pointer() {
		return false
	}
	// compare individual label pointer addresses
	for j := range a.labels {
		if j < len(b.labels) {
			if a.labels[j] == b.labels[j] {
				return false
			}
		}
	}
	// compare individual value pointer addresses
	for k := range a.values {
		if k < len(b.values) {
			if a.values[k] == b.values[k] {
				return false
			}
		}
	}
	return true
}

// does not skip comment lines or blank lines
// parses the first row only to get number of fields
func extractCSVDimensions(b []byte, comma rune) (numRows, numCols int, err error) {
	numRows = bytes.Count(b, []byte{'\n'})
	// no trailing \n?
	if len(b) > 0 && b[len(b)-1] != '\n' {
		numRows++
	}
	// subtract empty rows (back-to-back newlines)
	emptyRows := bytes.Count(b, []byte{'\n', '\n'})
	numRows -= emptyRows
	r := bytes.NewReader(b)
	csvReader := csv.NewReader(r)
	csvReader.Comma = comma
	fields, err := csvReader.Read()
	if err != nil {
		return 0, 0, err
	}
	numCols = len(fields)
	return numRows, numCols, nil
}

func filter(containers []*valueContainer, filters map[string]FilterFn) ([]int, error) {
	// subIndexes contains the index positions computed across all the filters
	var subIndexes [][]int
	for containerName, filter := range filters {
		err := filter.validate()
		if err != nil {
			return nil, err
		}
		position, err := indexOfContainer(containerName, containers)
		if err != nil {
			return nil, err
		}
		subIndex := containers[position].filter(filter)
		subIndexes = append(subIndexes, subIndex)
	}
	intersection := intersection(subIndexes,
		reflect.ValueOf(containers[0].slice).Len())
	// reduce the subindexes to a single index that shares all the values
	return intersection, nil
}

func removeDefaultNameIndicator(name string) string {
	return regexp.MustCompile(`^\*`).ReplaceAllString(name, "")
}

func suppressDefaultName(name string) string {
	if regexp.MustCompile(`^\*`).MatchString(name) {
		return "-"
	}
	return name
}

func filterByValue(containers []*valueContainer, values map[string]interface{}) ([]int, error) {
	subIndexes := make([][]int, len(values))
	var incrementor int
	for k, v := range values {
		i, err := indexOfContainer(k, containers)
		if err != nil {
			return nil, err
		}
		subIndex := containers[i].indexOfRows(v)
		subIndexes[incrementor] = subIndex
		incrementor++
	}
	index := intersection(subIndexes, containers[0].len())
	return index, nil
}

func groupCounts(rowIndices [][]int) []int {
	ret := make([]int, len(rowIndices))
	for i := range rowIndices {
		ret[i] = len(rowIndices[i])
	}
	return ret
}

func rowCount(rowIndices [][]int) int {
	var ret int
	for i := range rowIndices {
		ret += len(rowIndices[i])
	}
	return ret
}

// expand a valueContainer by repeating each value n times, where n is aligned to value's index position
func (vc *valueContainer) expand(n []int) *valueContainer {
	var length int
	for i := range n {
		length += n[i]
	}

	vals := reflect.MakeSlice(reflect.TypeOf(vc.slice), length, length)
	nulls := make([]bool, length)

	v := reflect.ValueOf(vc.slice)
	var counter int
	for i := 0; i < v.Len(); i++ {
		src := v.Index(i)
		isNull := vc.isNull[i]
		for repeat := 0; repeat < n[i]; repeat++ {
			dst := vals.Index(counter)
			dst.Set(src)

			if isNull {
				nulls[counter] = true
			}
			counter++
		}
	}
	ret := newValueContainer(
		vals.Interface(),
		nulls,
		vc.name,
		vc.id,
	)
	return ret
}

// convert vc.slice to []interface
func (vc *valueContainer) interfaceSlice() []interface{} {
	v := reflect.ValueOf(vc.slice)
	ret := make([]interface{}, v.Len())
	for i := range ret {
		if vc.isNull[i] {
			ret[i] = nil
		} else {
			ret[i] = v.Index(i).Interface()
		}
	}
	return ret
}

// -- private
type clocker interface {
	now() time.Time
}

type realClock struct{}

func (c realClock) now() time.Time {
	return time.Now()
}

func writeRecords(containers []*valueContainer, byColumn bool, numColLevels int) [][]string {
	ret := make([][]string, len(containers))
	for k := range containers {
		headerSlots := make([]string, numColLevels)
		// len(headers) should never be > numColLevels()
		// if len(headers) < numColLevels(), excess header rows will remain blank
		headers := splitNameIntoLevels(containers[k].name)
		for l := range headers {
			headerSlots[l] = headers[l]
		}
		ret[k] = append(headerSlots, containers[k].string().slice...)
	}
	// overwrite null values, skipping headers
	for k := range ret {
		for i := range ret[k][numColLevels:] {
			if containers[k].isNull[i] {
				ret[k][i+numColLevels] = optionsNullPrinter
			}
		}
	}
	if !byColumn {
		ret = transposeRecords(ret)
	}
	return ret
}

func writeInterfaceRecords(containers []*valueContainer, byColumn bool, numColLevels int) [][]interface{} {
	ret := make([][]interface{}, len(containers))
	for k := range containers {
		headerSlots := make([]interface{}, numColLevels)
		// len(headers) should never be > numColLevels()
		// if len(headers) < numColLevels(), excess header rows will remain blank
		headers := splitNameIntoLevels(containers[k].name)
		for l := range headers {
			headerSlots[l] = headers[l]
		}
		ret[k] = append(headerSlots, containers[k].interfaceSlice()...)
	}
	if !byColumn {
		ret = transposeInterfaceRecords(ret)
	}
	return ret
}
