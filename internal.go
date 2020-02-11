package tada

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
)

func (s *Series) resetWithError(err error) {
	s.values = nil
	s.labels = nil
	s.err = err
}

func (df *DataFrame) resetWithError(err error) {
	df.values = nil
	df.labels = nil
	df.name = ""
	df.err = err
}

func seriesWithError(err error) *Series {
	return &Series{
		err: err,
	}
}

func dataFrameWithError(err error) *DataFrame {
	return &DataFrame{
		err: err,
	}
}

func isSlice(input interface{}) bool {
	return reflect.TypeOf(input).Kind() == reflect.Slice
}

// makeDefaultLabels returns a sequential series of numbers (inclusive of min, exclusive of max) and a companion isNull slice.
func makeDefaultLabels(min, max int) (labels []int, isNull []bool) {
	labels = make([]int, max-min)
	isNull = make([]bool, len(labels))
	for i := range labels {
		labels[i] = min + i
		isNull[i] = false
	}
	return
}

// makeIntRange returns a sequential series of numbers (inclusive of min, exclusive of max)
func makeIntRange(min, max int) []int {
	ret := make([]int, max-min)
	for i := range ret {
		ret[i] = min + i
	}
	return ret
}

// convert []Element to []interface of Element values only
func handleElementsSlice(input interface{}) []interface{} {
	elements, ok := input.([]Element)
	if ok {
		ret := make([]interface{}, len(elements))
		for i := range ret {
			ret[i] = elements[i].val
		}
		return ret
	}
	return nil
}

// search for a name only once. if it is found multiple times, return only the first
func findMatchingKeysBetweenTwoLabelContainers(labels1 []*valueContainer, labels2 []*valueContainer) ([]int, []int) {
	var leftKeys, rightKeys []int
	searched := make(map[string]bool)
	for j := range labels1 {
		key := labels1[j].name
		if _, ok := searched[key]; ok {
			continue
		} else {
			searched[key] = true
		}
		for k := range labels2 {
			if key == labels2[k].name {
				leftKeys = append(leftKeys, j)
				rightKeys = append(rightKeys, k)
				break
			}
		}
	}
	return leftKeys, rightKeys
}

// if name is not found in either columns nor labels, return error
func findNameInColumnsOrLabels(name string, cols []*valueContainer, labels []*valueContainer) (index int, isCol bool, err error) {
	// first check column names
	if lvl, err := findColWithName(name, cols); err == nil {
		return lvl, true, nil
		// then check label level names
	} else if lvl, err := findColWithName(name, labels); err == nil {
		return lvl, false, nil
	} else {
		return 0, false, fmt.Errorf("no matching name (%s) in either columns or label levels", name)
	}
}

// findColWithName returns the position of the first level within `cols` with a name matching `name`, or an error if no level matches
func findColWithName(name string, cols []*valueContainer) (int, error) {
	for j := range cols {
		if cols[j].name == name {
			return j, nil
		}
	}
	return 0, fmt.Errorf("name (%v) does not match any existing column", name)
}

func withColumn(cols []*valueContainer, name string, input interface{}, requiredLen int) ([]*valueContainer, error) {
	switch reflect.TypeOf(input).Kind() {
	// `input` is string: rename label level
	case reflect.String:
		lvl, err := findColWithName(name, cols)
		if err != nil {
			return nil, fmt.Errorf("cannot rename column: %v", err)
		}
		cols[lvl].name = input.(string)
	case reflect.Slice:
		isNull := setNullsFromInterface(input)
		if isNull == nil {
			return nil, fmt.Errorf("unable to calculate null values ([]%v not supported)", reflect.TypeOf(input).Elem())
		}
		if l := reflect.ValueOf(input).Len(); l != requiredLen {
			return nil, fmt.Errorf(
				"cannot replace items in column %s: length of input does not match existing length (%d != %d)",
				name, l, requiredLen)
		}
		// `input` is supported slice
		lvl, err := findColWithName(name, cols)
		if err != nil {
			// `name` does not already exist: append new label level
			cols = append(cols, &valueContainer{slice: input, name: name, isNull: isNull})
		} else {
			// `name` already exists: overwrite existing label level
			cols[lvl].slice = input
			cols[lvl].isNull = isNull
		}
	case reflect.Ptr:
		v, ok := input.(*Series)
		if !ok {
			return nil, fmt.Errorf("unsupported input: *Series is only supported pointer")
		}

		if v.Len() != requiredLen {
			return nil, fmt.Errorf(
				"cannot replace items in column %s: length of input Series does not match existing length (%d != %d)",
				name, v.Len(), requiredLen)
		}
		// `name` does not already exist: append new level
		lvl, err := findColWithName(name, cols)
		if err != nil {
			cols = append(cols, v.values)
			cols[len(cols)-1].name = name
		} else {
			// `name` already exists: overwrite existing level
			cols[lvl] = v.values
			cols[lvl].name = name
		}

	default:
		return nil, fmt.Errorf("unsupported input kind: must be either slice, string, or Series")
	}
	return cols, nil
}

func intersection(slices [][]int) []int {
	set := make(map[int]int)
	for _, slice := range slices {
		for i := range slice {
			if _, ok := set[slice[i]]; !ok {
				set[slice[i]] = 1
			} else {
				set[slice[i]]++
			}
		}
	}
	var ret []int
	for k, v := range set {
		if v == len(slices) {
			ret = append(ret, k)
		}
	}
	return ret
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

func minIntSlice(slice []int) int {
	var min int
	for _, val := range slice {
		if val < min {
			min = val
		}
	}
	return min
}

func maxIntSlice(slice []int) int {
	var max int
	for _, val := range slice {
		if val > max {
			max = val
		}
	}
	return max
}

func (vc *valueContainer) valid() []int {
	index := make([]int, 0)
	for i, isNull := range vc.isNull {
		if !isNull {
			index = append(index, i)
		}
	}
	return index
}

func (vc *valueContainer) null() []int {
	index := make([]int, 0)
	for i, isNull := range vc.isNull {
		if isNull {
			index = append(index, i)
		}
	}
	return index
}

// subsetRows modifies vc in place to contain ony the rows specified by index.
// If any position is out of range, returns an error
func (vc *valueContainer) subsetRows(index []int) error {
	v := reflect.ValueOf(vc.slice)
	l := v.Len()
	retIsNull := make([]bool, len(index))
	retVals := reflect.MakeSlice(v.Type(), len(index), len(index))
	// []int{1, 5}
	// indexPosition: [0, 1], indexValue: [1,5]
	for indexPosition, indexValue := range index {
		if indexValue >= l {
			return fmt.Errorf("index out of range (%d > %d)", indexValue, l-1)
		}
		retIsNull[indexPosition] = vc.isNull[indexValue]
		ptr := retVals.Index(indexPosition)
		ptr.Set(v.Index(indexValue))
	}

	vc.slice = retVals.Interface()
	vc.isNull = retIsNull
	return nil
}

// subsetCols returns a new set of valueContainers containing only the columns specified by index.
// If any position is out of range, returns error
func subsetCols(cols []*valueContainer, index []int) ([]*valueContainer, error) {
	retLabels := make([]*valueContainer, len(index))
	for indexPosition, indexValue := range index {
		if indexValue >= len(cols) {
			return nil, fmt.Errorf("index out of range (%d > %d)", indexValue, len(cols)-1)
		}
		retLabels[indexPosition] = cols[indexValue]
	}
	return retLabels, nil
}

// head returns the first number of rows specified by `n`
func (vc *valueContainer) head(n int) *valueContainer {
	v := reflect.ValueOf(vc.slice)
	var retIsNull []bool
	retVals := v.Slice(0, n)
	retIsNull = vc.isNull[:n]

	return &valueContainer{
		slice:  retVals.Interface(),
		isNull: retIsNull,
		name:   vc.name,
	}
}

// tail returns the last number of rows specified by `n`
func (vc *valueContainer) tail(n int) *valueContainer {
	v := reflect.ValueOf(vc.slice)
	var retIsNull []bool
	retVals := v.Slice(len(vc.isNull)-n, len(vc.isNull))
	retIsNull = vc.isNull[len(vc.isNull)-n : len(vc.isNull)]

	return &valueContainer{
		slice:  retVals.Interface(),
		isNull: retIsNull,
		name:   vc.name,
	}
}

// rangeSlice returns the rows starting with first and ending with last (inclusive)
func (vc *valueContainer) rangeSlice(first, last int) *valueContainer {
	v := reflect.ValueOf(vc.slice)
	var retIsNull []bool
	retVals := v.Slice(first, last+1)
	retIsNull = vc.isNull[first : last+1]

	return &valueContainer{
		slice:  retVals.Interface(),
		isNull: retIsNull,
		name:   vc.name,
	}
}

func (vc *valueContainer) shift(n int) *valueContainer {
	v := reflect.ValueOf(vc.slice)
	vals := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
	isNull := make([]bool, v.Len())
	for i := 0; i < v.Len(); i++ {
		position := i - n
		if position < 0 || position >= v.Len() {
			vals.Index(i).Set(reflect.Zero(reflect.TypeOf(vc.slice).Elem()))
			isNull[i] = true
		} else {
			vals.Index(i).Set(v.Index(position))
			isNull[i] = vc.isNull[position]
		}
	}
	return &valueContainer{
		slice:  vals.Interface(),
		isNull: isNull,
		name:   vc.name,
	}
}

// convert to string as lowest common denominator
func (vc *valueContainer) append(other *valueContainer) *valueContainer {
	retSlice := append(vc.str().slice, other.str().slice...)
	retIsNull := append(vc.isNull, other.isNull...)
	return &valueContainer{
		slice:  retSlice,
		isNull: retIsNull,
		name:   vc.name,
	}
}

func (vc *valueContainer) iterRow(index int) Element {
	return Element{
		val:    reflect.ValueOf(vc.slice).Index(index).Interface(),
		isNull: vc.isNull[index]}
}

func (vc *valueContainer) gt(comparison float64) []int {
	index, _ := vc.filter(FilterFn{F64: func(v float64, isNull bool) bool {
		if v > comparison && !isNull {
			return true
		}
		return false
	}})
	return index
}

func (vc *valueContainer) lt(comparison float64) []int {
	index, _ := vc.filter(FilterFn{F64: func(v float64, isNull bool) bool {
		if v < comparison && !isNull {
			return true
		}
		return false
	}})
	return index
}

func (vc *valueContainer) gte(comparison float64) []int {
	index, _ := vc.filter(FilterFn{F64: func(v float64, isNull bool) bool {
		if v >= comparison && !isNull {
			return true
		}
		return false
	}})
	return index
}

func (vc *valueContainer) lte(comparison float64) []int {
	index, _ := vc.filter(FilterFn{F64: func(v float64, isNull bool) bool {
		if v <= comparison && !isNull {
			return true
		}
		return false
	}})
	return index
}

func (vc *valueContainer) floateq(comparison float64) []int {
	index, _ := vc.filter(FilterFn{F64: func(v float64, isNull bool) bool {
		if v == comparison && !isNull {
			return true
		}
		return false
	}})
	return index
}

func (vc *valueContainer) floatneq(comparison float64) []int {
	index, _ := vc.filter(FilterFn{F64: func(v float64, isNull bool) bool {
		if v != comparison && !isNull {
			return true
		}
		return false
	}})
	return index
}

func (vc *valueContainer) eq(comparison string) []int {
	index, _ := vc.filter(FilterFn{String: func(v string, isNull bool) bool {
		if v == comparison && !isNull {
			return true
		}
		return false
	}})
	return index
}

func (vc *valueContainer) neq(comparison string) []int {
	index, _ := vc.filter(FilterFn{String: func(v string, isNull bool) bool {
		if v != comparison && !isNull {
			return true
		}
		return false
	}})
	return index
}

func (vc *valueContainer) contains(substr string) []int {
	index, _ := vc.filter(FilterFn{String: func(v string, isNull bool) bool {
		if strings.Contains(v, substr) && !isNull {
			return true
		}
		return false
	}})
	return index
}

func (vc *valueContainer) before(comparison time.Time) []int {
	index, _ := vc.filter(FilterFn{DateTime: func(v time.Time, isNull bool) bool {
		if v.Before(comparison) && !isNull {
			return true
		}
		return false
	}})
	return index
}

func (vc *valueContainer) after(comparison time.Time) []int {
	index, _ := vc.filter(FilterFn{DateTime: func(v time.Time, isNull bool) bool {
		if v.After(comparison) && !isNull {
			return true
		}
		return false
	}})
	return index
}

func (vc *valueContainer) filter(filter FilterFn) ([]int, error) {
	var index []int
	if filter.F64 != nil {
		slice := vc.float().slice
		isNull := vc.float().isNull
		for i := range slice {
			if filter.F64(slice[i], isNull[i]) {
				index = append(index, i)
			}
		}
	} else if filter.String != nil {
		slice := vc.str().slice
		isNull := vc.str().isNull
		for i := range slice {
			if filter.String(slice[i], isNull[i]) {
				index = append(index, i)
			}
		}
	} else if filter.DateTime != nil {
		slice := vc.dateTime().slice
		isNull := vc.dateTime().isNull
		for i := range slice {
			if filter.DateTime(slice[i], isNull[i]) {
				index = append(index, i)
			}
		}
	} else {
		return nil, fmt.Errorf("no filter function provided")
	}
	return index, nil
}

func (vc *valueContainer) applyFormat(apply ApplyFormatFn) interface{} {
	var ret interface{}
	if apply.F64 != nil {
		slice := vc.float().slice
		retSlice := make([]string, len(slice))
		for i := range slice {
			retSlice[i] = apply.F64(slice[i])
		}
		ret = retSlice
	} else if apply.DateTime != nil {
		slice := vc.dateTime().slice
		retSlice := make([]string, len(slice))
		for i := range slice {
			retSlice[i] = apply.DateTime(slice[i])
		}
		ret = retSlice
	}
	return ret
}

func (vc *valueContainer) apply(apply ApplyFn) interface{} {
	var ret interface{}
	if apply.F64 != nil {
		slice := vc.float().slice
		retSlice := make([]float64, len(slice))
		for i := range slice {
			retSlice[i] = apply.F64(slice[i])
		}
		ret = retSlice
	} else if apply.String != nil {
		slice := vc.str().slice
		retSlice := make([]string, len(slice))
		for i := range slice {
			retSlice[i] = apply.String(slice[i])
		}
		ret = retSlice
	} else if apply.DateTime != nil {
		slice := vc.dateTime().slice
		retSlice := make([]time.Time, len(slice))
		for i := range slice {
			retSlice[i] = apply.DateTime(slice[i])
		}
		ret = retSlice
	}
	return ret
}

// expects slices to be same-lengthed
func isEitherNull(isNull1, isNull2 []bool) []bool {
	ret := make([]bool, len(isNull1))
	for i := 0; i < len(isNull1); i++ {
		ret[i] = isNull1[i] || isNull2[i]
	}
	return ret
}

func (vc *valueContainer) sort(dtype DType, descending bool, index []int) []int {
	var srt sort.Interface
	switch dtype {
	case Float:
		d := vc.float()
		d.index = index
		srt = d
		if descending {
			srt = sort.Reverse(srt)
		}
		sort.Stable(srt)
		return d.index
	case String:
		d := vc.str()
		d.index = index
		srt = d
		if descending {
			srt = sort.Reverse(srt)
		}
		sort.Stable(srt)
		return d.index
	case DateTime:
		d := vc.dateTime()
		d.index = index
		srt = d
		if descending {
			srt = sort.Reverse(srt)
		}
		sort.Stable(srt)
		return d.index
	}

	return nil
}

// convertColNamesToIndexPositions converts a slice of label or column names to index positions.
// If any name is not in the set of columns, returns an error
func convertColNamesToIndexPositions(names []string, columns []*valueContainer) ([]int, error) {
	ret := make([]int, len(names))
	for i, name := range names {
		lvl, err := findColWithName(name, columns)
		if err != nil {
			return nil, err
		}
		ret[i] = lvl
	}
	return ret, nil
}

// concatenateLabelsToStringsreduces all label levels referenced in the index to a single slice of concatenated strings
func concatenateLabelsToStrings(labels []*valueContainer, index []int) []string {
	sep := "|"
	labelStrings := make([][]string, len(index))
	// coerce every label level referenced in the index to a separate string slice
	for j := range index {
		labelStrings[j] = labels[j].str().slice
	}
	ret := make([]string, len(labelStrings[0]))
	// for each row, combine labels into one concatenated string
	for i := 0; i < len(labelStrings[0]); i++ {
		labelComponents := make([]string, len(index))
		for j := range index {
			labelComponents[j] = labelStrings[j][i]
		}
		concatenatedString := strings.Join(labelComponents, sep)
		ret[i] = concatenatedString
	}
	// return a single slice of strings
	return ret
}

// labelsToMap reduces all label levels referenced in the index to two maps and a slice of strings:
// 1) map[string][]int: the key is a single concatenated string of the labels
// and the value is an integer slice of all the positions where the key appears in the labels, preserving the order in which they appear,
// 2) map[string]int: same key as above, but the value is the integer of the first position where the key appears in the labels.
// 3) []string: the map keys in the order in which they appear in the Series
func labelsToMap(labels []*valueContainer, index []int) (
	allIndex map[string][]int, firstIndex map[string]int,
	orderedKeys []string, originalIndexToUniqueIndex map[int]int) {
	sep := "|"
	// coerce all label levels referenced in the index to string
	labelStrings := make([][]string, len(index))
	for j := range index {
		labelStrings[j] = labels[index[j]].str().slice
	}
	allIndex = make(map[string][]int)
	firstIndex = make(map[string]int)
	orderedKeys = make([]string, 0)
	originalIndexToUniqueIndex = make(map[int]int, len(labelStrings[0]))
	orderedKeyIndex := make(map[string]int)
	// for each row, combine labels into a single string
	l := len(labelStrings[0])
	for i := 0; i < l; i++ {
		labelComponents := make([]string, len(labelStrings))
		for j := range labelStrings {
			labelComponents[j] = labelStrings[j][i]
		}
		key := strings.Join(labelComponents, sep)
		if _, ok := allIndex[key]; !ok {
			allIndex[key] = []int{i}
			firstIndex[key] = i
			orderedKeyIndex[key] = len(orderedKeys)
			orderedKeys = append(orderedKeys, key)
		} else {
			allIndex[key] = append(allIndex[key], i)
		}
		originalIndexToUniqueIndex[i] = orderedKeyIndex[key]
	}
	return
}

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

func (s *Series) combineMath(other *Series, ignoreMissing bool, fn func(v1 float64, v2 float64) float64) *Series {
	retFloat := make([]float64, s.Len())
	retIsNull := make([]bool, s.Len())
	lookupVals := s.Lookup(other, "left", nil, nil)
	lookupFloat := lookupVals.SliceFloat64()
	lookupNulls := lookupVals.SliceNulls()
	originalFloat := s.SliceFloat64()
	originalNulls := s.SliceNulls()
	for i := range originalFloat {
		// handle null lookup
		if lookupNulls[i] {
			if ignoreMissing {
				retFloat[i] = originalFloat[i]
				retIsNull[i] = originalNulls[i]
				continue
			}
			retFloat[i] = 0
			retIsNull[i] = true
			continue
		}
		// actual combination logic
		combinedFloat := fn(originalFloat[i], lookupFloat[i])
		// handle division by 0
		if math.IsNaN(combinedFloat) || math.IsInf(combinedFloat, 0) {
			combinedFloat = 0
			retIsNull[i] = true
			continue
		}
		retFloat[i] = combinedFloat
		retIsNull[i] = originalNulls[i]
	}
	return &Series{values: &valueContainer{slice: retFloat, isNull: retIsNull}, labels: s.labels}
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
		s = s.Valid()
		return s, nil
	default:
		return nil, fmt.Errorf("unsupported how: must be `left`, `right`, or `inner`")
	}
}

func lookupDataFrame(how string,
	name string, values1 []*valueContainer, labels1 []*valueContainer, leftOn []int,
	values2 []*valueContainer, labels2 []*valueContainer, rightOn []int,
	excludeLeft []string, excludeRight []string) (*DataFrame, error) {
	mergedLabelsCols1 := append(labels1, values1...)
	mergedLabelsCols2 := append(labels2, values2...)
	switch how {
	case "left":
		return lookupDataFrameWithAnchor(name, mergedLabelsCols1, labels1, leftOn,
			values2, mergedLabelsCols2, rightOn, excludeRight), nil
	case "right":
		return lookupDataFrameWithAnchor(name, mergedLabelsCols2, labels2, rightOn,
			values1, mergedLabelsCols1, leftOn, excludeLeft), nil
	case "inner":
		df := lookupDataFrameWithAnchor(name, mergedLabelsCols1, labels1, leftOn,
			values2, mergedLabelsCols2, rightOn, excludeRight)
		df = df.Valid()
		return df, nil
	default:
		return nil, fmt.Errorf("unsupported how: must be `left`, `right`, or `inner`")
	}
}

// cuts labels by leftOn and rightOn, anchors to labels in labels1, finds matches in labels2
// looks up values in values2, converts to Series and preserves the name from values1
func lookupWithAnchor(
	name string, labels1 []*valueContainer, leftOn []int,
	values2 *valueContainer, labels2 []*valueContainer, rightOn []int) *Series {
	toLookup := concatenateLabelsToStrings(labels1, leftOn)
	_, lookupSource, _, _ := labelsToMap(labels2, rightOn)
	matches := matchLabelPositions(toLookup, lookupSource)
	v := reflect.ValueOf(values2.slice)
	isNull := make([]bool, len(matches))
	// return type is set to same type as within lookupSource
	vals := reflect.MakeSlice(v.Type(), len(matches), len(matches))
	for i, matchedIndex := range matches {
		// positive match: copy value from values2
		if matchedIndex != -1 {
			vals.Index(i).Set(v.Index(matchedIndex))
			isNull[i] = values2.isNull[i]
			// no match: set to zero value
		} else {
			vals.Index(i).Set(reflect.Zero(reflect.TypeOf(values2.slice).Elem()))
			isNull[i] = true
		}
	}
	return &Series{
		values: &valueContainer{slice: vals.Interface(), isNull: isNull, name: name},
		labels: labels1,
	}
}

// cuts labels by leftOn and rightOn, anchors to labels in labels1, finds matches in labels2
// looks up values in every column in values2 (excluding colNames matching `except`),
// preserves the column names from values2, converts to dataframe with `name`
func lookupDataFrameWithAnchor(
	name string, mergedLabelsCols1 []*valueContainer, labels1 []*valueContainer, leftOn []int,
	values2 []*valueContainer, mergedLabelsCols2 []*valueContainer, rightOn []int, exclude []string) *DataFrame {
	toLookup := concatenateLabelsToStrings(mergedLabelsCols1, leftOn)
	_, lookupSource, _, _ := labelsToMap(mergedLabelsCols2, rightOn)
	matches := matchLabelPositions(toLookup, lookupSource)
	// slice of slices
	var retVals []*valueContainer
	for k := range values2 {
		var skip bool
		for _, exclusion := range exclude {
			// skip any column whose name is also used in the lookup
			if values2[k].name == exclusion {
				skip = true
			}
		}
		if skip {
			continue
		}
		v := reflect.ValueOf(values2[k].slice)
		isNull := make([]bool, len(matches))
		// return type is set to same type as within lookupSource
		vals := reflect.MakeSlice(v.Type(), len(matches), len(matches))
		for i, matchedIndex := range matches {
			// positive match: copy value from values2
			if matchedIndex != -1 {
				vals.Index(i).Set(v.Index(matchedIndex))
				isNull[i] = values2[k].isNull[i]
				// no match
			} else {
				vals.Index(i).Set(reflect.Zero(reflect.TypeOf(values2[k].slice).Elem()))
				isNull[i] = true
			}
		}
		retVals = append(retVals, &valueContainer{slice: vals.Interface(), isNull: isNull, name: values2[k].name})
	}
	return &DataFrame{
		values: retVals,
		labels: labels1,
		name:   name,
	}
}

func (vc *valueContainer) dropRow(index int) error {
	v := reflect.ValueOf(vc.slice)
	l := v.Len()
	if index >= l {
		return fmt.Errorf("index out of range (%d > %d)", index, l-1)
	}
	retIsNull := append(vc.isNull[:index], vc.isNull[index+1:]...)
	retVals := reflect.MakeSlice(v.Type(), 0, 0)
	retVals = reflect.AppendSlice(v.Slice(0, index), v.Slice(index+1, l))

	vc.slice = retVals.Interface()
	vc.isNull = retIsNull
	return nil
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

func (vc *valueContainer) copy() *valueContainer {
	v := reflect.ValueOf(vc.slice)
	vals := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
	for i := 0; i < v.Len(); i++ {
		ptr := vals.Index(i)
		ptr.Set(v.Index(i))
	}
	isNull := make([]bool, len(vc.isNull))
	copy(isNull, vc.isNull)
	return &valueContainer{
		slice:  vals.Interface(),
		isNull: isNull,
		name:   vc.name,
	}
}

func setNullsFromInterface(input interface{}) []bool {
	var ret []bool
	if reflect.TypeOf(input).Kind() != reflect.Slice {
		return nil
	}
	switch input.(type) {
	case []float64:
		vals := input.([]float64)
		ret = make([]bool, len(vals))
		for i := range ret {
			if math.IsNaN(vals[i]) {
				ret[i] = true
			} else {
				ret[i] = false
			}
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
		for i := range ret {
			if isNullInterface(vals[i]) {
				ret[i] = true
			} else {
				ret[i] = false
			}
		}
		// no null value possible
	case []bool, []uint, []uint8, []uint16, []uint32, []uint64, []int, []int8, []int16, []int32, []int64, []float32:
		l := reflect.ValueOf(input).Len()
		ret = make([]bool, l)
		for i := range ret {
			ret[i] = false
		}
	case []Element:
		vals := input.([]Element)
		ret = make([]bool, len(vals))
		for i := range ret {
			ret[i] = vals[i].isNull
		}
	default:
		return nil
	}
	return ret
}

func isNullInterface(i interface{}) bool {
	switch i.(type) {
	case float64:
		f := i.(float64)
		if math.IsNaN(f) {
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
	}
	return false
}

func isNullString(s string) bool {
	nullStrings := []string{"NaN", "n/a", "N/A", "", "nil"}
	for _, ns := range nullStrings {
		if strings.TrimSpace(s) == ns {
			return true
		}
	}
	return false
}

// math

// sum sums the non-null values at the index positions in `vals`. If all values are null, the final result is null.
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

// mean calculates the mean of the non-null values at the index positions in `vals`.
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

// median calculates the median of the non-null values at the index positions in `vals`.
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
		return math.NaN(), true
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

// std calculates the standard deviation of the non-null values at the index positions in `vals`.
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

// count counts the non-null values at the `index` positions in `vals`.
// Compatible with Grouped calculations as well as Series
func count(vals []float64, isNull []bool, index []int) (float64, bool) {
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
	return float64(counter), false
}

// min returns the min of the non-null values at the `index` positions in `vals`.
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

// max returns the max of the non-null values at the `index` positions in `vals`.
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

// returns the first non-null value as a string
func first(vals []string, isNull []bool, index []int) (string, bool) {
	for _, i := range index {
		if !isNull[i] {
			return vals[i], false
		}
	}
	return "", true
}

// returns the last non-null value as a string
func last(vals []string, isNull []bool, index []int) (string, bool) {
	for i := len(index) - 1; i >= 0; i-- {
		if !isNull[index[i]] {
			return vals[index[i]], false
		}
	}
	return "", true
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
