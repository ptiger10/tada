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

func seriesWithError(err error) *Series {
	return &Series{
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

// search for a name only once. if it is found multiple times, return only the first
func findMatchingKeysBetweenTwoLabels(labels1 []*valueContainer, labels2 []*valueContainer) ([]int, []int) {
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

// findLabelPositions returns a slice of row positions where `label` (spanning one or more levels) is contained within `labels`
func findLabelPositions(label string, labels []*valueContainer) ([]int, error) {
	toFind := strings.Split(label, "|")
	for i := range toFind {
		toFind[i] = strings.TrimSpace(toFind[i])
	}
	if len(toFind) != len(labels) {
		return nil, fmt.Errorf("label (%v) must reference same number of levels as labels (%d != %d)", label, len(toFind), len(labels))
	}
	l := reflect.ValueOf(labels[0].slice).Len()
	ret := make([]int, 0)
	for i := 0; i < l; i++ {
		match := true
		for j := range labels {
			v := reflect.ValueOf(labels[j].slice)
			if v.Index(i).String() != toFind[j] {
				match = false
				break
			}
		}
		if match {
			ret = append(ret, i)
		}
	}
	if len(ret) == 0 {
		return nil, fmt.Errorf("label (%v) does not exist", label)
	}
	return ret, nil
}

// findLevelWithName returns the position of the first level within `labels` with a name matching `name`, or an error if no level matches
func findLevelWithName(name string, labels []*valueContainer) (int, error) {
	for j := range labels {
		if labels[j].name == name {
			return j, nil
		}
	}
	return 0, fmt.Errorf("name (%v) does not match any existing level", name)
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

// subsetRows returns the rows specified by index. If any position is out of range, returns an error
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

func (vc *valueContainer) apply(apply ApplyFn) (interface{}, error) {
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
	} else {
		return nil, fmt.Errorf("no apply function provided")
	}
	return ret, nil
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

// labelNamesToIndex converts a slice of label names to index positions. If any name is not in index, returns an error
func labelNamesToIndex(names []string, levels []*valueContainer) ([]int, error) {
	ret := make([]int, len(names))
	for i, name := range names {
		lvl, err := findLevelWithName(name, levels)
		if err != nil {
			return nil, err
		}
		ret[i] = lvl
	}
	return ret, nil
}

// labelsToString reduces all label levels referenced in the index to a single slice of concatenated strings
func labelsToStrings(labels []*valueContainer, index []int) []string {
	sep := "|"
	labelStrings := make([][]string, len(index))
	// coerce every label level referenced in the index to a separate string slice
	for j := range index {
		labelStrings[j] = labels[j].str().slice
	}
	ret := make([]string, len(labelStrings[0]))
	// for each row, combine labels into one concatenated string
	for i := 0; i < len(labelStrings[0]); i++ {
		components := make([]string, len(labels))
		for j := range labels {
			components[j] = labelStrings[j][i]
		}
		concatenatedString := strings.Join(components, sep)
		ret[i] = concatenatedString
	}
	// return a single slice of strings
	return ret
}

// labelsToMap reduces all label levels referenced in the index to two maps:
// the first where the key is a single concatenated string of the labels
// and the value is an integer slice of all the positions where the key appears in the labels, preserving the order in which they appear,
// and the second where the value is the integer of the first position where the key appears in the labels.
func labelsToMap(labels []*valueContainer, index []int) (map[string][]int, map[string]int) {
	sep := "|"
	// coerce all label levels referenced in the index to string
	labelStrings := make([][]string, len(index))
	for j := range index {
		labelStrings[j] = labels[index[j]].str().slice
	}
	ret := make(map[string][]int, len(labelStrings[0]))
	retFirst := make(map[string]int, len(labelStrings[0]))
	// for each row, combine labels into a single string
	l := len(labelStrings[0])
	for i := 0; i < l; i++ {
		components := make([]string, len(labelStrings))
		for j := range labelStrings {
			components[j] = labelStrings[j][i]
		}
		key := strings.Join(components, sep)
		if _, ok := ret[key]; !ok {
			ret[key] = []int{i}
			retFirst[key] = i
		} else {
			ret[key] = append(ret[key], i)
		}
	}
	return ret, retFirst
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

func lookup(how string,
	values1 *valueContainer, labels1 []*valueContainer, leftOn []int,
	values2 *valueContainer, labels2 []*valueContainer, rightOn []int) (*Series, error) {
	switch how {
	case "left":
		return lookupWithAnchor(values1, labels1, leftOn, values2, labels2, rightOn), nil
	case "right":
		return lookupWithAnchor(values2, labels2, rightOn, values1, labels1, leftOn), nil
	case "inner":
		s := lookupWithAnchor(values1, labels1, leftOn, values2, labels2, rightOn)
		s.InPlace().DropNull()
		return s, nil
	default:
		return nil, fmt.Errorf("unsupported how: must be `left`, `right`, or `inner`")
	}
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

func lookupWithAnchor(
	values1 *valueContainer, labels1 []*valueContainer, leftOn []int,
	values2 *valueContainer, labels2 []*valueContainer, rightOn []int) *Series {
	toLookup := labelsToStrings(labels1, leftOn)
	_, lookupSource := labelsToMap(labels2, rightOn)
	matches := matchLabelPositions(toLookup, lookupSource)
	v := reflect.ValueOf(values2.slice)
	isNull := make([]bool, len(matches))
	// return type is set to same type as within lookupSource
	vals := reflect.MakeSlice(v.Type(), len(matches), len(matches))
	for i, matchedIndex := range matches {
		// positive match
		if matchedIndex != -1 {
			vals.Index(i).Set(v.Index(matchedIndex))
			isNull[i] = values2.isNull[i]
			// no match
		} else {
			vals.Index(i).Set(reflect.Zero(reflect.TypeOf(values2.slice).Elem()))
			isNull[i] = true
		}
	}
	return &Series{
		values: &valueContainer{slice: vals.Interface(), isNull: isNull, name: values1.name},
		labels: labels1,
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
