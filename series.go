package tada

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

// -- CONSTRUCTORS

// NewSeries constructs a Series from a slice of values and optional label slices.
// Supported underlying slice types: real numbers, string, time.Time, boolean, or interface.
func NewSeries(slice interface{}, labels ...interface{}) *Series {
	// handle values
	if !isSlice(slice) {
		return &Series{err: fmt.Errorf("NewSeries(): unsupported kind (%v); must be slice", reflect.TypeOf(slice))}
	}
	isNull := setNullsFromInterface(slice)
	if isNull == nil {
		return &Series{err: fmt.Errorf(
			"NewSeries(): unable to calculate null values ([]%v not supported)", reflect.TypeOf(slice).Elem())}
	}
	values := &valueContainer{slice: slice, isNull: isNull}

	// handle labels
	retLabels := make([]*valueContainer, len(labels))
	if len(retLabels) == 0 {
		// default labels
		defaultLabels, isNull := makeDefaultLabels(0, reflect.ValueOf(slice).Len())
		retLabels = append(retLabels, &valueContainer{slice: defaultLabels, isNull: isNull, name: "*0"})
	} else {
		for i := range retLabels {
			input := labels[i]
			if !isSlice(input) {
				return seriesWithError(fmt.Errorf("NewSeries(): unsupported label kind (%v) at level %d; must be slice", reflect.TypeOf(input), i))
			}
			isNull := setNullsFromInterface(input)
			if isNull == nil {
				return seriesWithError(fmt.Errorf(
					"NewSeries(): unable to calculate null values at level %d ([]%v not supported)", i, reflect.TypeOf(input).Elem()))
			}
			// handle special case of []Element: convert to []interface{}
			elements, ok := input.([]Element)
			if ok {
				ret := make([]interface{}, len(isNull))
				for i := range ret {
					ret[i] = elements[i].val
				}
				input = ret
			}
			retLabels[i] = &valueContainer{slice: input, isNull: isNull, name: fmt.Sprintf("*%d", i)}
		}
	}

	return &Series{values: values, labels: retLabels}
}

// Copy returns a deep copy of a Series with no shared references to the original.
func (s *Series) Copy() *Series {
	labels := make([]*valueContainer, len(s.labels))
	for i := 0; i < len(s.labels); i++ {
		labels[i] = s.labels[i].copy()
	}
	return &Series{
		values: s.values.copy(),
		labels: labels,
		err:    s.err,
	}
}

// ToDataFrame converts a Series to a 1-column DataFrame.
func (s *Series) ToDataFrame() *DataFrame {
	s = s.Copy()
	return &DataFrame{
		values: []*valueContainer{s.values},
		labels: s.labels,
		err:    s.err,
	}
}

// -- GETTERS

// Err returns the most recent error attached to the Series, if any.
func (s *Series) Err() error {
	return s.err
}

// Len returns the number of rows in the Series.
func (s *Series) Len() int {
	return reflect.ValueOf(s.values.slice).Len()
}

// Levels returns the number of columns of labels in the Series.
func (s *Series) Levels() int {
	return len(s.labels)
}

// Elements returns the underlying value and isNull for each row.
// If any `level` is provided, returns the Elements of the columns of labels at that level.
func (s *Series) Elements(level ...int) []Element {
	ret := make([]Element, s.Len())
	if len(level) == 0 {
		v := reflect.ValueOf(s.values.slice)
		for i := 0; i < s.Len(); i++ {
			ret[i] = Element{
				val:    v.Index(i).Interface(),
				isNull: s.values.isNull[i],
			}
		}
		// handle optional level
	} else if len(level) > 0 {
		lvl := level[0]
		if lvl >= len(s.labels) {
			return []Element{}
		}
		v := reflect.ValueOf(s.labels[lvl].slice)
		for i := range ret {
			ret[i] = Element{
				val:    v.Index(i).Interface(),
				isNull: s.labels[lvl].isNull[i],
			}
		}
	}
	return ret
}

// Subset returns only the rows specified at the index positions, in the order specified. Returns a new Series.
func (s *Series) Subset(index []int) *Series {
	s = s.Copy()
	s.InPlace().Subset(index)
	return s
}

// Subset returns only the rows specified at the index positions, in the order specified.
// Modifies the underlying Series in place.
func (s *SeriesMutator) Subset(index []int) {
	if reflect.DeepEqual(index, []int{-999}) {
		s.series.resetWithError(errors.New(
			"Subset(): invalid filter (every filter must have at least one filter function; if ColName is supplied, it must be valid)"))
	}
	err := s.series.values.subsetRows(index)
	if err != nil {
		s.series.resetWithError(fmt.Errorf("Subset(): %v", err))
		return
	}
	for j := 0; j < len(s.series.labels); j++ {
		s.series.labels[j].subsetRows(index)
	}
	return
}

// SubsetLabels returns only the labels specified at the index positions, in the order specified.
// Returns a new Series.
func (s *Series) SubsetLabels(index []int) *Series {
	s = s.Copy()
	s.InPlace().SubsetLabels(index)
	return s
}

// SubsetLabels returns only the labels specified at the index positions, in the order specified.
// Modifies the underlying Series in place.
func (s *SeriesMutator) SubsetLabels(index []int) {
	l := len(s.series.labels)
	retLabels := make([]*valueContainer, len(index))
	for indexPosition, indexValue := range index {
		if indexValue >= l {
			s.series.resetWithError(fmt.Errorf("SubsetLabels(): index out of range (%d > %d)", indexValue, l-1))
			return
		}
		retLabels[indexPosition] = s.series.labels[indexValue]
	}
	s.series.labels = retLabels
	return
}

// Head returns the first n rows of the Series. If n is greater than the length of the Series, returns the entire Series.
// In either case, returns a new Series.
func (s *Series) Head(n int) *Series {
	if s.Len() < n {
		n = s.Len()
	}
	index := makeIntRange(0, n)
	return s.Subset(index)
}

// Tail returns the last n rows of the Series. If n is greater than the length of the Series, returns the entire Series.
// In either case, returns a new Series.
func (s *Series) Tail(rows int) *Series {
	if s.Len() < rows {
		rows = s.Len()
	}
	index := makeIntRange(s.Len()-rows, s.Len())
	return s.Subset(index)
}

// Valid returns all the rows with non-null values.
// Returns a new Series.
func (s *Series) Valid() *Series {
	index := s.values.valid()
	return s.Subset(index)
}

// Null returns all the rows with null values.
// Returns a new Series.
func (s *Series) Null() *Series {
	index := s.values.null()
	return s.Subset(index)
}

// Index stub
func (s *Series) Index(label string) ([]int, error) {
	ret, err := findLabelPositions(label, s.labels)
	if err != nil {
		return nil, fmt.Errorf("Index(): %v", err)
	}
	return ret, nil
}

// IndexRange stub
func (s *Series) IndexRange(firstLabel, lastLabel string) ([]int, error) {
	index1, err := findLabelPositions(firstLabel, s.labels)
	if err != nil {
		return nil, fmt.Errorf("IndexRange(): %v", err)
	}
	index2, err := findLabelPositions(lastLabel, s.labels)
	if err != nil {
		return nil, fmt.Errorf("IndexRange(): %v", err)
	}
	index := makeIntRange(minIntSlice(index1), maxIntSlice(index2)+1)
	return index, nil
}

// -- SETTERS

// InPlace returns a SeriesMutator, which contains most of the same methods as Series but never returns a new Series.
// If you want to save memory and improve performance and do not need to preserve the original Series, consider using InPlace().
func (s *Series) InPlace() *SeriesMutator {
	return &SeriesMutator{series: s}
}

// WithLabels resolves as follows:
//
// If a scalar string is supplied as `input` and a column of labels exists that matches `name`: rename the level to match `input`
//
// If a slice is supplied as `input` and a column of labels exists that matches `name`: replace the values at this level to match `input`
//
// If a slice is supplied as `input` and a column of labels does not exist that matches `name`: append a new level with a name matching `name` and values matching `input`
//
// Error conditions: supplying slice of unsupported type, supplying slice with a different length than the underlying Series, or supplying scalar string and `name` that does not match an existing label level.
// In all cases, returns a new Series.
func (s *Series) WithLabels(name string, input interface{}) *Series {
	s.Copy()
	s.InPlace().WithLabels(name, input)
	return s
}

// WithLabels resolves as follows:
//
// If a scalar string is supplied as `input` and a column of labels exists that matches `name`: rename the level to match `input`
//
// If a slice is supplied as `input` and a column of labels exists that matches `name`: replace the values at this level to match `input`
//
// If a slice is supplied as `input` and a column of labels does not exist that matches `name`: append a new level with a name matching `name` and values matching `input`
//
// Error conditions: supplying slice of unsupported type, supplying slice with a different length than the underlying Series, or supplying scalar string and `name` that does not match an existing label level.
// In all cases, modifies the underlying Series in place.
func (s *SeriesMutator) WithLabels(name string, input interface{}) {
	switch reflect.TypeOf(input).Kind() {

	// `input` is string: rename label level
	case reflect.String:
		lvl, err := findLevelWithName(name, s.series.labels)
		if err != nil {
			s.series.resetWithError(fmt.Errorf("WithLabels(): cannot rename label level: %v", err))
			return
		}
		s.series.labels[lvl].name = input.(string)
		return
	case reflect.Slice:
		isNull := setNullsFromInterface(input)
		if isNull == nil {
			s.series.resetWithError(fmt.Errorf("WithLabels(): unable to calculate null values ([]%v not supported)", reflect.TypeOf(input).Elem()))
			return
		}
		if l := reflect.ValueOf(input).Len(); l != s.series.Len() {
			s.series.resetWithError(fmt.Errorf(
				"WithLabels(): cannot replace labels in level %s: length of input does not match length of Series (%d != %d)", name, l, s.series.Len()))
			return
		}
		// `input` is supported slice
		lvl, err := findLevelWithName(name, s.series.labels)
		if err != nil {
			// `name` does not already exist: append new label level
			s.series.labels = append(s.series.labels, &valueContainer{slice: input, name: name, isNull: isNull})
			return
		}
		// `name` already exists: overwrite existing label level
		s.series.labels[lvl].slice = input
		s.series.labels[lvl].isNull = isNull
	default:
		s.series.resetWithError(fmt.Errorf("WithLabels(): unsupported input kind: must be either slice or string"))
	}
}

// WithRow stub
func (s *Series) WithRow(label string, values interface{}) *Series {
	return nil
}

// Drop removes the row at the specified index.
// Returns a new Series.
func (s *Series) Drop(index int) *Series {
	s.Copy()
	s.InPlace().Drop(index)
	return s
}

// Drop removes the row at the specified index.
// Modifies the underlying Series in place.
func (s *SeriesMutator) Drop(index int) {
	err := s.series.values.dropRow(index)
	if err != nil {
		s.series.resetWithError(fmt.Errorf("Drop(): %v", err))
		return
	}
	for j := 0; j < len(s.series.labels); j++ {
		s.series.labels[j].dropRow(index)
	}
}

// DropNull removes the null values in the Series.
// Returns a new Series.
func (s *Series) DropNull() *Series {
	s.Copy()
	s.InPlace().DropNull()
	return s
}

// DropNull removes the null values in the Series.
// Modifies the underlying Series in place.
func (s *SeriesMutator) DropNull() {
	index := s.series.values.valid()
	s.Subset(index)
	return
}

// Name modifies the name of a Series in place and returns the original Series.
func (s *Series) Name(name string) *Series {
	s.values.name = name
	return s
}

// -- SORT

// Sort removes the null values in the Series.
// Returns a new Series.
func (s *Series) Sort(by ...Sorter) *Series {
	s.Copy()
	s.InPlace().Sort(by...)
	return s
}

// Sort stub
func (s *SeriesMutator) Sort(by ...Sorter) {
	// original index
	index := makeIntRange(0, s.series.Len())
	var vals *valueContainer
	// default for handling no Sorters: values as float in ascending order
	if len(by) == 0 {
		// must copy the values to sort to avoid prematurely overwriting underlying data
		vals = s.series.values.copy()
		index = vals.sort(Float, false, index)
		s.Subset(index)
		return
	}
	for i := len(by) - 1; i >= 0; i-- {
		// Sorter with empty ColName -> use Series values
		if by[i].ColName == "" {
			vals = s.series.values.copy()
		} else {
			lvl, err := findLevelWithName(by[i].ColName, s.series.labels)
			if err != nil {
				s.series.resetWithError(fmt.Errorf(
					"Sort(): cannot use label level: %v", err))
				return
			}
			vals = s.series.labels[lvl].copy()
		}
		// overwrite index with new index
		index = vals.sort(by[i].DType, by[i].Descending, index)
	}
	s.Subset(index)
	return
}

// -- FILTERS

// Filter applies one or more filters to a Series and returns the intersection of the row positions that satisfy all filters.
func (s *Series) Filter(filters ...FilterFn) []int {
	if len(filters) == 0 {
		return makeIntRange(0, s.Len())
	}
	var subIndexes [][]int
	for _, filter := range filters {
		var data *valueContainer
		if filter.ColName == "" {
			data = s.values
		} else if lvl, err := findLevelWithName(filter.ColName, s.labels); err != nil {
			return []int{-999}
		} else {
			data = s.labels[lvl]
		}
		subIndex, err := data.filter(filter)
		if err != nil {
			return []int{-999}
		}
		subIndexes = append(subIndexes, subIndex)
	}
	index := intersection(subIndexes)
	return index
}

// GT coerces Series values to float64 and returns each row position with a non-null value greater than `comparison`.
func (s *Series) GT(comparison float64) []int {
	return s.values.gt(comparison)
}

// GTE coerces Series values to float64 and returns each row position with a non-null value greater than or equal to `comparison`.
func (s *Series) GTE(comparison float64) []int {
	return s.values.gte(comparison)
}

// LT coerces Series values to float64 and returns each row position with a non-null value less than `comparison`.
func (s *Series) LT(comparison float64) []int {
	return s.values.lt(comparison)
}

// LTE coerces Series values to float64 and returns each row position with a non-null value less than or equal to `comparison`.
func (s *Series) LTE(comparison float64) []int {
	return s.values.lte(comparison)
}

// FloatEQ coerces Series values to float64 and returns each row position with a non-null value equal to `comparison`.
func (s *Series) FloatEQ(comparison float64) []int {
	return s.values.floateq(comparison)
}

// FloatNEQ coerces Series values to float64 and returns each row position with a non-null value not equal to `comparison`.
func (s *Series) FloatNEQ(comparison float64) []int {
	return s.values.floatneq(comparison)
}

// EQ coerces Series values to string and returns each row position with a non-null value equal to `comparison`.
func (s *Series) EQ(comparison string) []int {
	return s.values.eq(comparison)
}

// NEQ coerces Series values to string and returns each row position with a non-null value not equal to `comparison`.
func (s *Series) NEQ(comparison string) []int {
	return s.values.neq(comparison)
}

// Contains coerces Series values to string and returns each row position with a non-null value containing `comparison`.
func (s *Series) Contains(substr string) []int {
	return s.values.contains(substr)
}

// Before coerces Series values to time.Time and returns each row position with a non-null value before `comparison`.
func (s *Series) Before(comparison time.Time) []int {
	return s.values.before(comparison)
}

// After coerces Series values to time.Time and returns each row position with a non-null value after `comparison`.
func (s *Series) After(comparison time.Time) []int {
	return s.values.after(comparison)
}

// -- APPLY

// Apply stub
func (s *Series) Apply(function ApplyFn) *Series {
	s.Copy()
	s.InPlace().Apply(function)
	return s
}

// Apply applies a user-defined function to every row in the Series and coerces all values to match the function type.
// Apply may be applied to a level of labels (if no column is specified, the main Series values are used).
// Modifies the underlying Series in place.
func (s *SeriesMutator) Apply(function ApplyFn) {
	var data *valueContainer
	var err error
	if function.ColName == "" {
		data = s.series.values
	} else if lvl, err := findLevelWithName(function.ColName, s.series.labels); err == nil {
		data = s.series.labels[lvl]
	} else {
		if function.ColName == s.series.values.name {
			data = s.series.values
		} else {
			s.series.resetWithError(fmt.Errorf("Apply(): %v", err))
			return
		}
	}
	data.slice, err = data.apply(function)
	if err != nil {
		s.series.resetWithError(fmt.Errorf("Apply(): %v", err))
	}
	return
}

// -- MERGERS

// Lookup stub
func (s *Series) Lookup(other *Series, how string, leftOn []string, rightOn []string) *Series {
	var leftKeys, rightKeys []int
	var err error
	if len(leftOn) == 0 || len(rightOn) == 0 {
		if !(len(leftOn) == 0 && len(rightOn) == 0) {
			return seriesWithError(fmt.Errorf("Lookup(): if either leftOn or rightOn is empty, both must be empty"))
		}
	}
	if len(leftOn) == 0 {
		leftKeys, rightKeys = findMatchingKeysBetweenTwoLabels(s.labels, other.labels)
	} else {
		leftKeys, err = labelNamesToIndex(leftOn, s.labels)
		if err != nil {
			return seriesWithError(fmt.Errorf("Lookup(): %v", err))
		}
		rightKeys, err = labelNamesToIndex(rightOn, other.labels)
		if err != nil {
			return seriesWithError(fmt.Errorf("Lookup(): %v", err))
		}
	}
	ret, err := lookup(how, s.values, s.labels, leftKeys, other.values, other.labels, rightKeys)
	if err != nil {
		return seriesWithError(fmt.Errorf("Lookup(): %v", err))
	}
	return ret
}

// Add stub
func (s *Series) Add(other *Series, ignoreMissing bool) *Series {
	fn := func(v1 float64, v2 float64) float64 {
		return v1 + v2
	}
	return s.combineMath(other, ignoreMissing, fn)
}

// Subtract stub
func (s *Series) Subtract(other *Series, ignoreMissing bool) *Series {
	fn := func(v1 float64, v2 float64) float64 {
		return v1 - v2
	}
	return s.combineMath(other, ignoreMissing, fn)
}

// Multiply stub
func (s *Series) Multiply(other *Series, ignoreMissing bool) *Series {
	fn := func(v1 float64, v2 float64) float64 {
		return v1 * v2
	}
	return s.combineMath(other, ignoreMissing, fn)
}

// Divide stub
func (s *Series) Divide(other *Series, ignoreMissing bool) *Series {
	fn := func(v1 float64, v2 float64) float64 {
		defer func() {
			recover()
		}()
		return v1 / v2
	}
	return s.combineMath(other, ignoreMissing, fn)
}

// -- GROUPERS

// GroupBy stub
func (s *Series) GroupBy(names ...string) *GroupedSeries {
	index, err := labelNamesToIndex(names, s.labels)
	if err != nil {
		return &GroupedSeries{err: fmt.Errorf("GroupBy(): %v", err)}
	}
	g, _, orderedKeys := labelsToMap(s.labels, index)
	return &GroupedSeries{
		groups:      g,
		orderedKeys: orderedKeys,
		series:      s,
	}
}

// -- ITERATORS

// IterRows returns a slice of maps that return the underlying data for every row in the Series.
// The key in each map is a column header, including label level names.
// The name of the main Series values is the same as the name of the Series itself.
// The value in each map is an Element containing an interface value and whether or not the value is null.
// If multiple columns have the same header, only the values of the right-most column are returned.
func (s *Series) IterRows() []map[string]Element {
	ret := make([]map[string]Element, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = make(map[string]Element, s.Levels()+1)
		for j := range s.labels {
			ret[i][s.labels[j].name] = s.labels[j].iterRow(i)
		}
		ret[i][s.values.name] = s.values.iterRow(i)
	}
	return ret
}

// -- MATH

// Sum stub
func (s *Series) Sum() float64 {
	sum, _ := sum(
		s.values.float().slice,
		s.values.isNull,
		makeIntRange(0, s.Len()))
	return sum
}

// Mean stub
func (s *Series) Mean() float64 {
	mean, _ := mean(
		s.values.float().slice,
		s.values.isNull,
		makeIntRange(0, s.Len()))
	return mean
}

// Median stub
func (s *Series) Median() float64 {
	median, _ := median(
		s.values.float().slice,
		s.values.isNull,
		makeIntRange(0, s.Len()))
	return median
}

// Std stub
func (s *Series) Std() float64 {
	std, _ := std(
		s.values.float().slice,
		s.values.isNull,
		makeIntRange(0, s.Len()))
	return std
}

// -- Slicers

// SliceFloat64 coerces the Series values into []float64.
// If `level` is provided, the column of labels at that level is coerced instead.
// If multiple levels are provides, only the first is used. If the level is out of range, a nil value is returned.
func (s *Series) SliceFloat64(level ...int) []float64 {
	if len(level) == 0 {
		return s.values.float().slice
	}
	lvl := level[0]
	if lvl >= s.Levels() {
		return nil
	}
	return s.labels[lvl].float().slice
}

// SliceString coerces the Series values into []string.
// If `level` is provided, the column of labels at that level is coerced instead.
// If multiple levels are provides, only the first is used. If the level is out of range, a nil value is returned.
func (s *Series) SliceString(level ...int) []string {
	if len(level) == 0 {
		return s.values.str().slice
	}
	lvl := level[0]
	if lvl >= s.Levels() {
		return nil
	}
	return s.labels[lvl].str().slice
}

// SliceTime coerces the Series values into []time.Time.
// If `level` is provided, the column of labels at that level is coerced instead.
// If multiple levels are provides, only the first is used. If the level is out of range, a nil value is returned.
func (s *Series) SliceTime(level ...int) []time.Time {
	if len(level) == 0 {
		return s.values.dateTime().slice
	}
	lvl := level[0]
	if lvl >= s.Levels() {
		return nil
	}
	return s.labels[lvl].dateTime().slice
}

// SliceNulls returns whether each value is null or not.
// If `level` is provided, the column of labels at that level is coerced instead.
// If multiple levels are provides, only the first is used. If the level is out of range, a nil value is returned.
func (s *Series) SliceNulls(level ...int) []bool {
	if len(level) == 0 {
		return s.values.isNull
	}
	lvl := level[0]
	if lvl >= s.Levels() {
		return nil
	}
	return s.labels[lvl].isNull
}
