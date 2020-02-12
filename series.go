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
	if reflect.ValueOf(slice).Len() == 0 {
		return &Series{err: fmt.Errorf("NewSeries(): slice cannot be empty")}
	}
	isNull := setNullsFromInterface(slice)
	if isNull == nil {
		return &Series{err: fmt.Errorf(
			"NewSeries(): unable to calculate null values ([]%v not supported)", reflect.TypeOf(slice).Elem())}
	}
	elements := handleElementsSlice(slice)
	if elements != nil {
		slice = elements
	}
	values := &valueContainer{slice: slice, isNull: isNull}

	// handle labels
	retLabels := make([]*valueContainer, len(labels))
	if len(retLabels) == 0 {
		// default labels
		defaultLabels := makeDefaultLabels(0, reflect.ValueOf(slice).Len())
		retLabels = append(retLabels, defaultLabels)
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
			elements := handleElementsSlice(slice)
			if elements != nil {
				input = elements
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
		values:        []*valueContainer{s.values},
		labels:        s.labels,
		colLevelNames: []string{"*0"},
		err:           s.err,
	}
}

// ToCSV converts a Series to a DataFrame and returns as [][]string.
func (s *Series) ToCSV() [][]string {
	df := &DataFrame{
		values: []*valueContainer{s.values},
		labels: s.labels,
		err:    s.err,
	}
	return df.ToCSV()
}

// -- GETTERS

func (s *Series) String() string {
	return s.ToDataFrame().String()
}

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
// If any `labelLevel` is provided, returns the Elements of the columns of labels at that level.
func (s *Series) Elements(labelLevel ...int) []Element {
	ret := make([]Element, s.Len())
	if len(labelLevel) == 0 {
		v := reflect.ValueOf(s.values.slice)
		for i := 0; i < s.Len(); i++ {
			ret[i] = Element{
				val:    v.Index(i).Interface(),
				isNull: s.values.isNull[i],
			}
		}
		// handle optional labelLevel
	} else if len(labelLevel) > 0 {
		lvl := labelLevel[0]
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
	for j := range s.series.labels {
		s.series.labels[j].subsetRows(index)
	}
	return
}

// SubsetLabels includes only the columns of labels specified at the index positions, in the order specified.
// Returns a new Series.
func (s *Series) SubsetLabels(index []int) *Series {
	s = s.Copy()
	s.InPlace().SubsetLabels(index)
	return s
}

// SubsetLabels includes only the columns of labels specified at the index positions, in the order specified.
// Modifies the underlying Series in place.
func (s *SeriesMutator) SubsetLabels(index []int) {
	labels, err := subsetCols(s.series.labels, index)
	if err != nil {
		s.series.resetWithError(fmt.Errorf("SubsetLabels(): %v", err))
		return
	}
	s.series.labels = labels
	return
}

// Head returns the first `n` rows of the Series. If `n` is greater than the length of the Series, returns the entire Series.
// In either case, returns a new Series.
func (s *Series) Head(n int) *Series {
	if s.Len() < n {
		n = s.Len()
	}
	retVals := s.values.head(n)
	retLabels := make([]*valueContainer, s.Levels())
	for j := range s.labels {
		retLabels[j] = s.labels[j].head(n)
	}
	return &Series{values: retVals, labels: retLabels}
}

// Tail returns the last `n` rows of the Series. If `n` is greater than the length of the Series, returns the entire Series.
// In either case, returns a new Series.
func (s *Series) Tail(n int) *Series {
	if s.Len() < n {
		n = s.Len()
	}
	retVals := s.values.tail(n)
	retLabels := make([]*valueContainer, s.Levels())
	for j := range s.labels {
		retLabels[j] = s.labels[j].tail(n)
	}
	return &Series{values: retVals, labels: retLabels}
}

// Range returns the rows of the Series starting at `first` and `ending` with last (inclusive).
// If either `first` or `last` is greater than the length of the Series, a Series error is returned.
// In all cases, returns a new Series.
func (s *Series) Range(first, last int) *Series {
	if first >= s.Len() {
		return seriesWithError(fmt.Errorf("Range(): first index out of range (%d > %d)", first, s.Len()-1))
	} else if last >= s.Len() {
		return seriesWithError(fmt.Errorf("Range(): last index out of range (%d > %d)", last, s.Len()-1))
	}
	retVals := s.values.rangeSlice(first, last)
	retLabels := make([]*valueContainer, s.Levels())
	for j := range s.labels {
		retLabels[j] = s.labels[j].rangeSlice(first, last)
	}
	return &Series{values: retVals, labels: retLabels}
}

// DropNull returns all the rows with non-null values.
// Returns a new Series.
func (s *Series) DropNull() *Series {
	s = s.Copy()
	s.InPlace().DropNull()
	return s
}

// DropNull returns all the rows with non-null values.
// Modifies the underlying Series.
func (s *SeriesMutator) DropNull() {
	index := s.series.values.valid()
	s.Subset(index)
}

// Null returns all the rows with null values.
// Returns a new Series.
func (s *Series) Null() *Series {
	index := s.values.null()
	return s.Subset(index)
}

// Shift shifts all the values `n` rows upward while keeping labels constant.
// Returns a new Series.
func (s *Series) Shift(n int) *Series {
	s = s.Copy()
	s.InPlace().Shift(n)
	return s
}

// Shift shifts all the values `n` rows upward while keeping labels constant.
// // Modifies the underlying Series.
func (s *SeriesMutator) Shift(n int) {
	if s.series.Len() < n {
		n = s.series.Len()
	}
	s.series.values = s.series.values.shift(n)
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
	labels, err := withColumn(s.series.labels, name, input, s.series.Len())
	if err != nil {
		s.series.resetWithError(fmt.Errorf("WithLabels(): %v", err))
	}
	s.series.labels = labels
}

// WithValues stub
func (s *Series) WithValues(input interface{}) *Series {
	s.Copy()
	s.InPlace().WithValues(input)
	return s
}

// WithValues stub
func (s *SeriesMutator) WithValues(input interface{}) {
	// synthesize a collection of valueContainers, ensuring that name already exists
	vals, err := withColumn([]*valueContainer{s.series.values}, s.series.values.name, input, s.series.Len())
	if err != nil {
		s.series.resetWithError(fmt.Errorf("WithValues(): %v", err))
	}
	s.series.values = vals[0]
	return
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
	for j := range s.series.labels {
		s.series.labels[j].dropRow(index)
	}
}

// Append adds the `other` values as new rows to the Series.
// Returns a new Series.
func (s *Series) Append(other *Series) *Series {
	s.Copy()
	s.InPlace().Append(other)
	return s
}

// Append adds the `other` values as new rows to the Series by coercing all values to string.
// Returns a new Series.
func (s *SeriesMutator) Append(other *Series) {
	if len(other.labels) != len(s.series.labels) {
		s.series.resetWithError(
			fmt.Errorf("other Series must have same number of label levels as original Series (%d != %d)",
				len(other.labels), len(s.series.labels)))
	}
	for j := range s.series.labels {
		s.series.labels[j] = s.series.labels[j].append(other.labels[j])
	}
	s.series.values = s.series.values.append(other.values)
	return
}

// SetName modifies the name of a Series in place and returns the original Series.
func (s *Series) SetName(name string) *Series {
	s.values.name = name
	return s
}

// Name returns the name of the Series
func (s *Series) Name() string {
	return s.values.name
}

// -- SORT

// Sort sorts the values `by` zero or more Sorter specifications.
// If no Sorter is supplied, sorts by Series values (as float64) in ascending order.
// If a Sorter is supplied without a ColName or name matching the Series name, sorts by Series values.
// If no DType is supplied, sorts as float64.
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
		if by[i].ColName == "" || by[i].ColName == s.series.values.name {
			vals = s.series.values.copy()
		} else {
			lvl, err := findColWithName(by[i].ColName, s.series.labels)
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
// If no filter is provided, returns a list of all index positions in the Series.
// In event of error, returns []int
func (s *Series) Filter(filters ...FilterFn) []int {
	if len(filters) == 0 {
		return makeIntRange(0, s.Len())
	}
	// subIndexes contains the index positions computed across all the filters
	var subIndexes [][]int
	for _, filter := range filters {
		var data *valueContainer
		// if no column name is specified in a filter, use the series values
		if filter.ColName == "" || filter.ColName == s.values.name {
			data = s.values
		} else if lvl, err := findColWithName(filter.ColName, s.labels); err != nil {
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
	// reduce the subindexes to a single index that shares all the values
	index := intersection(subIndexes)
	return index
}

// Where stub
func (s *Series) Where(filters []FilterFn, ifTrue, ifFalse string) *Series {
	ret := make([]string, s.Len())
	index := s.Filter(filters...)
	for _, i := range index {
		ret[i] = ifTrue
	}
	inverseIndex := difference(makeIntRange(0, s.Len()), index)
	for _, i := range inverseIndex {
		ret[i] = ifFalse
	}
	return &Series{
		values: &valueContainer{
			slice:  ret,
			isNull: s.values.isNull,
			name:   s.values.name,
		},
		labels: s.labels,
	}
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
func (s *Series) Apply(lambda ApplyFn) *Series {
	s.Copy()
	s.InPlace().Apply(lambda)
	return s
}

// Apply applies a user-defined `lambda` function to every row in the Series and coerces all values to match the lambda type.
// Apply may be applied to a level of labels (if no column is specified, the main Series values are used).
// Modifies the underlying Series in place.
func (s *SeriesMutator) Apply(lambda ApplyFn) {
	err := lambda.validate()
	if err != nil {
		s.series.resetWithError((fmt.Errorf("Apply(): %v", err)))
		return
	}
	var data *valueContainer
	// if colName is not specified, use the main values
	if lambda.ColName == "" || lambda.ColName == s.series.values.name {
		data = s.series.values
	} else if lvl, err := findColWithName(lambda.ColName, s.series.labels); err == nil {
		data = s.series.labels[lvl]
	} else {
		s.series.resetWithError(fmt.Errorf("Apply(): %v", err))
		return
	}
	data.slice = data.apply(lambda)
	// set to null if null either prior to or after transformation
	data.isNull = isEitherNull(s.series.values.isNull, setNullsFromInterface(data.slice))
	return
}

// ApplyFormat stub
func (s *Series) ApplyFormat(lambda ApplyFormatFn) *Series {
	s.Copy()
	s.InPlace().ApplyFormat(lambda)
	return s
}

// ApplyFormat stub
func (s *SeriesMutator) ApplyFormat(lambda ApplyFormatFn) {
	err := lambda.validate()
	if err != nil {
		s.series.resetWithError((fmt.Errorf("Apply(): %v", err)))
		return
	}
	var data *valueContainer
	// if colName is not specified, use the main values
	if lambda.ColName == "" || lambda.ColName == s.series.values.name {
		data = s.series.values
	} else if lvl, err := findColWithName(lambda.ColName, s.series.labels); err == nil {
		data = s.series.labels[lvl]
	} else {
		s.series.resetWithError(fmt.Errorf("Apply(): %v", err))
		return
	}
	data.slice = data.applyFormat(lambda)
	// set to null if null either prior to or after transformation
	data.isNull = isEitherNull(s.series.values.isNull, setNullsFromInterface(data.slice))
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
		leftKeys, rightKeys = findMatchingKeysBetweenTwoLabelContainers(s.labels, other.labels)
	} else {
		leftKeys, err = convertColNamesToIndexPositions(leftOn, s.labels)
		if err != nil {
			return seriesWithError(fmt.Errorf("Lookup(): %v", err))
		}
		rightKeys, err = convertColNamesToIndexPositions(rightOn, other.labels)
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
func (s *Series) GroupBy(names ...string) GroupedSeries {
	var index []int
	var err error
	// if no names supplied, group by all label levels
	if len(names) == 0 {
		index = makeIntRange(0, s.Levels())
	} else {
		index, err = convertColNamesToIndexPositions(names, s.labels)
		if err != nil {
			return GroupedSeries{err: fmt.Errorf("GroupBy(): %v", err)}
		}
	}
	g, _, orderedKeys, _ := labelsToMap(s.labels, index)
	labelNames := make([]string, len(index))
	for i, pos := range index {
		labelNames[i] = s.labels[pos].name
	}
	return GroupedSeries{
		groups:      g,
		orderedKeys: orderedKeys,
		series:      s,
		labelNames:  labelNames,
	}
}

// -- ITERATORS

// IterRows returns a slice of maps that return the underlying data for every row in the Series.
// The key in each map is a column header, including label level headers.
// The name of the main values header is the same as the name of the Series itself.
// The value in each map is an Element containing an interface value and a bool denoting if the value is null.
// If multiple columns have the same header, only the Elements of the right-most column are returned.
func (s *Series) IterRows() []map[string]Element {
	ret := make([]map[string]Element, s.Len())
	for i := 0; i < s.Len(); i++ {
		// all label levels + the main Series values
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
	return s.math(sum)
}

// Mean stub
func (s *Series) Mean() float64 {
	return s.math(mean)
}

// Median stub
func (s *Series) Median() float64 {
	return s.math(median)
}

// Std stub
func (s *Series) Std() float64 {
	return s.math(std)
}

// Count stub
func (s *Series) Count() int {
	count := s.math(count)
	return int(count)
}

// Min stub
func (s *Series) Min() float64 {
	return s.math(min)
}

// Max stub
func (s *Series) Max() float64 {
	return s.math(max)
}

// Earliest stub
func (s *Series) Earliest() time.Time {
	return s.timeFunc(earliest)
}

// Latest stub
func (s *Series) Latest() time.Time {
	return s.timeFunc(latest)
}

func (s *Series) math(mathFunction func([]float64, []bool, []int) (float64, bool)) float64 {
	output, _ := mathFunction(
		s.values.float().slice,
		s.values.isNull,
		makeIntRange(0, s.Len()))
	return output
}

func (s *Series) timeFunc(timeFunction func([]time.Time, []bool, []int) (time.Time, bool)) time.Time {
	output, _ := timeFunction(
		s.values.dateTime().slice,
		s.values.isNull,
		makeIntRange(0, s.Len()))
	return output
}

// aligns output with original series. analogous to Pandas transform concept
func (s *Series) alignedMath(alignedFunction func([]float64, []bool, []int) []float64) []float64 {
	retVals := alignedFunction(
		s.values.float().slice,
		s.values.isNull,
		makeIntRange(0, s.Len()))
	return retVals
}

// CumSum returns the cumulative sum at each row position
func (s *Series) CumSum() *Series {
	isNull := make([]bool, s.Len())
	for i := range isNull {
		isNull[i] = false
	}
	return &Series{
		values: &valueContainer{
			slice: s.alignedMath(cumsum),
			// no null values possible
			isNull: isNull,
			name:   "cumsum"},
		labels: s.labels,
	}
}

// Rank stub
func (s *Series) Rank() *Series {
	slice := s.alignedMath(rank)
	isNull := make([]bool, s.Len())
	for i, val := range slice {
		if val == -999 {
			isNull[i] = true
		} else {
			isNull[i] = false
		}
	}
	return &Series{
		values: &valueContainer{
			slice:  slice,
			isNull: isNull,
			name:   "rank",
		},
		labels: s.labels,
	}
}

// Cut stub
func (s *Series) Cut(bins []float64, andLess, andMore bool, labels []string) *Series {
	retSlice, err := s.values.cut(bins, andLess, andMore, labels)
	if err != nil {
		return seriesWithError(fmt.Errorf("Cut(): %v", err))
	}
	retVals := &valueContainer{
		slice:  retSlice,
		isNull: setNullsFromInterface(retSlice),
		name:   s.values.name,
	}
	return &Series{
		values: retVals,
		labels: s.labels,
	}
}

// PercentileCut stub
func (s *Series) PercentileCut(bins []float64, labels []string) *Series {
	retSlice, err := s.values.pcut(bins, labels)
	if err != nil {
		return seriesWithError(fmt.Errorf("Cut(): %v", err))
	}
	retVals := &valueContainer{
		slice:  retSlice,
		isNull: setNullsFromInterface(retSlice),
		name:   s.values.name,
	}
	return &Series{
		values: retVals,
		labels: s.labels,
	}
}

// -- Slicers

// SliceFloat64 coerces the Series values into []float64.
// If `labelLevel` is provided, the column of labels at that level is coerced instead.
// If multiple levels are provides, only the first is used. If the level is out of range, a nil value is returned.
func (s *Series) SliceFloat64(labelLevel ...int) []float64 {
	if len(labelLevel) == 0 {
		return s.values.float().slice
	}
	lvl := labelLevel[0]
	if lvl >= s.Levels() {
		return nil
	}
	return s.labels[lvl].float().slice
}

// SliceString coerces the Series values into []string.
// If `labelLevel` is provided, the column of labels at that level is coerced instead.
// If multiple levels are provides, only the first is used. If the level is out of range, a nil value is returned.
func (s *Series) SliceString(labelLevel ...int) []string {
	if len(labelLevel) == 0 {
		return s.values.str().slice
	}
	lvl := labelLevel[0]
	if lvl >= s.Levels() {
		return nil
	}
	return s.labels[lvl].str().slice
}

// SliceTime coerces the Series values into []time.Time.
// If `labelLevel` is provided, the column of labels at that level is coerced instead.
// If multiple levels are provides, only the first is used. If the level is out of range, a nil value is returned.
func (s *Series) SliceTime(labelLevel ...int) []time.Time {
	if len(labelLevel) == 0 {
		return s.values.dateTime().slice
	}
	lvl := labelLevel[0]
	if lvl >= s.Levels() {
		return nil
	}
	return s.labels[lvl].dateTime().slice
}

// SliceNulls returns whether each value is null or not.
// If `labelLevel` is provided, the column of labels at that level is coerced instead.
// If multiple levels are provides, only the first is used. If the level is out of range, a nil value is returned.
func (s *Series) SliceNulls(labelLevel ...int) []bool {
	if len(labelLevel) == 0 {
		return s.values.isNull
	}
	lvl := labelLevel[0]
	if lvl >= s.Levels() {
		return nil
	}
	return s.labels[lvl].isNull
}
