package tada

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"time"

	"github.com/ptiger10/tablediff"
)

// -- CONSTRUCTORS

// NewSeries constructs a Series from a slice of values and optional label slices.
// Supported underlying slice types: real numbers, string, time.Time, boolean, or interface.
func NewSeries(slice interface{}, labels ...interface{}) *Series {
	// handle values
	values, err := makeValueContainerFromInterface(slice, "0")
	if err != nil {
		return seriesWithError(fmt.Errorf("NewSeries(): `slice`: %v", err))
	}

	// handle labels
	retLabels, err := makeValueContainersFromInterfaces(labels, true)
	if err != nil {
		return seriesWithError(fmt.Errorf("NewSeries(): `labels`: %v", err))
	}
	if len(retLabels) == 0 {
		// default labels
		defaultLabels := makeDefaultLabels(0, reflect.ValueOf(slice).Len())
		retLabels = append(retLabels, defaultLabels)
	}

	return &Series{values: values, labels: retLabels}
}

// Copy returns a deep copy of a Series with no shared references to the original.
func (s *Series) Copy() *Series {
	return &Series{
		values:     s.values.copy(),
		labels:     copyContainers(s.labels),
		err:        s.err,
		sharedData: false,
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

// EqualsCSV converts a Series to csv, compares it to another csv, and evaluates whether the two match and isolates their differences
func (s *Series) EqualsCSV(csv [][]string, ignoreLabels bool) (bool, *tablediff.Differences) {
	compare, _ := s.ToCSV(ignoreLabels)
	diffs, eq := tablediff.Diff(compare, csv)
	return eq, diffs
}

// ToCSV converts a Series to a DataFrame and returns as [][]string.
func (s *Series) ToCSV(ignoreLabels bool) ([][]string, error) {
	if s.values == nil {
		return nil, fmt.Errorf("ToCSV(): cannot export empty Series")
	}
	df := &DataFrame{
		values:        []*valueContainer{s.values},
		labels:        s.labels,
		colLevelNames: []string{"*0"},
		err:           s.err,
	}
	csv := df.ToCSV(ignoreLabels)
	return csv, nil
}

// -- GETTERS

func (s *Series) String() string {
	return s.ToDataFrame().String()
}

// Err returns the most recent error attached to the Series, if any.
func (s *Series) Err() error {
	return s.err
}

// At returns the Element at the `index` position. If `index` is out of range, returns an empty Element.
func (s *Series) At(index int) Element {
	if index >= s.Len() {
		return Element{}
	}
	v := reflect.ValueOf(s.values.slice)
	return Element{
		Val:    v.Index(index).Interface(),
		IsNull: s.values.isNull[index],
	}
}

// Len returns the number of rows in the Series.
func (s *Series) Len() int {
	return reflect.ValueOf(s.values.slice).Len()
}

// numLevels returns the number of columns of labels in the Series.
func (s *Series) numLevels() int {
	return len(s.labels)
}

// Cast casts the underlying Series slice values to either []float64, []string, or []time.Time.
// Use cast to improve performance when calling multiple operations on values.
func (s *Series) Cast(dtype DType) {
	s.values.cast(dtype)
	return
}

// IndexOf stub. If name does not match any container, -1 is returned
func (s *Series) IndexOf(name string) int {
	i, err := indexOfContainer(name, s.labels)
	if err != nil {
		return -1
	}
	return i
}

// SelectLabels finds the first level with matching `name` and returns as a Series with all existing label levels (including itself).
// If label level name is default (prefixed with *), removes the prefix.
// Returns a new Series with shared labels.
func (s *Series) SelectLabels(name string) *Series {
	index, err := indexOfContainer(name, s.labels)
	if err != nil {
		return seriesWithError(fmt.Errorf("SelectLabels(): %v", err))
	}
	values := s.labels[index]
	retValues := &valueContainer{
		slice:  values.slice,
		isNull: values.isNull,
		name:   removeDefaultNameIndicator(values.name),
	}
	return &Series{
		values:     retValues,
		labels:     s.labels,
		sharedData: true,
	}
}

// Subset returns only the rows specified at the index positions, in the order specified. Returns a new Series.
// Returns a new Series.
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
			"Subset(): likely invalid filter (every filter must have at least one filter function; if ColName is supplied, it must be valid)"))
		return
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

// SwapLabels swaps the label levels with names `i` and `j`.
// Returns a new Series.
func (s *Series) SwapLabels(i, j string) *Series {
	s = s.Copy()
	s.InPlace().SwapLabels(i, j)
	return s
}

// SwapLabels swaps the label levels with names `i` and `j`.
// Modifies the underlying Series in place.
func (s *SeriesMutator) SwapLabels(i, j string) {
	index1, err := indexOfContainer(i, s.series.labels)
	if err != nil {
		s.series.resetWithError(fmt.Errorf("SwapLabels(): `i`: %v", err))
		return
	}
	index2, err := indexOfContainer(j, s.series.labels)
	if err != nil {
		s.series.resetWithError(fmt.Errorf("SwapLabels(): `j`: %v", err))
		return
	}
	s.series.labels[index1], s.series.labels[index2] = s.series.labels[index2], s.series.labels[index1]
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
	labels, err := subsetContainers(s.series.labels, index)
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
	retLabels := make([]*valueContainer, s.numLevels())
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
	retLabels := make([]*valueContainer, s.numLevels())
	for j := range s.labels {
		retLabels[j] = s.labels[j].tail(n)
	}
	return &Series{values: retVals, labels: retLabels}
}

// Range returns the rows of the Series starting at `first` and `ending` immediately prior to last (left-inclusive, right-exclusive).
// If either `first` or `last` is out of range, a Series error is returned.
// In all cases, returns a new Series.
func (s *Series) Range(first, last int) *Series {
	if first > last {
		return seriesWithError(fmt.Errorf("Range(): first is greater than last (%d > %d)", first, last))
	}
	if first >= s.Len() {
		return seriesWithError(fmt.Errorf("Range(): first index out of range (%d > %d)", first, s.Len()-1))
	} else if last > s.Len() {
		return seriesWithError(fmt.Errorf("Range(): last index out of range (%d > %d)", last, s.Len()))
	}
	retVals := s.values.rangeSlice(first, last)
	retLabels := make([]*valueContainer, s.numLevels())
	for j := range s.labels {
		retLabels[j] = s.labels[j].rangeSlice(first, last)
	}
	return &Series{values: retVals, labels: retLabels}
}

// FillNull fills all the null values and makes them not-null.
// Returns a new Series.
func (s *Series) FillNull(how NullFiller) *Series {
	s = s.Copy()
	s.InPlace().FillNull(how)
	return s
}

// FillNull fills all the null values and makes them not-null.
// Modifies the underlying Series.
func (s *SeriesMutator) FillNull(how NullFiller) {
	s.series.values.fillnull(how)
	return
}

// ValueIsNull returns whether the value at row `i` is null. If `i` is out of range, returns true.
func (s *Series) ValueIsNull(i int) bool {
	if i >= s.Len() {
		return true
	}
	return s.values.isNull[i]
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

// Shift replaces the value in row i with the value in row i - `n`, or null if that index is out of range.
// Returns a new Series.
func (s *Series) Shift(n int) *Series {
	s = s.Copy()
	s.InPlace().Shift(n)
	return s
}

// Shift replaces the value in row i with the value in row i - `n`, or null if that index is out of range.
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
	if optionSharedDataWarning && s.sharedData {
		log.Print(
			"WARNING: this Series shares its labels and values with the Series/DataFrame " +
				"from which it was derived, so InPlace changes will modify those objects too. " +
				"To avoid this, make a new Series with Series.Copy()")
	}
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
		return
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
		return
	}
	s.series.values = vals[0]
	return
}

// DropRow removes the row at the specified index.
// Returns a new Series.
func (s *Series) DropRow(index int) *Series {
	s.Copy()
	s.InPlace().DropRow(index)
	return s
}

// DropRow removes the row at the specified index.
// Modifies the underlying Series in place.
func (s *SeriesMutator) DropRow(index int) {
	err := s.series.values.dropRow(index)
	if err != nil {
		s.series.resetWithError(fmt.Errorf("DropRow(): %v", err))
		return
	}
	for j := range s.series.labels {
		s.series.labels[j].dropRow(index)
	}
}

// DropLabels removes the first label level matching `name`.
// Returns a new Series.
func (s *Series) DropLabels(name string) *Series {
	s.Copy()
	s.InPlace().DropLabels(name)
	return s
}

// DropLabels removes the first label level matching `name`.
// Modifies the underlying Series in place.
func (s *SeriesMutator) DropLabels(name string) {
	newCols, err := dropFromContainers(name, s.series.labels)
	if err != nil {
		s.series.resetWithError(fmt.Errorf("DropLabels(): %v", err))
		return
	}
	s.series.labels = newCols
	return
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
			fmt.Errorf("Append(): other Series must have same number of label levels as original Series (%d != %d)",
				len(other.labels), len(s.series.labels)))
		return
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

// Relabel stub
func (s *Series) Relabel(levelNames []string) *Series {
	s = s.Copy()
	s.InPlace().Relabel(levelNames)
	return s
}

// Relabel stub
func (s *SeriesMutator) Relabel(levelNames []string) {
	for _, name := range levelNames {
		lvl, err := indexOfContainer(name, s.series.labels)
		if err != nil {
			s.series.resetWithError(fmt.Errorf("Relabel(): %v", err))
			return
		}
		s.series.labels[lvl].relabel()
	}
	return
}

// SetLabelNames sets the names of all the label levels in the Series and returns the entire Series.
func (s *Series) SetLabelNames(levelNames []string) *Series {
	if len(levelNames) != len(s.labels) {
		return seriesWithError(
			fmt.Errorf("SetLabelNames(): number of `levelNames` must match number of levels in Series (%d != %d)", len(levelNames), len(s.labels)))
	}
	for j := range levelNames {
		s.labels[j].name = levelNames[j]
	}
	return s
}

// ListLabelNames returns the name and position of all the label levels in the Series
func (s *Series) ListLabelNames() []string {
	return listNames(s.labels)
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
	// default for handling no Sorters: values as float in ascending order
	if len(by) == 0 {
		by = []Sorter{{Name: s.series.values.name, DType: Float, Descending: false}}
	}
	// replace "" with values
	for i := range by {
		if by[i].Name == "" {
			by[i].Name = s.series.values.name
		}
	}
	mergedLabelsAndValues := append(s.series.labels, s.series.values)
	newIndex, err := sortContainers(mergedLabelsAndValues, by)
	if err != nil {
		s.series.resetWithError(fmt.Errorf("Sort(): %v", err))
		return
	}
	// rearrange the data in place with the final index
	s.Subset(newIndex)
	return
}

func filter(containers []*valueContainer, filters map[string]FilterFn) ([]int, error) {
	// subIndexes contains the index positions computed across all the filters
	var subIndexes [][]int
	for containerName, filter := range filters {
		err := filter.validate()
		if err != nil {
			return nil, fmt.Errorf("filter: %v", err)
		}
		index, err := indexOfContainer(containerName, containers)
		if err != nil {
			return nil, fmt.Errorf("filter: %v", err)
		}
		subIndex, err := containers[index].filter(filter)
		if err != nil {
			return nil, fmt.Errorf("filter: %v", err)
		}
		subIndexes = append(subIndexes, subIndex)
	}
	// reduce the subindexes to a single index that shares all the values
	return intersection(subIndexes), nil
}

// -- FILTERS

// Filter applies one or more filters to the containers in the Series
// and returns the intersection of the row positions that satisfy all filters.
// Filter may be applied to any label level by supplying the label name as a key in the `filter` map.
// Filter may be applied to the Series values by supplying as key either the Series name or an empty string ("").
// If no filter is provided, returns a list of all index positions in the Series.
// In event of error, returns []int{-999}
func (s *Series) Filter(filters map[string]FilterFn) []int {
	if len(filters) == 0 {
		return makeIntRange(0, s.Len())
	}
	// replace "" with values
	for k, v := range filters {
		if k == "" {
			filters[s.values.name] = v
			delete(filters, k)
		}
	}

	mergedLabelsAndValues := append(s.labels, s.values)
	ret, err := filter(mergedLabelsAndValues, filters)
	if err != nil {
		return []int{-999}
	}
	return ret
}

// Where stub
// To do: check for bad filter
func (s *Series) Where(filters map[string]FilterFn, ifTrue, ifFalse interface{}) *Series {
	ret := make([]interface{}, s.Len())
	index := s.Filter(filters)
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

// BeforeOrEqual coerces Series values to time.Time and returns
// each row position with a non-null value before or equal to `comparison`.
func (s *Series) BeforeOrEqual(comparison time.Time) []int {
	return s.values.beforeOrEqual(comparison)
}

// After coerces Series values to time.Time and returns each row position with a non-null value after `comparison`.
func (s *Series) After(comparison time.Time) []int {
	return s.values.after(comparison)
}

// AfterOrEqual coerces Series values to time.Time and returns
// each row position with a non-null value after or equal to `comparison`.
func (s *Series) AfterOrEqual(comparison time.Time) []int {
	return s.values.afterOrEqual(comparison)
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
	s.series.values.slice = s.series.values.apply(lambda)
	// set to null if null either prior to or after transformation
	s.series.values.isNull = isEitherNull(s.series.values.isNull, setNullsFromInterface(s.series.values.slice))
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
		s.series.resetWithError((fmt.Errorf("ApplyFormat(): %v", err)))
		return
	}
	s.series.values.slice = s.series.values.applyFormat(lambda)
	// set to null if null either prior to or after transformation
	s.series.values.isNull = isEitherNull(s.series.values.isNull, setNullsFromInterface(s.series.values.slice))
	return
}

// -- MERGERS

// Lookup stub
func (s *Series) Lookup(other *Series) *Series {
	return s.LookupAdvanced(other, "left", nil, nil)
}

// LookupAdvanced stub
func (s *Series) LookupAdvanced(other *Series, how string, leftOn []string, rightOn []string) *Series {
	var leftKeys, rightKeys []int
	var err error
	if len(leftOn) == 0 || len(rightOn) == 0 {
		if !(len(leftOn) == 0 && len(rightOn) == 0) {
			return seriesWithError(fmt.Errorf("LookupAdvanced(): if either leftOn or rightOn is empty, both must be empty"))
		}
	}
	if len(leftOn) == 0 {
		leftKeys, rightKeys = findMatchingKeysBetweenTwoLabelContainers(s.labels, other.labels)
	} else {
		leftKeys, err = convertColNamesToIndexPositions(leftOn, s.labels)
		if err != nil {
			return seriesWithError(fmt.Errorf("LookupAdvanced(): %v", err))
		}
		rightKeys, err = convertColNamesToIndexPositions(rightOn, other.labels)
		if err != nil {
			return seriesWithError(fmt.Errorf("LookupAdvanced(): %v", err))
		}
	}
	ret, err := lookup(how, s.values, s.labels, leftKeys, other.values, other.labels, rightKeys)
	if err != nil {
		return seriesWithError(fmt.Errorf("LookupAdvanced(): %v", err))
	}
	return ret
}

// Merge stub
func (s *Series) Merge(other *Series) *DataFrame {
	return s.ToDataFrame().Merge(other.ToDataFrame())
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
	var index []int
	var err error
	// if no names supplied, group by all label levels
	if len(names) == 0 {
		index = makeIntRange(0, s.numLevels())
	} else {
		index, err = convertColNamesToIndexPositions(names, s.labels)
		if err != nil {
			return &GroupedSeries{err: fmt.Errorf("GroupBy(): %v", err)}
		}
	}
	newLabels, rowIndices, orderedKeys, _ := reduceContainers(s.labels, index)
	return &GroupedSeries{
		orderedKeys: orderedKeys,
		rowIndices:  rowIndices,
		labels:      newLabels,
		series:      s,
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
		ret[i] = make(map[string]Element, s.numLevels()+1)
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
	return s.floatFunc(sum)
}

// Mean stub
func (s *Series) Mean() float64 {
	return s.floatFunc(mean)
}

// Median stub
func (s *Series) Median() float64 {
	return s.floatFunc(median)
}

// Std stub
func (s *Series) Std() float64 {
	return s.floatFunc(std)
}

// Count stub
func (s *Series) Count() int {
	count := s.floatFunc(count)
	return int(count)
}

// NUnique stub
func (s *Series) NUnique() int {
	unique := s.stringFunc(nunique)
	i, _ := strconv.Atoi(unique)
	return i
}

// Min stub
func (s *Series) Min() float64 {
	return s.floatFunc(min)
}

// Max stub
func (s *Series) Max() float64 {
	return s.floatFunc(max)
}

// Earliest stub
func (s *Series) Earliest() time.Time {
	return s.timeFunc(earliest)
}

// Latest stub
func (s *Series) Latest() time.Time {
	return s.timeFunc(latest)
}

func (s *Series) floatFunc(floatFunction func([]float64, []bool, []int) (float64, bool)) float64 {
	output, _ := floatFunction(
		s.values.float64().slice,
		s.values.isNull,
		makeIntRange(0, s.Len()))
	return output
}

func (s *Series) stringFunc(stringFunction func([]string, []bool, []int) (string, bool)) string {
	output, _ := stringFunction(
		s.values.string().slice,
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
		s.values.float64().slice,
		s.values.isNull,
		makeIntRange(0, s.Len()))
	return retVals
}

// Resample stub
func (s *Series) Resample(by Resampler) *Series {
	s = s.Copy()
	s.InPlace().Resample(by)
	return s
}

// Resample stub
func (s *SeriesMutator) Resample(by Resampler) {
	s.series.values.resample(by)
	return
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
		return seriesWithError(fmt.Errorf("PercentileCut(): %v", err))
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
func (s *Series) SliceFloat64() []float64 {
	output := make([]float64, s.Len())
	copy(output, s.values.float64().slice)
	return output
}

// SliceString coerces the Series values into []string.
func (s *Series) SliceString() []string {
	output := make([]string, s.Len())
	copy(output, s.values.string().slice)
	return output

}

// SliceTime coerces the Series values into []time.Time.
func (s *Series) SliceTime() []time.Time {
	output := make([]time.Time, s.Len())
	copy(output, s.values.dateTime().slice)
	return output
}

// SliceNulls returns whether each value is null or not.
func (s *Series) SliceNulls() []bool {
	output := make([]bool, s.Len())
	copy(output, s.values.isNull)
	return output
}

// Interface returns a copy of the underlying Series data as an interface.
func (s *Series) Interface() interface{} {
	ret := s.values.copy()
	return ret.slice
}

// DType returns the slice type of the underlying Series values
func (s *Series) DType() string {
	return s.values.dtype()
}

// ValueCounts stub
func (s *Series) ValueCounts() map[string]int {
	return s.values.valueCounts()
}

// Unique returns the first appearance of all non-null values in the Series.
func (s *Series) Unique(valuesOnly bool) *Series {
	var index []int
	if valuesOnly {
		index = s.values.uniqueIndex()
	} else {
		mergedLabelsAndValues := append(s.labels, s.values)
		index = multiUniqueIndex(mergedLabelsAndValues)
	}
	return s.Subset(index)
}
