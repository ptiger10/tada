// Package tada (TAble DAta) enables test-driven data pipelines.
//
// tada combines concepts from pandas, spreadsheets, R, Apache Spark, and SQL.
// Its most common use cases are cleaning, aggregating, transforming, and analyzing data.
// Some notable features of tada:
//
// * flexible constructor that supports most primitive data types
//
// * seamlessly handles null data and type conversions
//
// * robust datetime support
//
// * advanced filtering, lookups and merging, grouping, sorting, and pivoting
//
// * multi-level labels and columns
//
// * complete test coverage
//
// * interoperable with existing pandas dataframes via Apache Arrow
//
// The key data types are Series, DataFrames, and groupings of each.
// A Series is analogous to one column of a spreadsheet, and a DataFrame is analogous to a whole spreadsheet.
// Printing either data type will render an ASCII table.
//
// Both Series and DataFrames have one or more "label levels".
// On printing, these appear as the leftmost columns in a table, and typically have values that help identify ("label") specific rows.
// They are analogous to the "index" concept in pandas.
//
// For more detail and implementation notes, see https://docs.google.com/document/d/18DvZzd6Tg6Bz0SX0fY2SrXOjE8d9xDhU6bDEnaIc_rM/
package tada

import (
	"time"
)

type valueContainer struct {
	slice  interface{}
	isNull []bool
	cache  []string
	name   string
	id     string
}

var tadaID = "tadaID_"

// A Series is a single column of data with one or more levels of aligned labels.
type Series struct {
	values     *valueContainer
	labels     []*valueContainer
	sharedData bool
	err        error
}

// A SeriesIterator iterates over the rows in a Series.
type SeriesIterator struct {
	current int
	s       *Series
}

// A SeriesMutator is used to change Series values in place.
type SeriesMutator struct {
	series *Series
}

// A DataFrame is one or more columns of data with one or more levels of aligned labels.
// A DataFrame is analogous to a spreadsheet.
type DataFrame struct {
	labels        []*valueContainer
	values        []*valueContainer
	name          string
	err           error
	colLevelNames []string
}

// A DataFrameIterator iterates over the rows in a DataFrame.
type DataFrameIterator struct {
	current int
	df      *DataFrame
}

// A DataFrameMutator is used to change DataFrame values in place.
type DataFrameMutator struct {
	dataframe *DataFrame
}

// A GroupedSeries is a collection of row positions sharing the same group key.
// A GroupedSeries has a reference to an underlying Series, which is used for reduce operations.
type GroupedSeries struct {
	orderedKeys []string
	rowIndices  [][]int
	labels      []*valueContainer
	series      *Series
	aligned     bool
	err         error
}

// GroupedSeriesIterator iterates over all Series in the group.
type GroupedSeriesIterator struct {
	current    int
	rowIndices [][]int
	s          *Series
}

// A GroupedDataFrame is a collection of row positions sharing the same group key.
// A GroupedDataFrame has a reference to an underlying DataFrame, which is used for reduce operations.
type GroupedDataFrame struct {
	orderedKeys []string
	rowIndices  [][]int
	labels      []*valueContainer
	df          *DataFrame
	aligned     bool
	err         error
}

// GroupedDataFrameIterator iterates over all DataFrames in the group.
type GroupedDataFrameIterator struct {
	current    int
	rowIndices [][]int
	df         *DataFrame
}

// Matrix is an interface which is compatible with gonum's mat.Matrix interface
type Matrix interface {
	Dims() (r, c int)
	At(i, j int) float64
}

type floatValueContainer struct {
	slice  []float64
	isNull []bool
	index  []int
}

type stringValueContainer struct {
	slice  []string
	isNull []bool
	index  []int
}

type dateTimeValueContainer struct {
	slice  []time.Time
	isNull []bool
	index  []int
}

// A Sorter supplies details to the Sort() function.
// `Name` specifies the container (either label or column name) to sort.
// If `Descending` is true, values are sorted in descending order.
// `DType` specifies the data type to which values will be coerced before they are sorted (default: float64).
// Null values are always sorted to the bottom.
type Sorter struct {
	Name       string
	Descending bool
	DType      DType
}

// An Element is one {value, null status} pair in either a Series or DataFrame.
type Element struct {
	Val    interface{}
	IsNull bool
}

// NullFiller fills every row with a null value and changes the row status to not-null.
// If multiple fields are provided, resolves in the following order:
// 1) `FillForward` - fills with the last valid value,
// 2) `FillBackward` - fills with the next valid value,
// 3) `FillZero` - fills with the zero type of the slice,
// 4) `FillFloat` - coerces to float64 and fills with the value provided.
type NullFiller struct {
	FillForward  bool
	FillBackward bool
	FillZero     bool
	FillFloat    float64
}

// A FilterFn is an anonymous function supplied to a Filter or Where function.
// The function will be called on every val in the container.
type FilterFn func(value interface{}) bool

// An ApplyFn is an anonymous function supplied to an Apply function to convert one slice to another.
// The function input will be a slice, and it must return a slice of equal length (though the type may be different).
// isNull contains the null status of every row in the input slice.
// The null status of a row may be changed by setting that row's isNull element within the function body.
type ApplyFn func(slice interface{}, isNull []bool) (equalLengthSlice interface{})

// A ReduceFn is an anonymous function supplied to a Reduce function
// to reduce a slice of values to one value and one null status per group.
// isNull contains the null status of every value in the group.
type ReduceFn func(slice interface{}, isNull []bool) (value interface{}, null bool)

// DType is a DataType that may be used in Sort() or Cast().
type DType int

const (
	// Float64 -> float64
	Float64 DType = iota
	// String -> string
	String
	// DateTime -> time.Time
	DateTime // always tz-aware
	// Time -> civil.Time
	Time
	// Date -> civil.Date
	Date
)

// A WriteOption configures a write function.
// Available write options: WriteOptionExcludeLabels, WriteOptionDelimiter.
type WriteOption func(*writeConfig)

// A writeConfig configures a read function.
// All write functions accept zero or more modifiers that alter the default write config, which is:
// Include labels; "," as field delimiter; and rows as the major dimension of a nested slice.
type writeConfig struct {
	includeLabels bool
	delimiter     rune
}

// A ReadOption configures a read function.
// Available read options: ReadOptionHeaders, ReadOptionLabels, ReadOptionDelimiter, and ReadOptionSwitchDims.
type ReadOption func(*readConfig)

// A readConfig configures a read function.
// All read functions accept zero or more modifiers that alter the default read config, which is:
// 1 header row, 0 label levels, "," as field delimiter, and rows as the major dimension of a nested slice.
type readConfig struct {
	numHeaderRows  int
	numLabelLevels int
	delimiter      rune
	majorDimIsCols bool
}

// A JoinOption configures a lookup or merge function.
// Available lookup options: JoinOptionHow, JoinOptionLeftOn, JoinOptionRightOn
type JoinOption func(*joinConfig)

// A joinConfig configures a lookup or merge function.
// All lookup/merge functions accept zero or more modifiers that alter the default read config, which is:
// left join, no specified join keys (so automatically uses shared label names as keys)
type joinConfig struct {
	how     string
	leftOn  []string
	rightOn []string
}

// Resampler supplies logic for the Resample() function.
// Only the first `By` field that is selected (i.e., not left nil) is used - any others are ignored
// (if `ByWeek` is selected, it may be modified by `StartOfWeek`).
// `ByYear` truncates the timestamp by year.
// `ByMonth` truncates the timestamp by month.
// `ByDay` truncates the timestamp by day.
// `ByWeek` returns the first day of the most recent week (starting on `StartOfWeek`) relative to timestamp.
// Otherwise, truncates the timestamp `ByDuration`.
// If `Location` is not provided, time.UTC is used as the default location.
type Resampler struct {
	ByYear      bool
	ByMonth     bool
	ByDay       bool
	ByWeek      bool
	StartOfWeek time.Weekday
	ByDuration  time.Duration
	Location    *time.Location
}

// Binner supplies logic for the Bin() function.
// If `AndLess` is true, a bin is added that ranges between negative infinity and the first bin value.
// If `AndMore` is true, a bin is added that ranges between the last bin value and positive infinity.
// If `Labels` is not nil, then category names correspond to labels, and the number of labels must be one less than the number of bin values.
// Otherwise, category names are auto-generated from the range of the bin intervals.
type Binner struct {
	AndLess bool
	AndMore bool
	Labels  []string
}

// A StructTransposer is a row-oriented representation of a DataFrame
// that can be randomly shuffled or transposed into a column-oriented struct representation of a DataFrame.
// It is useful for intuitive row-oriented testing.
type StructTransposer [][]interface{}

// clocker interface for generating random numbers
var clock clocker = realClock{}
