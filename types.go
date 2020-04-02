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
// * well-suited to conventional IDE-based programming, but also supports notebook usage
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
}

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

// An Element is one value in either a Series or DataFrame.
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

// A FilterFn supplies logic to the Filter() function.
// Only the first field selected (i.e., not left nil) is used - any others are ignored.
// Values are coerced to the type specified in the field (e.g., DateTime -> time.Time) before the filter function is evaluated.
// Once it has been filtered, data retains its original type.
type FilterFn struct {
	Float64   func(val float64) bool
	String    func(val string) bool
	DateTime  func(val time.Time) bool
	Interface func(val interface{}) bool
}

// An ApplyFn supplies logic to the Apply() function.
// Only the first field selected (i.e., not left nil) is used - any others are ignored.
// Values are coerced to the type specified in the field (e.g., DateTime -> time.Time) before the apply function is evaluated.
type ApplyFn struct {
	Float64  func(val float64) float64
	String   func(val string) string
	DateTime func(val time.Time) time.Time
}

// A GroupReduceFn supplies logic to the Reduce() function.
// Only the first field selected (i.e., not left nil) is used - any others are ignored.
// If Interface is selected, the slice of values retains its original type (as interface{}, so must be type-asserted),
// and all output values must have the same type (though this may be different than the original type).
// Otherwise, values are coerced to the type specified in the field (e.g., DateTime -> time.Time) before the reduce function is evaluated.
//
type GroupReduceFn struct {
	Float64   func(slice []float64) float64
	String    func(slice []string) string
	DateTime  func(slice []time.Time) time.Time
	Interface func(slice interface{}) interface{}
}

// An ApplyFormatFn supplies logic to the ApplyFormat() function.
// Only the first field selected (i.e., not left nil) is used - any others are ignored.
// Values are coerced to the type specified in the field (e.g., DateTime -> time.Time) before the formatting function is evaluated.
type ApplyFormatFn struct {
	Float64  func(val float64) string
	DateTime func(val time.Time) string
}

// DType is a DataType that may be used in Sort() or Cast().
type DType int

const (
	// Float64 -> float64
	Float64 DType = iota
	// String -> string
	String
	// DateTime -> time.Time
	DateTime
)

// A WriteOption configures a write function.
// Available write options: WriteOptionExcludeLabels, WriteOptionDelimiter.
type WriteOption func(*writeConfig)

// A writeConfig configures a read function.
// All write functions accept zero or more modifiers that alter the default write config, which is:
// Include labels; "," as field delimiter; and rows as the major dimension of a nested slice.
type writeConfig struct {
	IncludeLabels bool
	Delimiter     rune
}

// A ReadOption configures a read function.
// Available read options: ReadOptionHeaders, ReadOptionLabels, ReadOptionDelimiter, and ReadOptionSwitchDims.
type ReadOption func(*readConfig)

// A readConfig configures a read function.
// All read functions accept zero or more modifiers that alter the default read config, which is:
// 1 header row, 0 label levels, "," as field delimiter, and rows as the major dimension of a nested slice.
type readConfig struct {
	NumHeaderRows  int
	NumLabelLevels int
	Delimiter      rune
	MajorDimIsCols bool
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
//
// In addition, the first `As` field to be selected is applied following truncation.
// If neither `As` field is selected, slice will be time.Time timestamp.
// If `AsCivilDate` is true, slice will be civil.Date (location and time-independent).
// If `AsCivilTime` is true, slice will be civil.Time (location and date-independent).
type Resampler struct {
	ByYear      bool
	ByMonth     bool
	ByDay       bool
	ByWeek      bool
	StartOfWeek time.Weekday
	ByDuration  time.Duration
	Location    *time.Location

	AsCivilDate bool // slice will be civil.Date after truncation
	AsCivilTime bool // slice will be civil.Time after truncation
}

// Cutter supplies logic for the Cut() function.
// If `AndLess` is true, a bin is added that ranges between negative infinity and the first bin value.
// If `AndMore` is true, a bin is added that ranges between the last bin value and positive infinity.
// If `Labels` is not nil, then category names correspond to labels, and the number of labels must be one less than the number of bin values.
// Otherwise, category names are auto-generated from the range of the bin intervals.
type Cutter struct {
	AndLess bool
	AndMore bool
	Labels  []string
}
