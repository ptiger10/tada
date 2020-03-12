// Package tada (TAble DAta) facilitates test-driven data pipelines.
// The key data structures are Series, DataFrames, and grouped versions of each.
// tada combines concepts from popular spreadsheet applications, Python pandas, Apache Spark, and SQL.
// The most common use cases for tada are exploring data, building extract, transform, and load (ETL) processes, and automating analytics.
// Some notable features of tada:
// * flexible constructor that supports most primitive data types
// * seamlessly handles null data and type conversions
// * well-suited to either the Jupyter notebook style of data exploration or conventional IDE-based programming
// * advanced filtering, grouping, and pivoting
// * hierarchical indexing (i.e., multi-level indexes and columns)
// * reads from either CSV or any spreadsheet or tabular data structured as [][]interface (e.g., Google Sheets)
// * complete test coverage
package tada

import (
	"time"
)

type valueContainer struct {
	slice    interface{}
	isNull   []bool
	cache    [][]byte
	newCache []string
	name     string
}

// A Series is a single column of data with one or more levels of aligned labels.
type Series struct {
	values     *valueContainer
	labels     []*valueContainer
	sharedData bool
	err        error
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

// A GroupedDataFrame is a collection of row positions sharing the same group key.
// A GroupedDataFrame has a reference to an underlying DataFrame, which is used for reduce operations.
type GroupedDataFrame struct {
	orderedKeys []string
	rowIndices  [][]int
	labels      []*valueContainer
	df          *DataFrame
	err         error
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
	GreaterThan float64
	LessThan    float64
	Contains    string
	Before      time.Time
	After       time.Time
	Float64     func(val float64) bool
	String      func(val string) bool
	DateTime    func(val time.Time) bool
	Interface   func(val interface{}) bool
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

// ReadConfig supplies configuration details to a Read function.
// `NumHeaderRows` specifies the number of rows at the top of the data that should be designated as column headers.
// `NumLabelCols` specifies the number of columns starting from the left of the data that should be designated as label levels.
// `Delimiter` specifies a custom field delimiter for use in ImportCSV (in the standard csv library, this delimiter is called Comma).
// If `MajorDimIsCols` is false, the data is read as though the major dimension is rows (the default for the standard csv library).
//
// For example, when reading this data: [["foo", "bar"], ["baz", "qux"]]
// `MajorDimIsCols` = false   		`MajorDimIsCols` = true
// (major dimension: rows)			(major dimension: columns)
//	foo bar							foo baz
//  baz qux							bar qux
type ReadConfig struct {
	NumHeaderRows  int
	NumLabelCols   int
	Delimiter      rune
	MajorDimIsCols bool
}

// Resampler supplies logic for the Resample() function.
// Only the first field selected (i.e., not left nil) is used - any others are ignored (except for `ByWeek` and `StartOfWeek`).
// If true, `ByYear` truncates the timestamp by year.
// If true, `ByMonth` truncates the timestamp by month.
// If true, `ByDay` truncates the timestamp by day.
// If true, `ByWeek` returns the first day of the most recent week (starting on `StartOfWeek`) relative to timestamp.
// Otherwise, truncates the timestamp `ByDuration`.
type Resampler struct {
	ByYear      bool
	ByMonth     bool
	ByDay       bool
	ByWeek      bool
	StartOfWeek time.Weekday
	ByDuration  time.Duration
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
