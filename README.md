![tada logo](logo.png)

[![Go Report Card](https://goreportcard.com/badge/github.com/ptiger10/tada)](https://goreportcard.com/report/github.com/ptiger10/tada) 
[![GoDoc](https://godoc.org/github.com/ptiger10/tada?status.svg)](https://godoc.org/github.com/ptiger10/tada) 
[![Build Status](https://travis-ci.org/ptiger10/tada.svg?branch=master)](https://travis-ci.org/ptiger10/tada)
[![codecov](https://codecov.io/gh/ptiger10/tada/branch/master/graph/badge.svg)](https://codecov.io/gh/ptiger10/tada)

# tada
tada (TAble DAta) is a package that enables test-driven data pipelines in pure Go.

**DISCLAIMER: still under development. API subject to breaking changes until v1.**

**If you still want to use this regardless of the disclaimer, congratulations, you are an alpha tester! Please DM your feedback to me on the Gophers slack channel (Dave Fort) or create an issue.**

tada combines concepts from pandas, spreadsheets, R, Apache Spark, and SQL.
Its most common use cases are cleaning, aggregating, transforming, and analyzing data.

Some notable features of tada:
* flexible constructor that supports most primitive data types
* seamlessly handles null data and type conversions
* robust datetime support
* advanced filtering, lookups and merging, grouping, sorting, and pivoting
* multi-level labels and columns
* complete test coverage
* interoperable with existing pandas dataframes via Apache Arrow
* comparable to pandas [performance](comparison_summary.txt) on key operations

The key data types are Series, DataFrames, and groupings of each.
A Series is analogous to one column of a spreadsheet, and a DataFrame is analogous to a whole spreadsheet.
Printing either data type will render an ASCII table.

Both Series and DataFrames have one or more "label levels".
On printing, these appear as the leftmost columns in a table, and typically have values that help identify ("label") specific rows.
They are analogous to the "index" concept in pandas.

For more detail and implementation notes, see [this doc](https://docs.google.com/document/d/18DvZzd6Tg6Bz0SX0fY2SrXOjE8d9xDhU6bDEnaIc_rM/edit?usp=sharing).

*Logo: @egonelbre, licensed under CC0*

## Example
You start with a CSV. Like most real-world data, it is messy. This one is missing a score in the first row. And we know that scores must range between 0 and 10, so the score of -100 and 1000 in the second and third rows must also be erroneous:
```
var data = `name, score
            joe doe,
            john doe, -100
            jane doe, 1000
            john doe, 5
            jane doe, 8
            john doe, 7
            jane doe, 10`
```
You want to write and validate a function that discards erroneous data, groups by the `name` column, and returns the mean of the groups. 

First you write a test. You can test in two ways:
### Comparing to stringified csv (compares stringified values, regardless of type)
```
func TestDataPipeline(t *testing.T) {
  want := `name, mean_score
           jane doe, 9
           john doe, 6`


  df, _ := tada.ReadCSV(strings.NewReader(data))
  ret := sampleDataPipeline(df)
  eq, diffs, _ := ret.EqualsCSV(true, strings.NewReader(want))
  if !eq {
    t.Errorf("sampleDataPipeline(): got %v, want %v, has diffs: \n%v", ret, want, diffs)
  }
}
```

### Comparing to struct (comapres typed values)
```
func Test_sampleDataPipelineTyped(t *testing.T) {
	type output struct {
		Name      []string  `tada:"name"`
		MeanScore []float64 `tada:"mean_score"`
	}
	want := output{
		Name:      []string{"jane doe", "john doe"},
		MeanScore: []float64{9, 5},
	}

	df, _ := ReadCSV(strings.NewReader(data))

	out := sampleDataPipeline(df)
	var got output
	out.Struct(&got)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("sampleDataPipelineTyped(): got %v, want %v", got, want)
	}
}
```

Then you write the data pipeline:
```
func sampleDataPipeline(df *DataFrame) *DataFrame {
	err := df.HasCols("name", "score")
	if err != nil {
		log.Fatal(err)
	}
	df.InPlace().DropNull()
	validScore := FilterFn{Float64: func(v float64) bool { return v >= 0 && v <= 10 }}
	df.InPlace().Filter(map[string]FilterFn{"score": validScore})
	df.InPlace().Sort(Sorter{Name: "name", DType: String})
	return df.GroupBy("name").Mean("score")
}
```
More [examples](https://godoc.org/github.com/ptiger10/tada#pkg-examples)



## Usage
### Constructor:
#### Series
`s := tada.NewSeries([]float{1,2,3})`
##### with one level of labels
`s := tada.NewSeries([]float{1,2,3}, []string{"foo", "bar", "baz"})`
#### DataFrame
```
df := tada.NewDataFrame([]interface{}{
  []string{"a"}, 
  []float64{100},
}).SetColNames([]string{"foo", "bar"})
```

### Reading from CSV
```
f, err := os.Open("foo.csv")
... handle err
defer f.Close()
df, err := tada.ReadCSV(f)
... handle err
```

More [examples](https://godoc.org/github.com/ptiger10/tada#pkg-examples)

## Performance Tuning
* Modify a Series or DataFrame in place (without returning a new copy) by first calling `InPlace()`.
* If you expect to use a column as numeric, string, or time.Time values multiple times, `Cast()` it to `tada.Float64`, `tada.String`, or `tada.DateTime`, respectively.

## Inter-process communication (IPC)
* Apache Arrow
  * Read from existing Pandas dataframes using the Apache Arrow specification. 
  * Because the go/arrow library is still not v1.0, convenience functions and patterns are versioned in a [separate repo](https://github.com/ptiger10/tada-io).