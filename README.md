![tada logo](logo.png)

[![Go Report Card](https://goreportcard.com/badge/github.com/ptiger10/tada)](https://goreportcard.com/report/github.com/ptiger10/tada) 
[![GoDoc](https://godoc.org/github.com/ptiger10/tada?status.svg)](https://godoc.org/github.com/ptiger10/tada) 
[![Build Status](https://travis-ci.org/ptiger10/tada.svg?branch=master)](https://travis-ci.org/ptiger10/tada)
[![codecov](https://codecov.io/gh/ptiger10/tada/branch/master/graph/badge.svg)](https://codecov.io/gh/ptiger10/tada)

# tada
tada (TAble DAta) is a package that enables test-driven data pipelines in pure Go.

**DISCLAIMER: still under development. API subject to breaking changes until v1. Use in production at your own risk.**

tada combines concepts from pandas, spreadsheets, R, Apache Spark, and SQL.
Its most common use cases are cleaning, aggregating, transforming, and analyzing data.

Some notable features of tada:
* flexible constructor that supports most primitive data types
* seamlessly handles null data and type conversions
* well-suited to conventional IDE-based programming, but also supports exploratory notebook usage
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

First you write a test:
```
func TestDataPipeline(t *testing.T) {
  want := `name, mean_score
           jane doe, 9
           john doe, 6`


  df, err := tada.ReadCSV(strings.NewReader(data))
    ... handle err

  ret := DataPipeline(df)
  ok, diffs, err := ret.EqualsCSV(strings.NewReader(want))
    ... handle err
  if !ok {
    t.Errorf("DataPipeline(): got %v, want %v, has diffs: \n%v", ret, want, diffs)
  }
}
```

Then you write the data pipeline:
```
func DataPipeline(df *tada.DataFrame) *tada.DataFrame {
  err := df.HasCols("name", "score")
    ... handle err
  df.InPlace().DropNull()
  validScore := tada.FilterFn{Float64: func(v float64) bool { return v >= 0 && v <= 10 }}
	
  df.InPlace().Filter(map[string]tada.FilterFn{"score": validScore})
  df.InPlace().Sort(tada.Sorter{Name: "name", DType: tada.String})
  return df.GroupBy("name").Mean("score")
}
```
More [examples](https://godoc.org/github.com/ptiger10/tada#pkg-examples)

Extended [tutorial](tutorial.ipynb)


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

## Using with Jupyter notebooks
* Follow the instructions for installing [gophernotes](https://github.com/gopherdata/gophernotes), including jupyter.
* Install `tada` from anywhere using Go modules (this will install the latest version to `$GOPATH/pkg/mod/ptiger10/tada`, which is what gophernotes references to import 3rd party packages): `$ GO111MODULE=on go get -u github.com/ptiger10/tada`
* Run the same snippet whenever you want gophernotes to have access to the latest version of `tada`. 
* `$ jupyter notebook` (should launch a window in your default browser)
* From Home screen -> New -> Go

One of the biggest limitations of gophernotes is that it does not provide signature hinting. This is a [known issue](https://github.com/gopherdata/gophernotes/issues/173).

[Sample Notebook](tutorial.ipynb)

## Performance Tuning
* Modify a Series or DataFrame in place (without returning a new copy) by first calling `InPlace()`.
* If you expect to use a column as numeric, string, or time.Time values multiple times, `Cast()` it to `tada.Float64`, `tada.String`, or `tada.DateTime`, respectively.

## Inter-process communication (IPC)
* Apache Arrow
  * Read from existing Pandas dataframes using the Apache Arrow specification. 
  * Because the go/arrow library is still not v1.0, convenience functions and patterns are versioned in a [separate repo](https://github.com/ptiger10/tada-io).