# tada
Package tada (TAble DAta) enables test-driven data pipelines in pure Go.

tada combines concepts from pandas, spreadsheets, R, Apache Spark, and SQL.
Its most common use cases are cleaning, aggregating, transforming, and analyzing data.

Some notable features of tada:
* flexible constructor that supports most primitive data types
* seamlessly handles null data and type conversions
* well-suited to conventional IDE-based programming, but also supports exploratory notebook usage
* advanced filtering, grouping, sorting, and pivoting
* multi-level labels and columns
* complete test coverage
* comparable to pandas [performance](comparison_summary.txt) on key operations

The key data types are Series, DataFrames, and groupings of each.
A Series is analogous to one column of a spreadsheet, and a DataFrame is analogous to a whole spreadsheet.
Printing either data type will render an ASCII table.

Both Series and DataFrames have one or more "label levels".
On printing, these appear as the leftmost columns in a table, and typically have values that help identify ("label") specific rows.
They are analogous to the "index" concept in pandas.

For more detail and implementation notes, see [this doc](https://docs.google.com/document/d/18DvZzd6Tg6Bz0SX0fY2SrXOjE8d9xDhU6bDEnaIc_rM/edit?usp=sharing).


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
func Test_TransformData(t *testing.T) {
  want := `name, mean_score
           jane doe, 9
           john doe, 6`

  df, err := tada.ReadCSVFromString(records)
    ... handle err

  ret := TransformData(df)
  ok, diffs, err := ret.EqualsCSVFromString(want)
  ... handle err
  if !ok {
    t.Errorf("TransformData(): got %v, want %v, has diffs: \n%v", ret, want, diffs)
  }
}
```

Then you write the transformation steps:
```
func TransformData(df *tada.DataFrame) *tada.DataFrame {
  err := df.HasCols("name", "score")
    ... handle err
  df.InPlace().DropNull()
  validScore := tada.FilterFn{Float64: func(v float64) bool { return v >= 0 && v <= 10 }}
	df.InPlace().Filter(map[string]tada.FilterFn{"score": validScore})
  df.InPlace().Sort(tada.Sorter{Name: "name", DType: tada.String})
  return df.GroupBy("name").Mean("score")
}
```

Extended [tutorial](tutorial.ipynb)


## Basic usage
### Constructor:
#### Series
`s := tada.Series([]float{1,2,3})`
##### with one level of labels
`s := tada.Series([]float{1,2,3}, []string{"foo", "bar", "baz"})`
#### DataFrame
`df := tada.DataFrame([]interface{}{[]string{"foo"}, []float{2}})`

### Reading from:
#### CSV file
`df := tada.ImportCSV("foo.csv")`
#### CSV data as nested slice
`df := tada.ReadCSV([][]string{{"foo", "bar"}, {"baz", "qux}})`
#### Nested interface
`df := tada.ReadInterface([][]interface{}{[]float64{1, 2, 3}})`
#### Structs
`df := tada.ReadStruct([]ExampleStruct{{n: 1}, {n: 2}})`
#### gonum.Matrix
`df := tada.ReadMatrix(mat.NewDense(2, 1, []float64{1, 2}))`

### With options:
#### Designating the first row as column headers
`df := tada.ImportCSV("foo.csv", tada.ReadOptionHeaders(1))`
#### Designating the first two columns as label levels
`df := tada.ImportCSV("foo.csv", tada.ReadOptionLabels(2))`
#### Using a custom comma delimiter
`df := tada.ImportCSV("foo.csv", tada.ReadOptionDelimiter('|'))`
#### Using columns as the major dimension
`df := tada.ImportCSV("foo.csv", tada.ReadOptionSwitchDims())`

## Using with Jupyter notebooks
* Follow the instructions for installing [gophernotes](https://github.com/gopherdata/gophernotes), including jupyter.
* Install tada globally: `$ go get -u github.com/ptiger10/tada`
* `$ jupyter notebook` (should launch a window in your default browser)
* From Home screen -> New -> Go

One of the biggest limitations of gophernotes is that it does not provide signature hinting. This is a [known issue](https://github.com/gopherdata/gophernotes/issues/173).

[Sample Notebook](tutorial.ipynb)



