# tada
Package tada (TAble DAta) enables test-driven data pipelines in pure Go.

tada combines concepts from pandas (Python), spreadsheets, R, Apache Spark, and SQL.
Its most common use cases are cleaning, aggregating, transforming, and analyzing data.


## Example
You start with a CSV. Like most real-world data, it is messy (this one is missing a score in the first row):
```
var data = `name, score
            joe doe,
            john doe, 5
            jane doe, 8
            john doe, 7
            jane doe, 10`
```
You want to write a validated automation that discards null data, groups by the `name` column, and returns the mean of the groups. 

First you write a test:
```
func Test_TransformData(t *testing.T) {
  want := `name, mean
           jane doe, 9
           john doe, 6`

  ret := TransformData(data)
  ok, diffs, err := ret.EqualsCSVFromString(want)
  ... handle err
  if !ok {
    t.Errorf("TransformData(): got %v, want %v, has diffs: \n%v", ret, want, diffs)
  }
}
```

Then you write the transformation steps:
```
func TransformData(data string) *tada.DataFrame {
  df, err := tada.ReadCSVFromString(data)
    ... handle err
  err = df.HasCols("name", "score")
    ... handle err
  df.InPlace().DropNull()
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
#### CSV data as string
`df := tada.ReadCSVFromString("foo, bar\n baz, qux\n")`
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



## Why should I use tada instead of...?

* pandas
  * Because it is written in Go, tada enjoys all the benefits of the Go ecosystem: 
**type safety**, **transparent error handling**, **intuitive datetimes**,
and first-class tooling for **test-driven, IDE-centric development**. 
  * tada's API was designed to be less flexible but **more predictable** than pandas.
For example, some pandas functions return a Series under some conditions, but a DataFrame other times.
When you have less uncertainty about what an API will accept and return, you have one less reason to lean on an exploratory notebook, encouraging you to build your data pipeline in your typical IDE workflow.
  * tada is also **significantly smaller** (<1MB unpacked) than pandas (~230MB, including dependencies),
which becomes relevant when storing your program on a web server or uploading it as a serverless function.
  * However, pandas is faster and more fully featured, with the benefit of 1,500+ contributors developing it for more than decade.
That said, tada is already in the same ballpark for [performance](comparison_summary.txt) on several key operations.
  * pandas also benefits from adjacent libraries in the Python ecosystem including plotting libraries (e.g., matplotlib, Seaborn) and native support in Jupyter notebooks.
tada has no plotting features but can be used with limited-functionality Jupyter notebooks by following [these steps](#using-with-jupyter-notebooks).  

* spreadsheets 
  * With any of these open-source alternatives, you can easily build **reusable, scalable data automations**. 
With spreadsheets, you must either repeat the same actions over and over again - with high margin for error - or wrestle with confusing and brittle macros.
  * However, spreadsheets are intuitive to non-technical users, while open-sources alternatives all do require knowledge of programming. 

* R: 
  * Unlike R, which has a steep learning curve due to its idiosyncratic syntax 
and is custom-built for statistical computing and graphics,
general-purpose languages like Python and Go are **easier to learn** and **better suited to adjacent tasks** 
relevant to many analytics projects, such as concurrent web scraping or publishing data via a web server. 
  * Go also has first-class tooling for **test-driven, IDE-centric development**, which are not as prominent in R development.
  * However, R does allow more specialized analysis once you know how to use it.

* Apache Spark: 
  * Libraries like tada or pandas are **much easier to use**. 
  You do not need to configure a cluster to run them. Just import the library into your project and start calling the API.
  * However, when using a library you are constrained to the memory on your computer, and so cannot handle *really* big data as well Spark.

* Other Go implementations of DataFrames and Series: tada is **more fully featured**.
Notable advanced features include:
  * flexible constructors (accepts almost all Go primitive types, plus time.Time for time series analysis)
  * grouping, pivoting, sorting, and filtering
  * multi-level labels and columns

## Using with Jupyter notebooks
* Follow the instructions for installing [gophernotes](https://github.com/gopherdata/gophernotes), including jupyter.
* Install tada globally: `$ go get -u github.com/ptiger10/tada`
* `$ jupyter notebook` (should launch a window in your default browser)
* From Home screen -> New -> Go

One of the biggest limitations of gophernotes is that it does not provide signature hinting. This is a [known issue](https://github.com/gopherdata/gophernotes/issues/173).

[Sample Notebook](tutorial.ipynb)



