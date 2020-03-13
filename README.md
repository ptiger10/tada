# tada
Package tada (TAble DAta) enables test-driven data pipelines in pure Go.

tada combines concepts from pandas (Python), spreadsheets, R, Apache Spark, and SQL.
Its most common use cases are cleaning, aggregating, transforming, and analyzing data.

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

## Basic Usage
### Selecting data
```
>>> df
  amount       date place
a      1 2020-01-01   foo
b      3 2020-01-03   bar

>>> df.Col("amount")
a 1
b 3
name: amount

>>> df.Cols("amount, place")
  amount place
a      1   foo
b      3   bar

>>> df.Subset([]int{0})
  amount       date place
a      1 2020-01-01   foo

>>> df.Subset(df.Index("b"))
  amount       date place
b      3 2020-01-03   bar

>>> df.Subset(df.IndexFrom("a", "b"))
  amount       date place
a      1 2020-01-01   foo
b      3 2020-01-03   bar
```

### Math
```
// with no argument, sums all numeric columns
>>> df.Sum()
amount 4
name: sum

// can also provide columns explicitly
>>> df.Sum("amount, place")
amount   4
place  nil
name: sum
```

### Filtering data
```
>>> df.Subset(
     df.Float("amount").GT(2))
  amount       date place
1      3 2020-01-03   bar

>>> df.Subset(
     df.DateTime("date").Before(time.Time(2020,2,1,0,0,0,0,time.UTC)))
  amount       date place
0      1 2020-01-01   foo

>>> df.Subset(
     df.Str("place").Contains("f"))
  amount       date place
0      1 2020-01-01   foo
```

### Applying functions
```
>>> df
  a b c
0 1 3 5
1 2 4 6

>>> df.Float().Apply(func(v float64) float64{return (v+1)*2})
  a  b  c
0 4  8 12
1 6 10 14

>>> df.Col("a").Float().Multiply(2)
0 2
1 4
name: a
```

### Setting data

```
// WithCol accepts a Series, slice, or scalar
>>> df.WithCol("a", df.Col("a").Float().Multiply(2))
  a b c
0 2 3 5
1 4 4 6

>>> df.WithCol("name", []string{"foo", "bar"})
  a b c name
0 1 3 5  foo
1 2 4 6  bar

>>> df.WithCol("constant", 7)
  a b c constant
0 1 3 5        7
1 2 4 6        7
```


### Aggregating data
```
>>> df
  type amount year
0  foo      1 2019
1  foo      3 2019
2  bar      5 2019
3  bar      7 2019
4  bar     10 2020
5  bar    nil 2020
```


**Group by**
```
>>> df.GroupBy("type").Mean("amount")
    mean
foo  2.5
bar  7
```

**Pivot table**
```
>>> df.Pivot("year", "type").Sum("amount")
     foo bar
2019   4  12
2020 nil  10
```

### Combining data
```
>>> s
foo  1
bar  2

>>> s2
foo  5
qux 10

>>> df
     year amount
foo  2019      1
qux  2019      2
quux 2019      3

>>> df2
           city   population
foo       cairo            9
corge  new york            8
waldo     paris            2 
```

**Lookup**
```
>>> s2.Lookup(s)
foo   5
bar nil

>>> s2.Lookup(df)
foo    5
qux   10
quux nil

>>> df.Lookup(s)
     year amount
foo  2019      1
bar   nil    nil

>>> df2.Lookup(df)
      city  population
foo  cairo           9
qux    nil         nil
quux   nil         nil
```

**Extend**
```
>>> s.Extend(s2)
foo  1
bar  2
foo  5
qux 10

>>> df.Extend(df)
     year amount
foo  2019      1
qux  2019      2
quux 2019      3
foo  2019      1
qux  2019      2
quux 2019      3
```

**Merge**
```
>>> s.Merge(s2)
foo  1 5


