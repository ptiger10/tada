# tada
Predictable, flexible data handling and analysis in Go.

import "github.com/ptiger10/tada"

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









