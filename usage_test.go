package tada

// func TestUsage(t *testing.T) {
// 	df := NewDataFrame(
// 		[]interface{}{[]float64{1, 2, 3}, []float64{4, 5, 6}}, []string{"foo", "foo", "baz"}).
// 		SetCols([]string{"qux", "quux"})
// 	g := df.GroupBy().Sum()
// 	fmt.Print(g.values[1])
// }

// func TestMockCSV(t *testing.T) {
// 	c := [][]string{{"qux", "corge", "waldo"},
// 		{"1", "dog", "2/15/20"},
// 		{"1", "dog", "2/15/20"}}
// 	var b strings.Builder
// 	WriteMockCSV(c, &b, nil)
// }

// func TestAlign(t *testing.T) {
// 	df := NewDataFrame([]interface{}{[]float64{1, 2, 6}, []int{10, 20, 30}}, []string{"foo", "bar", "foo"}).SetCols([]string{"qux", "quux"})
// 	fmt.Println(df.GroupBy().Align("qux").Min())
// }
