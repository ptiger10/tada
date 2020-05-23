package tada_test

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"testing"

	"github.com/ptiger10/tada"
)

func sampleDataPipeline(df *tada.DataFrame) *tada.DataFrame {
	err := df.HasCols("name", "score")
	if err != nil {
		log.Fatal(err)
	}
	df.InPlace().DropNull()
	df.Cast(map[string]tada.DType{"score": tada.Float64})
	validScore := func(v interface{}) bool { return v.(float64) >= 0 && v.(float64) <= 10 }
	df.InPlace().Filter(map[string]tada.FilterFn{"score": validScore})
	df.InPlace().Sort(tada.Sorter{Name: "name", DType: tada.String})

	ret := df.GroupBy("name").Mean("score")
	if ret.Err() != nil {
		log.Fatal(ret.Err())
	}
	return ret
}

// func Test_sampleDataPipeline(t *testing.T) {
// 	data := `name, score
// 			joe doe,
// 			john doe, -100
// 			jane doe, 1000
//             john doe, 6
// 			jane doe, 8
//             john doe, 4
// 			jane doe, 10`

// 	want := `name, mean_score
// 			jane doe, 9
// 			john doe, 5`

// 	df, _ := tada.NewReader(strings.NewReader(data)).Read()

// 	ret := sampleDataPipeline(df)
// 	r := strings.NewReader(want)
// 	eq, diffs, _ := ret.EqualsCSV(true, r)
// 	if !eq {
// 		t.Errorf("sampleDataPipeline(): got %v, want %v, has diffs: \n%v", ret, want, diffs)
// 	}
// }

func Test_sampleDataPipelineTyped(t *testing.T) {
	data := `name, score
			joe doe,
			john doe, -100
			jane doe, 1000
            john doe, 6
			jane doe, 8
            john doe, 4
			jane doe, 10`

	type output struct {
		Name      []string  `tada:"name"`
		MeanScore []float64 `tada:"mean_score"`
	}
	want := output{
		Name:      []string{"jane doe", "john doe"},
		MeanScore: []float64{9, 5},
	}

	r := tada.NewCSVReader(strings.NewReader(data))
	r.TrimLeadingSpace = true
	df, _ := r.Read()
	fmt.Println(df)

	out := sampleDataPipeline(df)
	var got output
	w := tada.NewStructWriter(&got)
	out.WriteTo(w)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("sampleDataPipelineTyped(): got %v, want %v", got, want)
	}
}

func Test_sampleDataPipelineTypedInput(t *testing.T) {
	type input struct {
		Name    []string `tada:"name"`
		Score   []int    `tada:"score"`
		NullMap [][]bool `tada:"isNull"`
	}

	type output struct {
		Name      []string  `tada:"name"`
		MeanScore []float64 `tada:"mean_score"`
		NullMap   [][]bool  `tada:"isNull"`
	}

	data := input{
		Name:    []string{"joe doe", "john doe", "jane doe", "john doe", "jane doe", "john doe", "jane doe"},
		Score:   []int{0, -100, 1000, 6, 8, 4, 10},
		NullMap: [][]bool{{true, false, false, false, false, false, false}, {false, false, false, false, false, false, false}},
	}

	want := output{
		Name:      []string{"jane doe", "john doe"},
		MeanScore: []float64{9, 5},
		NullMap:   [][]bool{{false, false}, {false, false}},
	}

	df, err := tada.NewStructReader(data).Read()
	if err != nil {
		log.Fatal(err)
	}
	out := sampleDataPipeline(df)

	var got output

	w := tada.NewStructWriter(&got)
	out.WriteTo(w)
	if err != nil {
		log.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("sampleDataPipelineTypedInput(): got %v, want %v", got, want)
	}
}

func Test_sampleDataPipeline2(t *testing.T) {
	// data := `name, score
	// 		joe doe,
	// 		john doe, -100
	// 		jane doe, 1000
	//         john doe, 6
	// 		jane doe, 8
	//         john doe, 4
	// 		jane doe, 10`

	// df, _ := tada.NewReader(strings.NewReader(data)).Read()
	r := tada.NewRecordReader([][]string{{"foo", "bar"}, {"baz", "qux"}})
	r.ByColumn = false
	df, _ := r.Read()
	fmt.Println(df)

	// err := df.HasCols("name", "score")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// df.InPlace().DropNull()
	// df.Cast(map[string]tada.DType{"score": tada.Float64})
	// validScore := func(v interface{}) bool { return v.(float64) >= 0 && v.(float64) <= 10 }
	// df.InPlace().Filter(map[string]tada.FilterFn{"score": validScore})
	// df.InPlace().Sort(tada.Sorter{Name: "name", DType: tada.String})

	// ret := df.GroupBy("name").Mean("score")
	// if ret.Err() != nil {
	// 	log.Fatal(ret.Err())
	// }
	w := tada.NewRecordWriter()
	w.IncludeLabels = true
	df.WriteTo(w)
	fmt.Println(w.Records())
}
