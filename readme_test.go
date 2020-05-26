package tada_test

import (
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

func Test_sampleDataPipeline(t *testing.T) {
	data := `name, score
			joe doe,
			john doe, -100
			jane doe, 1000
            john doe, 6
			jane doe, 8
            john doe, 4
			jane doe, 10`

	wantRaw := `name, mean_score
			jane doe, 9
			john doe, 5`

	r := tada.NewCSVReader(strings.NewReader(data))
	r.TrimLeadingSpace = true
	df, err := r.Read()
	if err != nil {
		log.Fatal(err)
	}

	ret := sampleDataPipeline(df)
	got := tada.NewRecordWriter()
	got.IncludeLabels = true
	want := tada.NewCSVReader(strings.NewReader(wantRaw))
	want.TrimLeadingSpace = true
	eq, diffs, _ := ret.EqualRecords(got, want)
	if !eq {
		t.Errorf("sampleDataPipeline(): got %v, want %v, has diffs: \n%v", got.Records(), want.Records(), diffs)
	}
}

func Test_sampleDataPipelineTypedOutput(t *testing.T) {
	data := `name, score
			joe doe,
			john doe, -100
			jane doe, 1000
            john doe, 6
			jane doe, 8
            john doe, 4
			jane doe, 10`

	type output struct {
		Name      string  `json:"name"`
		MeanScore float64 `json:"mean_score"`
	}
	want := []output{
		{"jane doe", 9},
		{"john doe", 5},
	}

	r := tada.NewCSVReader(strings.NewReader(data))
	r.TrimLeadingSpace = true
	df, _ := r.Read()

	out := sampleDataPipeline(df)
	var got []output
	w := tada.NewStructWriter(&got)
	w.IncludeLabels = true
	err := out.WriteTo(w)
	if err != nil {
		log.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("sampleDataPipelineTypedOutput(): got %v, want %v", got, want)
	}
}

func Test_sampleDataPipelineTyped(t *testing.T) {
	type input struct {
		Name  string `json:"name"`
		Score int    `json:"score"`
	}

	type output struct {
		Name      string  `json:"name"`
		MeanScore float64 `json:"mean_score"`
	}

	data := []input{
		{"john doe", -100},
		{"jane doe", 1000},
		{"john doe", 6},
		{"jane doe", 8},
		{"john doe", 4},
		{"jane doe", 10},
	}

	want := []output{
		{"jane doe", 9},
		{"john doe", 5},
	}

	df, err := tada.NewStructReader(data).Read()
	if err != nil {
		log.Fatal(err)
	}
	out := sampleDataPipeline(df)

	var got []output
	w := tada.NewStructWriter(&got)
	w.IncludeLabels = true
	err = out.WriteTo(w)
	if err != nil {
		log.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("sampleDataPipelineTypedInput(): got %v, want %v", got, want)
	}
}
