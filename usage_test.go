package tada

import (
	"log"
	"strings"
	"testing"
)

func Test_sampleDataPipeline(t *testing.T) {
	data := `name, score
			joe doe,
			john doe, -100
			jane doe, 1000
            john doe, 6
			jane doe, 8
            john doe, 4
			jane doe, 10`

	want := `name, mean_score
			jane doe, 9
			john doe, 5`

	df, _ := ReadCSV(strings.NewReader(data))

	ret := sampleDataPipeline(df)
	eq, diffs, _ := ret.EqualsCSV(true, strings.NewReader(want))
	if !eq {
		t.Errorf("sampleDataPipeline(): got %v, want %v, has diffs: \n%v", ret, want, diffs)
	}
}

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
