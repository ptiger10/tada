package tada

import (
	"encoding/csv"
	"log"
	"strings"
	"testing"
)

func Test_transformData(t *testing.T) {
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

	// this block is equivalent to ReadCSVFromString(data)
	r := strings.NewReader(data)
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true
	records, err := cr.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	df, _ := ReadCSV(records)

	ret := exampleTransformData(df)
	ok, diffs, err := ret.EqualsCSVFromString(want, true)
	if err != nil {
		log.Fatal(err)
	}
	if !ok {
		t.Errorf("TransformData(): got %v, want %v, has diffs: \n%v", ret, want, diffs)
	}
}

func exampleTransformData(df *DataFrame) *DataFrame {
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
