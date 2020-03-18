package tada

import (
	"log"
	"testing"
)

func Test_TransformData(t *testing.T) {
	data := `name, score
            joe doe,
            john doe, 5
            jane doe, 8
            john doe, 7
			jane doe, 10`

	want := `name, mean_score
			jane doe, 9
			john doe, 6`

	df, err := ReadCSVFromString(data)
	if err != nil {
		log.Fatal(err)
	}
	ret := exampleTransformData(df)
	ok, diffs, err := ret.EqualsCSVFromString(want, false)
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
	df.InPlace().Sort(Sorter{Name: "name", DType: String})
	return df.GroupBy("name").Mean("score")
}
