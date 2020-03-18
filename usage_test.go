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
						 john doe, 6
						 jane doe, 9`

	ret := exampleTransformData(data)
	ok, diffs, err := ret.EqualsCSVFromString(want, false)
	if err != nil {
		log.Fatal(err)
	}
	if !ok {
		t.Errorf("TransformData(): got %v, want %v, has diffs: \n%v", ret, want, diffs)
	}
}

func exampleTransformData(data string) *DataFrame {
	df, err := ReadCSVFromString(data)
	if err != nil {
		log.Fatal(err)
	}
	err = df.HasCols("name", "score")
	if err != nil {
		log.Fatal(err)
	}
	df.InPlace().DropNull()
	return df.GroupBy("name").Mean("score")
}
