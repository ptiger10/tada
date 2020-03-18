package tada

import (
	"reflect"
	"testing"
	"time"
)

func Test_floatValueContainer_Less(t *testing.T) {
	type fields struct {
		slice  []float64
		isNull []bool
		index  []int
	}
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{"pass", fields{
			[]float64{1, 2, 3}, []bool{false, false, false}, []int{0, 1, 2}},
			args{0, 1},
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := floatValueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				index:  tt.fields.index,
			}
			if got := vc.Less(tt.args.i, tt.args.j); got != tt.want {
				t.Errorf("floatValueContainer.Less() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_floatValueContainer_Len(t *testing.T) {
	type fields struct {
		slice  []float64
		isNull []bool
		index  []int
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{"pass", fields{
			[]float64{1, 2, 3}, []bool{false, false, false}, []int{0, 1, 2}},
			3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := floatValueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				index:  tt.fields.index,
			}
			if got := vc.Len(); got != tt.want {
				t.Errorf("floatValueContainer.Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_floatValueContainer_Swap(t *testing.T) {
	type fields struct {
		slice  []float64
		isNull []bool
		index  []int
	}
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   floatValueContainer
	}{
		{"pass", fields{
			[]float64{1, 2, 3}, []bool{false, true, false}, []int{0, 1, 2}},
			args{0, 1},
			floatValueContainer{
				slice: []float64{2, 1, 3}, isNull: []bool{true, false, false}, index: []int{1, 0, 2}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := floatValueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				index:  tt.fields.index,
			}
			vc.Swap(tt.args.i, tt.args.j)
			if !reflect.DeepEqual(vc, tt.want) {
				t.Errorf("floatValueContainer.Swap() -> %v, want %v", vc, tt.want)
			}
		})
	}
}

func Test_stringValueContainer_Less(t *testing.T) {
	type fields struct {
		slice  []string
		isNull []bool
		index  []int
	}
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{"pass", fields{
			[]string{"bar", "foo", "baz"}, []bool{false, false, false}, []int{0, 1, 2}},
			args{0, 1},
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := stringValueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				index:  tt.fields.index,
			}
			if got := vc.Less(tt.args.i, tt.args.j); got != tt.want {
				t.Errorf("stringValueContainer.Less() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_stringValueContainer_Len(t *testing.T) {
	type fields struct {
		slice  []string
		isNull []bool
		index  []int
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{"pass", fields{
			[]string{"bar", "foo", "baz"}, []bool{false, false, false}, []int{0, 1, 2}},
			3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := stringValueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				index:  tt.fields.index,
			}
			if got := vc.Len(); got != tt.want {
				t.Errorf("stringValueContainer.Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_stringValueContainer_Swap(t *testing.T) {
	type fields struct {
		slice  []string
		isNull []bool
		index  []int
	}
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   stringValueContainer
	}{
		{"pass", fields{
			[]string{"bar", "foo", "baz"}, []bool{false, true, false}, []int{0, 1, 2}},
			args{0, 1},
			stringValueContainer{
				slice: []string{"foo", "bar", "baz"}, isNull: []bool{true, false, false}, index: []int{1, 0, 2}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := stringValueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				index:  tt.fields.index,
			}
			vc.Swap(tt.args.i, tt.args.j)
			if !reflect.DeepEqual(vc, tt.want) {
				t.Errorf("stringValueContainer.Swap() -> %v, want %v", vc, tt.want)
			}
		})
	}
}

func Test_dateTimeValueContainer_Less(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		slice  []time.Time
		isNull []bool
		index  []int
	}
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{"pass", fields{
			[]time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2)}, []bool{false, false, false}, []int{0, 1, 2}},
			args{0, 1},
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := dateTimeValueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				index:  tt.fields.index,
			}
			if got := vc.Less(tt.args.i, tt.args.j); got != tt.want {
				t.Errorf("dateTimeValueContainer.Less() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dateTimeValueContainer_Len(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		slice  []time.Time
		isNull []bool
		index  []int
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{"pass", fields{
			[]time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2)}, []bool{false, false, false}, []int{0, 1, 2}},
			3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := dateTimeValueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				index:  tt.fields.index,
			}
			if got := vc.Len(); got != tt.want {
				t.Errorf("dateTimeValueContainer.Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dateTimeValueContainer_Swap(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		slice  []time.Time
		isNull []bool
		index  []int
	}
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   dateTimeValueContainer
	}{
		{"pass", fields{
			[]time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2)}, []bool{false, true, false}, []int{0, 1, 2}},
			args{0, 1},
			dateTimeValueContainer{
				slice: []time.Time{d.AddDate(0, 0, 1), d, d.AddDate(0, 0, 2)}, isNull: []bool{true, false, false}, index: []int{1, 0, 2}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := dateTimeValueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				index:  tt.fields.index,
			}
			vc.Swap(tt.args.i, tt.args.j)
			if !reflect.DeepEqual(vc, tt.want) {
				t.Errorf("dateTimeValueContainer.Swap() -> %v, want %v", vc, tt.want)
			}
		})
	}
}

func Test_convertStringToFloat(t *testing.T) {
	type args struct {
		val          string
		originalBool bool
	}
	tests := []struct {
		name  string
		args  args
		want  float64
		want1 bool
	}{
		{"null", args{"foo", false}, 0, true},
		{"not null - float", args{"3.5", false}, 3.5, false},
		{"not null - int", args{"3", false}, 3, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := convertStringToFloat(tt.args.val, tt.args.originalBool)
			if got != tt.want {
				t.Errorf("convertStringToFloat() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("convertStringToFloat() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_convertBoolToFloat(t *testing.T) {
	type args struct {
		val bool
	}
	tests := []struct {
		name string
		args args
		want float64
	}{
		{"pass", args{true}, 1},
		{"pass", args{false}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertBoolToFloat(tt.args.val); got != tt.want {
				t.Errorf("convertBoolToFloat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_float64(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	tests := []struct {
		name   string
		fields fields
		want   floatValueContainer
	}{
		{"[]float64", fields{slice: []float64{1}, isNull: []bool{false}},
			floatValueContainer{slice: []float64{1}, isNull: []bool{false}}},
		{"[]string", fields{slice: []string{"", "foo", "3.5"}, isNull: []bool{true, false, false}},
			floatValueContainer{slice: []float64{0, 0, 3.5}, isNull: []bool{true, true, false}}},
		{"[][]byte", fields{slice: [][]byte{[]byte(""), []byte("foo"), []byte("3.5")}, isNull: []bool{true, false, false}},
			floatValueContainer{slice: []float64{0, 0, 3.5}, isNull: []bool{true, true, false}}},
		{"[]time.Time", fields{slice: []time.Time{{}, d}, isNull: []bool{true, false}},
			floatValueContainer{slice: []float64{0, 0}, isNull: []bool{true, true}}},
		{"[]bool", fields{slice: []bool{false, true, false}, isNull: []bool{false, false, true}},
			floatValueContainer{slice: []float64{0, 1, 0}, isNull: []bool{false, false, true}}},
		{"[]interface", fields{slice: []interface{}{"3.5", float64(1), int(1), uint(1), d, false}, isNull: []bool{false, false, false, false, false, false}},
			floatValueContainer{slice: []float64{3.5, 1, 1, 1, 0, 0}, isNull: []bool{false, false, false, false, true, false}}},
		{"[]int", fields{slice: []int{1}, isNull: []bool{false}},
			floatValueContainer{slice: []float64{1}, isNull: []bool{false}}},
		{"[][]float64", fields{slice: [][]float64{{1}}, isNull: []bool{false}},
			floatValueContainer{slice: []float64{0}, isNull: []bool{true}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			if got := vc.float64(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.float64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_string(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	tests := []struct {
		name      string
		fields    fields
		want      stringValueContainer
		wantCache []string
	}{
		{"[]float64", fields{slice: []float64{1}, isNull: []bool{false}},
			stringValueContainer{slice: []string{"1"}, isNull: []bool{false}},
			[]string{"1"},
		},
		{"[]string", fields{slice: []string{"", "foo", "3.5"}, isNull: []bool{true, false, false}},
			stringValueContainer{slice: []string{"", "foo", "3.5"}, isNull: []bool{true, false, false}},
			[]string{"", "foo", "3.5"},
		},
		{"[]time.Time", fields{slice: []time.Time{{}, d}, isNull: []bool{true, false}},
			stringValueContainer{slice: []string{"0001-01-01T00:00:00Z", "2020-01-01T00:00:00Z"}, isNull: []bool{true, false}},
			[]string{"0001-01-01T00:00:00Z", "2020-01-01T00:00:00Z"},
		},
		{"[]bool", fields{slice: []bool{false, true, false}, isNull: []bool{false, false, true}},
			stringValueContainer{slice: []string{"false", "true", "false"}, isNull: []bool{false, false, true}},
			[]string{"false", "true", "false"},
		},
		{"[]interface", fields{slice: []interface{}{"3.5", float64(1), int(1), uint(1), d, false}, isNull: []bool{false, false, false, false, false, false}},
			stringValueContainer{slice: []string{"3.5", "1", "1", "1", "2020-01-01T00:00:00Z", "false"}, isNull: []bool{false, false, false, false, false, false}},
			[]string{"3.5", "1", "1", "1", "2020-01-01T00:00:00Z", "false"},
		},
		{"[]int", fields{slice: []int{1}, isNull: []bool{false}},
			stringValueContainer{slice: []string{"1"}, isNull: []bool{false}},
			[]string{"1"},
		},
		{"[][]byte", fields{slice: [][]byte{{100, 100}, {105, 105}}, isNull: []bool{false, false}},
			stringValueContainer{slice: []string{"dd", "ii"}, isNull: []bool{false, false}},
			[]string{"dd", "ii"},
		},
		{"[][]string", fields{slice: [][]string{{"foo", "bar"}, {""}}, isNull: []bool{false, true}},
			stringValueContainer{slice: []string{"[foo bar]", "[]"}, isNull: []bool{false, true}},
			[]string{"[foo bar]", "[]"},
		},
		{"[][]float64", fields{slice: [][]float64{{1, 2}, {0}}, isNull: []bool{false, true}},
			stringValueContainer{slice: []string{"[1 2]", "[0]"}, isNull: []bool{false, true}},
			[]string{"[1 2]", "[0]"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			if got := vc.string(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.str() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(vc.cache, tt.wantCache) {
				t.Errorf("valueContainer.str() cache -> %v, want %v", vc.cache, tt.wantCache)
			}
		})
	}
}

func Test_convertStringToDateTime(t *testing.T) {
	type args struct {
		val string
	}
	tests := []struct {
		name  string
		args  args
		want  time.Time
		want1 bool
	}{
		{"YYYY-MM-DD", args{"2020-02-01"}, time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC), false},
		{"MM-DD-YYYY", args{"02-01-2020"}, time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC), false},
		{"MM/DD/YYYY", args{"02/01/2020"}, time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC), false},
		{"M/D/YYYY", args{"2/1/2020"}, time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC), false},
		{"M/D/YY", args{"2/1/20"}, time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC), false},
		{"not null", args{"2020-02-01 00:00:00 +0000 UTC"}, time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC), false},
		{"RFC3339", args{"2020-02-01T00:00:00Z"}, time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC), false},
		{"RFC3339Nano", args{"2020-02-01T00:00:00.0000000000Z"}, time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC), false},
		{"RFC822", args{"01 Feb 20 00:00 UTC"}, time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC), false},
		{"null", args{"foo"}, time.Time{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := convertStringToDateTime(tt.args.val)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertStringToDateTime() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("convertStringToDateTime() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_valueContainer_dateTime(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	tests := []struct {
		name   string
		fields fields
		want   dateTimeValueContainer
	}{
		{"[]float64", fields{slice: []float64{1}, isNull: []bool{false}},
			dateTimeValueContainer{slice: []time.Time{{}}, isNull: []bool{true}}},
		{"[]string", fields{slice: []string{"", "1/1/2020", "foo"}, isNull: []bool{true, false, true}},
			dateTimeValueContainer{slice: []time.Time{{}, d, {}}, isNull: []bool{true, false, true}}},
		{"[][]byte", fields{slice: [][]byte{[]byte(""), []byte("1/1/2020"), []byte("foo")}, isNull: []bool{true, false, true}},
			dateTimeValueContainer{slice: []time.Time{{}, d, {}}, isNull: []bool{true, false, true}}},
		{"[]time.Time", fields{slice: []time.Time{{}, d}, isNull: []bool{true, false}},
			dateTimeValueContainer{slice: []time.Time{{}, d}, isNull: []bool{true, false}}},
		{"[]bool", fields{slice: []bool{false, true, false}, isNull: []bool{false, false, true}},
			dateTimeValueContainer{slice: []time.Time{{}, {}, {}}, isNull: []bool{true, true, true}}},
		{"[]interface", fields{slice: []interface{}{"foo", float64(1), int(1), uint(1), d, false}, isNull: []bool{false, false, false, false, false, false}},
			dateTimeValueContainer{slice: []time.Time{{}, {}, {}, {}, d, {}}, isNull: []bool{true, true, true, true, false, true}}},
		{"[]int", fields{slice: []int{1}, isNull: []bool{false}},
			dateTimeValueContainer{slice: []time.Time{{}}, isNull: []bool{true}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			if got := vc.dateTime(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.dateTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_cast(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	type args struct {
		dtype DType
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *valueContainer
	}{
		{"float64 to float64", fields{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			args{Float64}, &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
		{"int to float64", fields{slice: []int{1}, isNull: []bool{false}, name: "foo"},
			args{Float64}, &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
		{"string to string", fields{slice: []string{"foo"}, isNull: []bool{false}, name: "foo"},
			args{String}, &valueContainer{
				cache: []string{"foo"},
				slice: []string{"foo"}, isNull: []bool{false}, name: "foo"}},
		{"int to string - set cache", fields{slice: []int{1}, isNull: []bool{false}, name: "foo"},
			args{String}, &valueContainer{slice: []string{"1"}, isNull: []bool{false}, name: "foo",
				cache: []string{"1"}}},
		{"datetime to datetime", fields{slice: []time.Time{d}, isNull: []bool{false}, name: "foo"},
			args{DateTime}, &valueContainer{slice: []time.Time{d}, isNull: []bool{false}, name: "foo"}},
		{"int to datetime", fields{slice: []int{1}, isNull: []bool{false}, name: "foo"},
			args{DateTime}, &valueContainer{slice: []time.Time{{}}, isNull: []bool{true}, name: "foo"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			vc.cast(tt.args.dtype)
			if !reflect.DeepEqual(vc, tt.want) {
				t.Errorf("vc.cast() -> %v, want %v", vc, tt.want)
			}
		})
	}
}

func Test_valueContainer_setCache(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		slice  interface{}
		isNull []bool
		cache  []string
		name   string
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{"byte", fields{slice: [][]byte{[]byte("foo")}}, []string{"foo"}},
		{"string", fields{slice: []string{"foo"}}, []string{"foo"}},
		{"float64", fields{slice: []float64{1}}, []string{"1"}},
		{"int", fields{slice: []int{1}}, []string{"1"}},
		{"datetime", fields{slice: []time.Time{d}}, []string{d.String()}},
		{"default", fields{slice: []int64{1}}, []string{"1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				cache:  tt.fields.cache,
				name:   tt.fields.name,
			}
			vc.setCache()
			if !reflect.DeepEqual(vc.cache, tt.want) {
				t.Errorf("vc.setCache() .cache ->  %v, want %v", vc.cache, tt.want)
			}
		})
	}
}
