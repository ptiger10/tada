package tada

import (
	"reflect"
	"testing"

	"github.com/ptiger10/tablediff"
)

func TestPrettyDiff(t *testing.T) {
	type args struct {
		got  interface{}
		want interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		want1   *tablediff.Differences
		wantErr bool
	}{
		{"pass - same",
			args{
				[]testStruct{
					{"foo", 1},
					{"bar", 2},
				},
				[]testStruct{
					{"foo", 1},
					{"bar", 2},
				},
			},
			true,
			nil,
			false,
		},
		{"fail - bad got",
			args{
				"foo",
				[]testStruct{
					{"foo", 3},
					{"bar", 2},
				},
			},
			false,
			nil,
			true,
		},
		{"fail - bad want",
			args{
				[]testStruct{
					{"foo", 1},
					{"bar", 2},
				},
				"foo",
			},
			false,
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := PrettyDiff(tt.args.got, tt.args.want)
			if (err != nil) != tt.wantErr {
				t.Errorf("PrettyDiff() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("PrettyDiff() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("PrettyDiff() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
