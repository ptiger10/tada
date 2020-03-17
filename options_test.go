package tada

import (
	"reflect"
	"testing"
)

func TestSetOptionDefaultSeparator(t *testing.T) {
	type args struct {
		sep string
	}
	tests := []struct {
		name string
		args args
	}{
		{"pass", args{"||"}},
	}
	for _, tt := range tests {
		archive := optionLevelSeparator
		t.Run(tt.name, func(t *testing.T) {
			SetOptionDefaultSeparator(tt.args.sep)
		})

		if got := optionLevelSeparator; got != tt.args.sep {
			t.Errorf("SetOptionDefaultSeparator() -> %v, want %v", got, tt.args.sep)
		}
		optionLevelSeparator = archive
	}
}

func TestPrintOptionMaxRows(t *testing.T) {
	type args struct {
		n int
	}
	tests := []struct {
		name string
		args args
	}{
		{"pass", args{5}},
	}
	for _, tt := range tests {
		archive := optionMaxRows
		t.Run(tt.name, func(t *testing.T) {
			PrintOptionMaxRows(tt.args.n)
		})

		if got := optionMaxRows; got != tt.args.n {
			t.Errorf("PrintOptionMaxRows() -> %v, want %v", got, tt.args.n)
		}
		optionMaxRows = archive
	}
}

func TestPrintOptionMaxColumns(t *testing.T) {
	type args struct {
		n int
	}
	tests := []struct {
		name string
		args args
	}{
		{"pass", args{5}},
	}
	for _, tt := range tests {
		archive := optionMaxColumns
		t.Run(tt.name, func(t *testing.T) {
			PrintOptionMaxColumns(tt.args.n)
		})

		if got := optionMaxColumns; got != tt.args.n {
			t.Errorf("PrintOptionMaxColumns() -> %v, want %v", got, tt.args.n)
		}
		optionMaxColumns = archive
	}
}

func TestPrintOptionMergeRepeats(t *testing.T) {
	type args struct {
		set bool
	}
	tests := []struct {
		name string
		args args
	}{
		{"pass", args{false}},
	}
	for _, tt := range tests {
		cache := optionMergeRepeats
		t.Run(tt.name, func(t *testing.T) {
			PrintOptionMergeRepeats(tt.args.set)
		})

		if got := optionMergeRepeats; got != tt.args.set {
			t.Errorf("PrintOptionMergeRepeats() -> %v, want %v", got, tt.args.set)
		}
		optionMergeRepeats = cache
	}
}

func TestPrintOptionWrapLine(t *testing.T) {
	type args struct {
		set bool
	}
	tests := []struct {
		name string
		args args
	}{
		{"pass", args{true}},
	}
	for _, tt := range tests {
		cache := optionWrapLines
		t.Run(tt.name, func(t *testing.T) {
			PrintOptionWrapLines(tt.args.set)
		})

		if got := optionWrapLines; got != tt.args.set {
			t.Errorf("PrintOptionWrapLines() -> %v, want %v", got, tt.args.set)
		}
		optionWrapLines = cache
	}
}
func TestSetOptionAddTimeFormat(t *testing.T) {
	cache := make([]string, len(optionDateTimeFormats))
	copy(cache, optionDateTimeFormats)
	type args struct {
		format string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{"pass", args{"foo"}, append(cache, "foo")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetOptionAddTimeFormat(tt.args.format)
			if !reflect.DeepEqual(optionDateTimeFormats, tt.want) {
				t.Errorf("SetOptionAddTimeFormat() -> %v, want %v", optionDateTimeFormats, tt.want)
			}
		})
	}
	optionDateTimeFormats = cache
}
