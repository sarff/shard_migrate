package shard

import (
	"testing"

	"github.com/sarff/shard_migrate/internal/config"
)

func TestGetShardIndex(t *testing.T) {
	type args struct {
		conf  *config.Config
		value any
	}

	conf, err := config.LoadConfig()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		args args
		want int
	}{
		{"00", args{conf, "79281558867"}, 0},
		{"00_1", args{conf, "79264710153"}, 0},
		{"00_2", args{conf, "79212520969"}, 0},
		{"01", args{conf, "79137744540"}, 1},
		{"01_1", args{conf, "79003206446"}, 1},
		{"01_2", args{conf, "79531325879"}, 1},
		{"02", args{conf, "79211371406"}, 2},
		{"02_1", args{conf, "79307422680"}, 2},
		{"02_2", args{conf, "79166094626"}, 2},

		{"03", args{conf, "79537420918"}, 3},
		{"03_1", args{conf, "79854437414"}, 3},
		{"03_2", args{conf, "79205507582"}, 3},

		{"04", args{conf, "79283054572"}, 4},
		{"04_1", args{conf, "79254181801"}, 4},
		{"04_2", args{conf, "79030722062"}, 4},

		{"05", args{conf, "89156214746"}, 5},
		{"05_1", args{conf, "79266280099"}, 5},
		{"05_2", args{conf, "79232780820"}, 5},

		{"06", args{conf, "79115781098"}, 6},
		{"06_1", args{conf, "79853338023"}, 6},
		{"06_2", args{conf, "79062082023"}, 6},

		{"07", args{conf, "79232191605"}, 7},
		{"07_1", args{conf, "79135079119"}, 7},
		{"07_2", args{conf, "79248334592"}, 7},

		{"08", args{conf, "79119580160"}, 8},
		{"08_1", args{conf, "79507121072"}, 8},
		{"08_2", args{conf, "79229314410"}, 8},
		{"09", args{conf, "79029223667"}, 9},
		{"10", args{conf, ""}, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetShardIndex(tt.args.conf, tt.args.value); got != tt.want {
				t.Errorf("GetShardIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetShardPath(t *testing.T) {
	type args struct {
		index    int
		ShardDir string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetShardPath(tt.args.index, tt.args.ShardDir); got != tt.want {
				t.Errorf("GetShardPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_pad(t *testing.T) {
	type args struct {
		n int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pad(tt.args.n); got != tt.want {
				t.Errorf("pad() = %v, want %v", got, tt.want)
			}
		})
	}
}
