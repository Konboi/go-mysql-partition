package partition

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestRangePartitioner(t *testing.T) {
	type Test struct {
		Title  string
		Input  []Partition
		Output string
		Do     func(...Partition) (Handler, error)
	}

	r := NewRangePartitioner(nil, "test2", "created_at", PartitionType("range columns"))
	tests := []Test{
		Test{
			Title: "create partition",
			Input: []Partition{Partition{
				Name:        "p20100101",
				Description: "2010-01-01",
			}},
			Output: "ALTER TABLE test2 PARTITION BY RANGE COLUMNS (created_at) (PARTITION p20100101 VALUES LESS THAN ('2010-01-01'))",
			Do: func(partitions ...Partition) (Handler, error) {
				return r.PrepareCreates(partitions...)
			},
		},
		Test{
			Title: "add partition",
			Input: []Partition{
				Partition{
					Name:        "p20110101",
					Description: "2011-01-01",
				},
				Partition{
					Name:        "p20120101",
					Description: "2012-01-01",
				},
			},
			Output: "ALTER TABLE test2 ADD PARTITION (PARTITION p20110101 VALUES LESS THAN ('2011-01-01'), PARTITION p20120101 VALUES LESS THAN ('2012-01-01'))",
			Do: func(partitions ...Partition) (Handler, error) {
				return r.PrepareAdds(partitions...)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Title, func(t *testing.T) {
			h, err := test.Do(test.Input...)
			if err != nil {
				t.Fatal("error exec.", err.Error())
			}

			if diff := cmp.Diff(h.Statement(), test.Output); diff != "" {
				t.Fatalf("error invalid result:%s", diff)
			}
		})
	}

	t.Run("catch all", func(t *testing.T) {
		p := Partition{
			Name:        "p20100101",
			Description: "TO_DAYS('2010-01-01')",
		}
		expect := "ALTER TABLE test3 PARTITION BY RANGE (TO_DAYS(created_at)) (PARTITION p20100101 VALUES LESS THAN (TO_DAYS('2010-01-01')), PARTITION pmax VALUES LESS THAN (MAXVALUE))"
		r := NewRangePartitioner(nil, "test3", "TO_DAYS(created_at)", CatchAllPartitionName("pmax"))
		h, err := r.PrepareCreates(p)
		if err != nil {
			t.Fatal("error prepare creates.", err.Error())
		}

		if diff := cmp.Diff(h.Statement(), expect); diff != "" {
			t.Fatalf("error invalid result. %s", diff)
		}
	})
}

func Test_range_buildPart(t *testing.T) {
	r := &Range{}

	p := Partition{
		Name:        "p111",
		Comment:     "test111",
		Description: "111",
	}
	expect := "PARTITION p111 VALUES LESS THAN (111) COMMENT = 'test111'"

	result, err := r.buildPart(p)
	if err != nil {
		t.Fatal("error build part.", err.Error())
	}

	if diff := cmp.Diff(result, expect); diff != "" {
		t.Fatalf("error invalid result:%s", diff)
	}
}

func Test_range_buildCatchAllPart(t *testing.T) {
	r := &Range{
		table: "test3",
		catchAllPartitionName: "pmax",
	}

	expect := "ALTER TABLE test3 ADD PARTITION (PARTITION pmax VALUES LESS THAN (MAXVALUE))"

	result, err := r.buildCatchAllPart()
	if err != nil {
		t.Fatal("error build catcch all part.", err.Error())
	}

	if diff := cmp.Diff(result, expect); diff != "" {
		t.Fatalf("error invalid result:%s", diff)
	}
}
