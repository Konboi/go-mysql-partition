package partition

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestListPartitioner(t *testing.T) {
	list := NewListPartitioner(nil, "test", "event_id", PartitionTypeList)

	type Test struct {
		Title  string
		Input  []Partition
		Output string
		Do     func(...Partition) (Handler, error)
	}

	tests := []Test{
		Test{
			Title:  "create partition sql",
			Input:  []Partition{Partition{Name: "p1", Description: "1"}},
			Output: "ALTER TABLE test PARTITION BY LIST (event_id) (PARTITION p1 VALUES IN (1))",
			Do: func(partitions ...Partition) (Handler, error) {
				return list.PrepareCreates(partitions...)
			},
		},
		Test{
			Title:  "add partition sql",
			Input:  []Partition{Partition{Name: "p2", Description: "2, 3"}},
			Output: "ALTER TABLE test ADD PARTITION (PARTITION p2 VALUES IN (2, 3))",
			Do: func(partitions ...Partition) (Handler, error) {
				return list.PrepareAdds(partitions...)
			},
		},
		Test{
			Title:  "drop partition sql",
			Input:  []Partition{Partition{Name: "p1"}},
			Output: "ALTER TABLE test DROP PARTITION p1",
			Do: func(partitions ...Partition) (Handler, error) {
				return list.PrepareDrops(partitions...)
			},
		},
		Test{
			Title:  "truncate partition sql",
			Input:  []Partition{Partition{Name: "p1"}},
			Output: "ALTER TABLE test TRUNCATE PARTITION p1",
			Do: func(partitions ...Partition) (Handler, error) {
				return list.PrepareTruncates(partitions...)
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
}

func Test_list_buildPart(t *testing.T) {
	l := &List{}

	p := Partition{
		Name:        "p1122",
		Comment:     "test1122",
		Description: "1122",
	}
	expect := "PARTITION p1122 VALUES IN (1122) COMMENT = 'test1122'"

	result, err := l.buildPart(p)
	if err != nil {
		t.Fatal("error build part.", err.Error())
	}

	if diff := cmp.Diff(result, expect); diff != "" {
		t.Fatalf("error invalid result:%s", diff)
	}
}
