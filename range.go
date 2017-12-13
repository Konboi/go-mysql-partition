package partition

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

// Range is range partition part builder
type Range struct {
	table                 string
	catchAllPartitionName string
}

var (
	numberRegexp  *regexp.Regexp
	bracketRegexp *regexp.Regexp
)

func init() {
	numberRegexp = regexp.MustCompile(`^[0-9]+$`)
	bracketRegexp = regexp.MustCompile(`\(`)
}

// NewRangePartitioner is XXX
func NewRangePartitioner(db *sql.DB, table, expresstion string, options ...Option) Partitioner {
	p := &partitioner{
		table:         table,
		db:            db,
		expression:    expresstion,
		partitionType: PartitionTypeRange,
		partBuilder: &Range{
			table: table,
		},
	}

	for _, option := range options {
		option(p)
	}

	return p
}

func (r *Range) buildPart(p Partition) (string, error) {
	if p.Description == "" {
		return "", fmt.Errorf("error no partition description is spcified")
	}

	if !numberRegexp.MatchString(p.Description) && p.Description != CatchAllPartitionValue && !bracketRegexp.MatchString(p.Description) {
		p.Description = fmt.Sprintf("'%s'", p.Description)
	}

	part := fmt.Sprintf("PARTITION %s VALUES LESS THAN (%s)", p.Name, p.Description)
	if p.Comment != "" {
		part = part + fmt.Sprintf(" COMMENT = '%s'", strings.Replace(p.Comment, "'", "", -1))
	}

	return part, nil
}

func (r *Range) buildCatchAllPart() (string, error) {
	if r.catchAllPartitionName == "" {
		return "", fmt.Errorf("catch_all_partition_name isn't specified")
	}

	part, err := r.buildPart(Partition{Name: r.catchAllPartitionName, Description: "MAXVALUE"})
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("ALTER TABLE %s ADD PARTITION (%s)", r.table, part), nil
}
