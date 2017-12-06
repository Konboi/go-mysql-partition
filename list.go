package partition

import (
	"database/sql"
	"fmt"
	"strings"
)

type List struct{}

func NewListPartitioner(db *sql.DB, table, expresstion string) Partitioner {
	return &partitioner{
		table:         table,
		db:            db,
		expression:    expresstion,
		partitionType: PartitionTypeList,
		partBuilder:   &List{},
	}
}

func (l *List) buildPart(p Partition) (string, error) {
	if p.Description == "" {
		return "", fmt.Errorf("error no partition description is spcified")
	}

	part := fmt.Sprintf("PARTITION %s VALUES IN (%s)", p.Name, p.Description)
	if p.Comment != "" {
		part = part + fmt.Sprintf(" COMMENT = '%s'", strings.Replace(p.Comment, "'", "", -1))
	}

	return part, nil
}
