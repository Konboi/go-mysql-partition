# go-mysql-partition

Utility for MySQL partitioning. inspired by [p5-MySQL-Partition](https://github.com/Songmu/p5-MySQL-Partition)

## Useage

`_example/main.go`

```go
package main

import (
	"database/sql"
	"log"

	"github.com/Konboi/go-mysql-partition"
	"github.com/lestrrat/go-test-mysqld"
)

func main() {
	mysqld, err := mysqltest.NewMysqld(nil)
	if err != nil {
		log.Fatal("error new mysqld", err.Error())
	}
	defer mysqld.Stop()

	db, err := sql.Open("mysql", mysqld.Datasource("test", "", "", 0))
	if err != nil {
		log.Fatal("error open.", err.Error())
	}

	if _, err := db.Exec(`CREATE TABLE test (
      id BIGINT unsigned NOT NULL auto_increment,
      event_id INTEGER NOT NULL,
      PRIMARY KEY (id, event_id)
    )`); err != nil {
		log.Fatal("error exec sceham.", err.Error())
	}

	// verbosee print exec query
	list := partition.NewListPartitioner(db, "test", "event_id", partition.Verbose(true))

	partitioned, err := list.IsPartitioned()
	if err != nil {
		log.Fatal("error is partitioned.")
	}

	if !partitioned {
		log.Println("test table event_id not partitioned")
	}

	if err := list.Creates(partition.NewPartition("e00001", "1", "event_id = 1")); err != nil {
		log.Fatal("error add partition.", err.Error())
	}

	partitioned, err = list.IsPartitioned()
	if err != nil {
		log.Fatal("error is partitioned.")
	}

	if partitioned {
		log.Println("test table event_id is partitioned.")
	}

	event2Partition := partition.NewPartition("e00002", "2", "event_id = 2")
	event3and4Partition := partition.NewPartition("e00003", "3,4", "event_id = 3 and 4")

	if err := list.Adds(event2Partition, event3and4Partition); err != nil {
		log.Fatal("error add partition.", err.Error())
	}

	var show1, show2 string
	if err := db.QueryRow("show create table test").Scan(&show1, &show2); err != nil {
		log.Fatal("error scan table", err.Error())
	}
	log.Println(show1, show2)

}
```

### result

`go run _example/main.go`

```
200~2017/12/18 17:36:10 test table event_id not partitioned
Following SQL sttement to be executed.
ALTER TABLE test PARTITION BY LIST (event_id) (PARTITION e00001 VALUES IN (1) COMMENT = 'event_id = 1')
done.
2017/12/18 17:36:11 test table event_id is partitioned.
Following SQL sttement to be executed.
ALTER TABLE test ADD PARTITION (PARTITION e00002 VALUES IN (2) COMMENT = 'event_id = 2', PARTITION e00003 VALUES IN (3,4) COMMENT = 'event_id = 3 and 4')
done.
2017/12/18 17:36:11 test CREATE TABLE `test` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `event_id` int(11) NOT NULL,
   PRIMARY KEY (`id`,`event_id`)
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY LIST (event_id)
(PARTITION e00001 VALUES IN (1) COMMENT = 'event_id = 1' ENGINE = InnoDB,
 PARTITION e00002 VALUES IN (2) COMMENT = 'event_id = 2' ENGINE = InnoDB,
 PARTITION e00003 VALUES IN (3,4) COMMENT = 'event_id = 3 and 4' ENGINE = InnoDB) */
```
