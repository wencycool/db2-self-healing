package main

import (
	"fmt"
	"my/db/db2"
)

func main() {
	dblist, err := db2.GetCurDatabases()
	if err != nil {
		panic(err)
	}
	for _, db := range dblist {
		fmt.Println(db.DbAlias, db.Directory)
	}

}
