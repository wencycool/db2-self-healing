package main

import (
	"fmt"
	"my/db/db2"
)

func main() {
	dbname := "sample"
	advis, err := db2.GetAdvisRaw(dbname, "db2inst1", "select * from ttt where varchar_col30='asdf' with ur")
	if err != nil {
		panic(err)
	}
	fmt.Println(advis.Improvement)
	for _, idx := range advis.AdvisIndexes {
		fmt.Println("Size:", idx.Size)
		fmt.Println(idx.Text)
	}
}
