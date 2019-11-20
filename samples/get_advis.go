package main

import (
	"fmt"
	"my/db/db2"
	"os/exec"
	"time"
)

func main() {
	exec.Command("db2", "connect to sample")
	dbname := "sample"
	t1 := time.Now()
	advis, err := db2.GetAdvisRaw(dbname, "db2inst1", "select * from ttt where varchar_col30='asdf' with ur")
	if err != nil {
		panic(err)
	}
	fmt.Println(advis)
	fmt.Println(advis.Improvement)
	for _, idx := range advis.AdvisIndexes {
		fmt.Println("sdfadf")
		fmt.Println("Size:", idx.Size)
		fmt.Println(idx)
		fmt.Println(idx.Text)
	}
	fmt.Println(time.Now().Sub(t1).String())
}
