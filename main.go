package main

import (
	"fmt"
	"log"
	"my/db/db2"
	"time"
)

func main() {
	a, b, c, d, e, err := db2.CollectData("sample", time.Duration(time.Second*10))
	if err != nil {
		log.Fatal(err)
	}
	for _, i := range a {
		fmt.Println(i)
	}
	for _, i := range b {
		fmt.Println(i)
	}
	for _, i := range c {
		fmt.Println(i)
	}
	for _, i := range d {
		fmt.Println(i)
	}
	for _, i := range e {
		fmt.Println(i)
	}

}
