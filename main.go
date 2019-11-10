package main

import (
	"fmt"
	"log"
	"my/db/db2"
)

func main() {
	a, b, c, d, err := db2.CollectData("sample")
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

}
