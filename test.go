package main

import (
	"fmt"
	"strings"
)

func main() {
	fmt.Println(strings.Split("asdf", "\n")[0])
}

type T struct {
	Id   int
	Next *T
}
