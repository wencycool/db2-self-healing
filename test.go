package main

import (
	"my/vm"
	"time"
)

func main() {
	vm.MemLimit(1024)
	a := make([]string, 0)
	for i := 0; i < 100000; i++ {
		a = append(a, "asdfasdfasfdasdfasdfasodfjasodfjasof")
		time.Sleep(time.Millisecond * 100)
	}

}
