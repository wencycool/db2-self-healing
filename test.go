package main

import "fmt"

func main() {
	fmt.Printf("\033[5;40;32m%s\033[0m", "test")
	fmt.Printf("\n %c[5;40;32m%s%c[0m\n\n", 0x1B, "testPrintColor", 0x1B)
	fmt.Println(0x1B)

}
