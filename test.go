package main

import (
	"fmt"
	"regexp"
)

func main() {
	str := "<123123><schema>"
	patt := regexp.MustCompile(`^<(\d+)><(\w+)>$`)
	fmt.Println(patt.MatchString(str))
	m := patt.FindAllStringSubmatch(str, 1)
	fmt.Println(m[0][2])
}
