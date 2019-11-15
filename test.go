package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

func main() {
	//统计代码行数
	cnt := 0
	filepath.Walk("/Users/wency/go/src/awesomeProject/db-self-healing/db", func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			f, _ := os.Open(path)
			bs, _ := ioutil.ReadAll(f)
			n := len(bytes.Split(bs, []byte("\n")))
			cnt += n
		}
		return nil
	})
	fmt.Println(cnt)
}
