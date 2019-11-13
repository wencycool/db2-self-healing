package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"time"
)

//测试返回接口

func main() {
	//测试连接DB2速度情况
	var in bytes.Buffer
	cmd := exec.Command("db2", "-x", "+p")
	cmd.Stdin = &in
	in.WriteString("connect to sample\n")
	bs, _ := cmd.CombinedOutput()
	fmt.Println("xxxxxxxxxx", string(bs))
	for i := 0; i < 5; i++ {
		go func() {
			for {
				s1, s2 := test()
				os.Stdout.WriteString(s1 + s2)
			}

		}()

	}
	select {}
}

func test() (string, string) {
	t1 := time.Now()
	cmd := exec.Command("db2", "-x", "select card from syscat.tables fetch first 1 rows only with ur")
	bs, _ := cmd.CombinedOutput()
	return string(bs), time.Now().Sub(t1).String()
}
