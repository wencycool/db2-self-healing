package main

import (
	"os"
	"os/exec"
)

func main() {
	cmd := exec.Command("db2advis", "-d SAMPLE")
	cmd.Stdout = os.Stdout
	os.Stdout.WriteString(cmd.String())
	cmd.Run()
}
