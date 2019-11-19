package db2

import (
	"bytes"
	"errors"
	"os/exec"
	"os/user"
	"strings"
)

//获取当前实例名
func GetCurInstanceName() (string, error) {
	user, err := user.Current()
	if err != nil {
		return "", err
	}
	return user.Name, nil
	/*
		if I1 := getCurInstance1();I1 != "" {
			return I1,nil
		}else if I2 := getCurInstance2();I2 != "" {
			return I2,nil
		}
		return "",errors.New("Cannot get Instance")
	*/
}

//优先通过DB2环境变量获取
func getCurInstance1() string {
	bs, err := exec.Command("db2", "get current instance").CombinedOutput()
	if err != nil {
		return ""
	}
	fields := strings.Split(string(bytes.TrimSpace(bytes.ReplaceAll(bs, []byte("\n"), []byte{}))), ":")
	if len(fields) == 2 {
		return strings.TrimSpace(fields[1])
	}
	return ""
}

//检查当前用户是否实例用户并且存在该实例
func getCurInstance2() string {
	u, _ := user.Current()
	bs, err := exec.Command("/bin/ps", "-ef", "|awk '$0 ~ /db2sysc 0/ {print $1;exit;}'").CombinedOutput()
	if err != nil {
		return ""
	}
	if u.Name == strings.TrimSpace(string(bs)) {
		return u.Name
	}
	return ""
}

//获取当前实例下数据库列表
func GetCurDatabases() ([]*DbEntiry, error) {
	entryList := make([]*DbEntiry, 0)
	bs, err := exec.Command("db2", "list db directory").CombinedOutput()
	result := string(bs)
	if err != nil {
		return nil, errors.New(result)
	}
	var dbalias, dbname, directory, releaseLevel, entryType string
	for _, v := range strings.Split(result, "\n") {
		fields := strings.Split(v, "=")
		if len(fields) != 2 {
			continue
		}
		f1, f2 := fields[0], strings.TrimSpace(fields[1])
		switch {
		case strings.Contains(f1, "Database alias"):
			dbalias = f2
		case strings.Contains(f1, "Database name"):
			dbname = f2
		case strings.Contains(f1, "Local database directory"):
			directory = f2
		case strings.Contains(f1, "Database release level"):
			releaseLevel = f2
		case strings.Contains(f1, "Directory entry type"):
			entryType = f2
			//只添加本地库
			if entryType == "Indirect" {
				entryList = append(entryList, &DbEntiry{
					DbAlias:      dbalias,
					DbName:       dbname,
					Directory:    directory,
					ReleaseLevel: releaseLevel,
					EntryType:    entryType,
				})
			}

		}
	}
	return entryList, nil
}

type DbEntiry struct {
	DbAlias      string
	DbName       string
	Directory    string
	ReleaseLevel string
	EntryType    string
}
