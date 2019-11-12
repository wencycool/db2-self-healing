package db2

import (
	"fmt"
	"os/exec"
	"strings"
)

/*
Value flag:
Provides specific information for the configuration parameter current value. Valid values are:
NONE - no additional information
AUTOMATIC - the configuration parameter has been set to automatic
COMPUTED - the configuration parameter has been set to a computed value
*/

//获取数据库和实例参数相关信息
type MonGetDbCfg struct {
	Name    string `column:"NAME"`
	Value   string `column:"VALUE"`
	ValFlag string `column:"VALUE_FLAGS"`
}

func GetMonGetDbCfgMap() (map[string]*MonGetDbCfg, error) {
	mp := make(map[string]*MonGetDbCfg, 0)
	cols := reflectMonGet(new(MonGetDbCfg))
	argSql := fmt.Sprintf("select %s from table(DB_GET_CFG(-1)) as t with ur", cols)
	cmd := exec.Command("db2", "-x", argSql)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}
	for _, line := range strings.Split(string(bs), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := new(MonGetDbCfg)
		if err := renderStruct(d, line); err != nil {
			log.Warn(err)
		}
		mp[d.Name] = d
	}
	return mp, nil
}

type MonGetDbmCfg struct {
	Name    string `column:"NAME"`
	Value   string `column:"VALUE"`
	ValFlag string `column:"VALUE_FLAGS"`
}

func GetMonGetDbmCfgMap() (map[string]*MonGetDbmCfg, error) {
	mp := make(map[string]*MonGetDbmCfg, 0)
	cols := reflectMonGet(new(MonGetDbmCfg))
	argSql := fmt.Sprintf("select %s from table(DBM_GET_CFG(-1)) AS T with ur", cols)
	cmd := exec.Command("db2", "-x", argSql)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}
	for _, line := range strings.Split(string(bs), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := new(MonGetDbmCfg)
		if err := renderStruct(d, line); err != nil {
			log.Warn(err)
		}
		mp[d.Name] = d
	}
	return mp, nil
}
