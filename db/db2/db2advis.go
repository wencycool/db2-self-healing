package db2

import (
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

//处理advis信息，生成索引建议最原始信息
type AdvisRaw struct {
	Schema          string //处理的SQLschema
	StmtText        string //SQL语句
	TotalSpace      int    //byte,total disk space constrained to
	NeedSpace       int    //byte,total disk space needed for initial set
	Timerons        int    //timerons  (without recommendations)
	advisedTimerons int    //timerons  (with current solution)
	Improvement     int    //improvement
	AdvisIndexes    []*AdvisIndex
}
type AdvisIndex struct {
	TabSchema string //表模式
	TabName   string //表名
	IdxSchema string //索引模式
	IdxName   string //索引名
	Text      string //索引语句
	Size      int    //byte,预计索引大小
}

//获取索引建议信息，但是执行较慢，建议并行执行或者一次执行多条SQL
func getAdvisRaw(dbname, schema, sql string) (*AdvisRaw, error) {
	advis := new(AdvisRaw)
	advis.Schema = schema
	advis.StmtText = sql
	args := []string{"-d", dbname, "-q", schema, "-n", schema, "-s", sql}
	cmd := exec.Command("db2advis", args...)
	bs, err := cmd.CombinedOutput()
	result := string(bs)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("SQL:%s;result:%s", "db2advis"+strings.Join(args, " "), result))
	}
	//解析advis语句
	pattern := regexp.MustCompile(`total disk space needed for initial set\s*\[\s*(?P<totalSize>\d+)(?:.\d+)*\] MB
total disk space constrained to\s+\[\s*(?P<needSize>\d+)(?:.\d+)\] MB
(?s:.+)(?P<indexCount>\d+)\s+indexes in current solution
 \[\s*(?P<advisedTimerons>\d+)(?:.\d+)*\s*\] timerons  \(without recommendations\)
 \[\s*(?P<Timerons>\d+)(?:.\d+)*\s*\] timerons  \(with current solution\)
 \[\s*(?P<Improvement>\d+)(?:.\d+)*%\s*\] improvement`)
	endPos := strings.Index(result, "LIST OF RECOMMENDED INDEXES")
	if endPos <= 0 {
		return nil, errors.New("找不到索引列表")
	}
	resultHead := result[:endPos]
	if !pattern.MatchString(resultHead) {
		return nil, errors.New("找不到索引开头匹配项")
	}
	match := pattern.FindStringSubmatch(resultHead)
	if totalSize, err := strconv.Atoi(match[1]); err != nil {
		return nil, err
	} else {
		advis.TotalSpace = totalSize << 20
	}
	if needSize, err := strconv.Atoi(match[2]); err != nil {
		return nil, err
	} else {
		advis.NeedSpace = needSize << 20
	}
	if advisedTimerons, err := strconv.Atoi(match[4]); err != nil {
		return nil, err
	} else {
		advis.advisedTimerons = advisedTimerons
	}
	if Timerons, err := strconv.Atoi(match[5]); err != nil {
		return nil, err
	} else {
		advis.Timerons = Timerons
	}
	if Improvement, err := strconv.Atoi(match[6]); err != nil {
		return nil, err
	} else {
		advis.Improvement = Improvement
	}
	startPos := strings.Index(result, "LIST OF RECOMMENDED INDEXES")
	endPos = strings.Index(result, "RECOMMENDED EXISTING INDEXES")
	pattern = regexp.MustCompile(`-- index\[\d+\],\s+(?P<indexSize>\d+)(?:.\d+)*MB\n(?s:.+)(?P<index>CREATE.+INDEX(?s:.+?)"(?P<idxSchema>.+?)"\."(?P<idxName>.+?)"(?s:.+?)ON(?s:.+?)"(?P<tabSchema>.+?)"\."(?P<tabName>.+?)"(?s:.+?));(?s:.+?)COMMIT WORK`)
	for _, submatch := range pattern.FindAllStringSubmatch(result[startPos:endPos], -1) {
		idx := new(AdvisIndex)
		if size, err := strconv.Atoi(submatch[1]); err != nil {
			return nil, err
		} else {
			idx.Size = size << 20
		}
		idx.Text = submatch[2]
		idx.IdxSchema = submatch[3]
		idx.IdxName = submatch[4]
		idx.TabSchema = submatch[5]
		idx.TabName = submatch[6]
		advis.AdvisIndexes = append(advis.AdvisIndexes, idx)
	}
	return advis, nil
}

func GetAdvisRaw(dbname, schema, sql string) (*AdvisRaw, error) {
	return getAdvisRaw(dbname, schema, sql)
}
