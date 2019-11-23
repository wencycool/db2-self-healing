package db2

import (
	"os/exec"
	"strings"
)

//结合mon_get_connection,mon_get_agent,ENV_GET_DB2_EDU_SYSTEM_RESOURCES三个系统视图，组成application当前的信息
//mon_get_agent只取当前的coordagent，AGENT_TYPE='COORDINATOR'，即协调agent信息,保持agent和appid一一对应

//定义记录连接分布信息，方便查询基于客户主机的连接数信息，返回客户机名和连接数的分组
func GetConnDistribByClientHostName() [][2]string {
	result := make([][2]string, 0)
	sqlArgs := `elect CLIENT_HOSTNAME,count(*) as cnt from  TABLE(MON_GET_CONNECTION(cast(NULL as bigint), -1)) AS t group by CLIENT_HOSTNAME with ur`
	cmd := exec.Command("db2", "+p", "-x", sqlArgs)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		return nil
	}
	for _, v := range strings.Split(string(bs), "\n") {
		fields := strings.Fields(v)
		if len(fields) == 2 {
			result = append(result, [2]string{fields[0], fields[1]})
		}
	}
	return result
}

//select CLIENT_HOSTNAME,count(*) from  TABLE(MON_GET_CONNECTION(cast(NULL as bigint), -1)) AS t group by CLIENT_HOSTNAME with ur
