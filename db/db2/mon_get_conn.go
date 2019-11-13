package db2

//结合mon_get_connection,mon_get_agent,ENV_GET_DB2_EDU_SYSTEM_RESOURCES三个系统视图，组成application当前的信息
//mon_get_agent只取当前的coordagent，AGENT_TYPE='COORDINATOR'，即协调agent信息,保持agent和appid一一对应

//定义记录当前活动的连接信息
type MonGetConn struct {
}

//定义记录连接分布信息，方便查询基于客户主机的连接数信息
type MonGetConnDistrib struct {
}
