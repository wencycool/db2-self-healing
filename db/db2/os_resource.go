package db2

import (
	"fmt"
	"os/exec"
	"reflect"
	"strings"
	"time"
)

//获取OS资源使用情况
type OsResource struct {
	SnapTime        time.Time `column:"CURRENT TIMESTAMP"`
	OsName          string    `column:"OS_NAME"`
	HostName        string    `column:"HOST_NAME"`
	CpuTotal        int64     `column:"CPU_TOTAL"` //int64不参与运算
	CpuOnline       int64     `column:"CPU_ONLINE"`
	CpuConf         int64     `column:"CPU_CONFIGURED "`
	MemTotal        int64     `column:"MEMORY_TOTAL"` //MB
	MemFree         int64     `column:"MEMORY_FREE"`
	SwapTotal       int64     `column:"MEMORY_SWAP_TOTAL"` //MB
	SwapFree        int64     `column:"MEMORY_SWAP_FREE"`
	VirtualTotal    int64     `column:"VIRTUAL_MEM_TOTAL"` //MB
	VirtualResd     int64     `column:"VIRTUAL_MEM_RESERVED"`
	VirtualFree     int64     `column:"VIRTUAL_MEM_FREE"`
	LoadShort       int64     `column:"CPU_LOAD_SHORT"`
	LoadMedium      int64     `column:"CPU_LOAD_MEDIUM"`
	LoadLong        int64     `column:"CPU_LOAD_LONG"`
	CpuUser         int       `column:"CPU_USER"`
	CpuSys          int       `column:"CPU_SYSTEM"`
	CpuIoWait       int       `column:"CPU_IOWAIT"`
	CpuIdle         int       `column:"CPU_IDLE"`
	SwapPgSize      int64     `column:"SWAP_PAGE_SIZE"`
	SwapPgsIn       int       `column:"SWAP_PAGES_IN"`
	SwapPgsOut      int       `column:"SWAP_PAGES_OUT"`
	OsFullVersion   string    `column:"OS_FULL_VERSION"`
	OsKernelVersion string    `column:"OS_KERNEL_VERSION"`
	OsArchType      string    `column:"OS_ARCH_TYPE"`
}

func NewOsResource() *OsResource {
	m := new(OsResource)
	ms := make([]*OsResource, 0)
	sqlArg := fmt.Sprintf("select %s from table(SYSPROC.ENV_GET_SYSTEM_RESOURCES())  AS t where t.MEMBER = 0 with ur ", reflectMonGet(m))
	cmd := exec.Command("db2", "-x", "+p", sqlArg)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		log.Warn(err)
	}
	for _, line := range strings.Split(string(bs), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := new(OsResource)
		if err := renderStruct(d, line); err != nil {
			log.Warn(err)
			continue
		}
		ms = append(ms, d)
	}
	return ms[0]
}

func (r *OsResource) Update(duration time.Duration) {
	go func() {
		numFields := reflect.TypeOf(r).Elem().NumField()
		fields := reflect.ValueOf(r).Elem()
		ticker := time.NewTicker(duration)
		lastR := NewOsResource()
		for {
			select {
			case <-ticker.C:
				curR := NewOsResource()
				for i := 0; i < numFields; i++ {
					if fields.Field(i).Type().String() == "int" && reflect.ValueOf(r).Elem().Field(i).CanSet() {
						fields.Field(i).SetInt(reflect.ValueOf(curR).Elem().Field(i).Int() - reflect.ValueOf(lastR).Elem().Field(i).Int())
					} else {
						fields.Field(i).Set(reflect.ValueOf(curR).Elem().Field(i))
					}
				}
				lastR = curR
			}
		}
	}()
}

//获取网络资源,按照主机聚合
type NetResource struct {
	SnapTime      time.Time `column:"CURRENT TIMESTAMP"`
	HostName      string    `column:"HOST_NAME"`
	ReceivedBytes int       `column:"TOTAL_BYTES_RECEIVED"`
	SendBytes     int       `column:"TOTAL_BYTES_SENT"`
}

func NewNetResource() *NetResource {
	m := new(NetResource)
	ms := make([]*NetResource, 0)
	sqlArg := fmt.Sprintf("select %s from (select sum(TOTAL_BYTES_RECEIVED) as TOTAL_BYTES_RECEIVED,sum(TOTAL_BYTES_SENT) as TOTAL_BYTES_SENT table(SYSPROC.ENV_GET_NETWORK_RESOURCES())  AS t where t.MEMBER = 0 group by HOST_NAME) with ur ", reflectMonGet(m))
	cmd := exec.Command("db2", "-x", "+p", sqlArg)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		log.Warn(err)
	}
	for _, line := range strings.Split(string(bs), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := new(NetResource)
		if err := renderStruct(d, line); err != nil {
			log.Warn(err)
			continue
		}
		ms = append(ms, d)
	}
	return ms[0]
}

func (r *NetResource) Update(duration time.Duration) {
	go func() {
		numFields := reflect.TypeOf(r).Elem().NumField()
		fields := reflect.ValueOf(r).Elem()
		ticker := time.NewTicker(duration)
		lastR := NewNetResource()
		for {
			select {
			case <-ticker.C:
				curR := NewNetResource()
				for i := 0; i < numFields; i++ {
					if fields.Field(i).Type().String() == "int" && reflect.ValueOf(r).Elem().Field(i).CanSet() {
						fields.Field(i).SetInt(reflect.ValueOf(curR).Elem().Field(i).Int() - reflect.ValueOf(lastR).Elem().Field(i).Int())
					} else {
						fields.Field(i).Set(reflect.ValueOf(curR).Elem().Field(i))
					}
				}
				lastR = curR
			}
		}
	}()
}

//获取数据库使用率资源(增量)，不能用和osresource做对比，短暂时间内计算存在差异
type Db2Resource struct {
	SnapTime       time.Time `column:"CURRENT TIMESTAMP"`
	Db2ProcessId   int64     `column:"DB2_PROCESS_ID"`
	CpuUser        int       `column:"CPU_USER"`
	CpuSys         int       `column:"CPU_SYSTEM"`
	Db2ProcessName string    `column:"DB2_PROCESS_NAME"`
}

func NewDb2Resource() *Db2Resource {
	m := new(Db2Resource)
	ms := make([]*Db2Resource, 0)
	sqlArg := fmt.Sprintf("select %s from TABLE(ENV_GET_DB2_SYSTEM_RESOURCES(-1))  AS t where t.DB2_PROCESS_NAME = 'db2sysc 0' with ur ", reflectMonGet(m))
	cmd := exec.Command("db2", "-x", "+p", sqlArg)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		log.Warn(err)
	}
	for _, line := range strings.Split(string(bs), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := new(Db2Resource)
		if err := renderStruct(d, line); err != nil {
			log.Warn(err)
			continue
		}
		ms = append(ms, d)
	}
	return ms[0]
}

func (r *Db2Resource) Update(duration time.Duration) {
	go func() {
		numFields := reflect.TypeOf(r).Elem().NumField()
		fields := reflect.ValueOf(r).Elem()
		ticker := time.NewTicker(duration)
		lastR := NewDb2Resource()
		for {
			select {
			case <-ticker.C:
				curR := NewDb2Resource()
				for i := 0; i < numFields; i++ {
					if fields.Field(i).Type().String() == "int" && reflect.ValueOf(r).Elem().Field(i).CanSet() {
						fields.Field(i).SetInt(reflect.ValueOf(curR).Elem().Field(i).Int() - reflect.ValueOf(lastR).Elem().Field(i).Int())
					} else {
						fields.Field(i).Set(reflect.ValueOf(curR).Elem().Field(i))
					}
				}
				lastR = curR
			}
		}
	}()

}
