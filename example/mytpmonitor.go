package main

import (
	"flag"
	"fmt"
	"github.com/2tvenom/myreplication"
	"github.com/chenzhe07/goconfig"
	_ "github.com/czgolib/log"
	_ "github.com/davecgh/go-spew/spew"
	"os"
	"regexp"
	"strconv"
	"time"
)

type MySQLInfo struct {
	Host           string
	Port           int
	Schema         string
	Timestamp      string
	Executiontime  uint32
	Nextbinlogname string
	Nextposition   uint32
	query          string
}

var (
	section  = "monitor"
	host     = "localhost"
	port     = int64(3306)
	username = "monitor"
	password = "monitor"
	binlog   = ""
	pos      = int64(4)
)

func timeFormat(t uint32) string {
	const time_format = "2006-01-02T15:04:05"
	return time.Unix(int64(t), 0).Format(time_format)
}

func logOut(info MySQLInfo) bool {
	var msgOut string
	msgOut = fmt.Sprintf("Time: %s\nHost: %s\nPort: %d\nSchema: %s\n"+
		"Executiontime: %d\nbinlogname: %s\nposition: %d\nQuery: \"%s\"\n",
		info.Timestamp, info.Host, info.Port, info.Schema, info.Executiontime,
		info.Nextbinlogname, info.Nextposition, info.query)
	fmt.Println(msgOut)

	return true
}

func main() {
	conf := flag.String("conf", "", "configure file.")
	s := flag.String("section", "monitor", "configure section.")
	h := flag.String("host", "localhost", "mysql server address.")
	P := flag.Int64("port", 3306, "mysql server port.")
	u := flag.String("user", "monitor", "mysql server user.")
	p := flag.String("pass", "monitor", "mysql server password for user.")
	f := flag.String("binlog", "", "mysql server binlog file name")
	n := flag.Int64("pos", 4, "mysql binlog position start dump from the pos.")
	flag.Parse()

	if len(*conf) <= 0 {
		host = *h
		port = *P
		username = *u
		password = *p
		binlog = *f
		pos = *n
	}

	section = *s
	c, err := goconfig.ReadConfigFile(*conf)
	host, err = c.GetString(section, "host")
	port, err = c.GetInt64(section, "port")
	username, err = c.GetString(section, "username")
	password, err = c.GetString(section, "password")
	binlog, err = c.GetString(section, "binlog")
	pos, err = c.GetInt64(section, "pos")
	if err != nil {
		panic("readconfigfile err: " + err.Error())
		os.Exit(1)
	}

	newConnection := myreplication.NewConnection()
	serverId := uint32(2)
	err = newConnection.ConnectAndAuth(host, int(port), username, password)

	if err != nil {
		panic("Client not connected and not autentificate to master server with error:" + err.Error())
	}
	//Get position and file name
	pos_end, filename, err := newConnection.GetMasterStatus()

	if err != nil {
		panic("Master status fail: " + err.Error())
	}

	if len(binlog) == 0 {
		binlog = filename
	}

	c.AddOption(section, "pos", strconv.FormatInt(int64(pos_end), 10))
	c.AddOption(section, "binlog", filename)
	c.WriteConfigFile(*conf, 0644, "monitor for mytpmonitor tool")

	if uint32(pos) == pos_end && binlog == filename {
		os.Exit(0)
	}

	el, err := newConnection.StartBinlogDump(uint32(pos), binlog, serverId)

	if err != nil {
		panic("Cant start bin log: " + err.Error())
	}

	events := el.GetEventChan()
	var LogPos uint32
	go func() {
		for {

			event := <-events
			//spew.Dump(event)
			switch e := event.(type) {
			case *myreplication.QueryEvent:
				LogPos = e.GetNextPosition()
				matched, matcherr := regexp.MatchString("(?i:^grant|^set|^revoke|^create|^drop|^alter|^truncate|^rename)", e.GetQuery())
				if matched && matcherr == nil {
					mysqlinfo := MySQLInfo{}
					mysqlinfo.Host = host
					mysqlinfo.Port = int(port)
					mysqlinfo.Schema = e.GetSchema()
					mysqlinfo.Timestamp = timeFormat(e.GetTimeStamp())
					mysqlinfo.Executiontime = e.GetExecutionTime()
					mysqlinfo.Nextbinlogname = el.GetLastLogFileName()
					mysqlinfo.Nextposition = e.GetNextPosition()
					mysqlinfo.query = e.GetQuery()
					logOut(mysqlinfo)
					if mysqlinfo.Nextbinlogname != filename {
						// as the binlog rotate, begin from 4
						LogPos = 4
					}
				}

			case *myreplication.IntVarEvent:
				LogPos = e.GetNextPosition()
			case *myreplication.XidEvent:
				LogPos = e.GetNextPosition()
			case *myreplication.UserVarEvent:
				LogPos = e.GetNextPosition()
			default:
			}

			if LogPos >= pos_end {
				os.Exit(0)
			}
		}
	}()

	err = el.Start()
	println(err.Error())
}
