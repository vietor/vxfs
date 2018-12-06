package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"vxfs/libs"
	"vxfs/libs/glog"
	"vxfs/proxy"
)

var (
	myName = "vxfs-proxyd"
	myVer  = "1.1"
	myArgs = struct {
		address  string
		safeCode string

		statsRefresh     int
		nameDataFreeMB   int
		storeDataFreeMB  int
		storeIndexFreeMB int
	}{}
)

func init() {
	flag.StringVar(&myArgs.address, "vxfsAddress", ":1750", "network bind address, [host:]port")
	flag.StringVar(&myArgs.safeCode, "vxfsSafeCode", "", "safe code for http upload & delete")
	flag.IntVar(&myArgs.statsRefresh, "vxfsStatsRefresh", 5, "stats refresh interval, second")
	flag.IntVar(&myArgs.nameDataFreeMB, "vxfsNameDataFree", 100, "require <name server> data free space, MB")
	flag.IntVar(&myArgs.storeDataFreeMB, "vxfsStoreDataFree", 200, "require <sotre server> data free space, MB")
	flag.IntVar(&myArgs.storeIndexFreeMB, "vxfsStoreIndexFree", 60, "require <sotre server> index free space, MB")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "The vxfs proxy server, Version: %s\n"+
			"\n%s <datacenter id> <machine id> <name server> <store server list>\n"+
			"\nFormats:\n"+
			"  <datacenter id> 1 ~ 31\n"+
			"  <machine id> 1 ~ 31\n"+
			"  <name server> host:port\n"+
			"  <sotre server list> id1/host1:port1,id2/host2:port2..., the id must gt 0\n"+
			"\nOptions:\n", myVer, myName)
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if flag.NArg() < 4 {
		fmt.Println("incorrect parameter count")
		flag.Usage()
		return
	}

	datacenterIdStr := flag.Args()[0]
	machineIdStr := flag.Args()[1]
	nameServerAddress := flag.Args()[2]
	storeServerGroup := flag.Args()[3]

	if !libs.IsHostPort(myArgs.address) {
		fmt.Println("incorrect option: vxfsAddress")
		flag.Usage()
		return
	}

	if !libs.IsIntegerText(datacenterIdStr) {
		fmt.Println("incorrect parameter: datacenter id")
		flag.Usage()
		return
	}
	datacenterId, _ := strconv.Atoi(datacenterIdStr)

	if !libs.IsIntegerText(machineIdStr) {
		fmt.Println("incorrect parameter: machine id")
		flag.Usage()
		return
	}
	machineId, _ := strconv.Atoi(machineIdStr)

	idMaker, err := proxy.NewSnowFlake(int64(datacenterId), int64(machineId), 0)
	if err != nil {
		glog.Exitln(err)
	}

	serviceManager := proxy.NewServiceManager(myArgs.nameDataFreeMB, myArgs.storeDataFreeMB, myArgs.storeIndexFreeMB, myArgs.statsRefresh)

	if !libs.IsStrictHostPort(nameServerAddress) {
		fmt.Println("incorrect parameter: name server format")
		flag.Usage()
		return
	}
	serviceManager.SetNameService(nameServerAddress)

	for _, storeServerUnit := range strings.Split(storeServerGroup, ",") {
		fields := strings.Split(storeServerUnit, "/")
		if len(fields) != 2 || !libs.IsIntegerText(fields[0]) || !libs.IsStrictHostPort(fields[1]) {
			fmt.Println("incorrect parameter: sotre server format")
			flag.Usage()
			return
		}
		id, _ := strconv.Atoi(fields[0])
		if id < 1 {
			fmt.Println("incorrect parameter: sotre server, id lt 1")
			flag.Usage()
			return
		}
		if err := serviceManager.AddStoreService(int32(id), fields[1]); err != nil {
			fmt.Println("incorrect parameter: sotre server, id was repeated")
			flag.Usage()
			return
		}
	}

	serviceManager.Startup()

	publicAddress, err := libs.GetPublicHostPort(myArgs.address)
	if err != nil {
		glog.Exitln(err)
	}

	server, err := proxy.NewProxyServer(myArgs.address, myArgs.safeCode, idMaker, serviceManager)
	if err != nil {
		glog.Exitln(err)
	}

	glog.Infof("Run http server at (%s) %% (%s)\n", myArgs.address, publicAddress)

	go server.Serve()
	libs.WaitProcessExit(func() {
		server.Close()
		serviceManager.Cleanup()
		glog.Infof("Stop http server at (%s) %% (%s)\n", myArgs.address, publicAddress)
	})
}
