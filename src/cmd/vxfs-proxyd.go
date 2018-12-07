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
		address   string
		safeCode  string
		noDigMime bool

		statsRefresh     int
		nameDataFreeMB   int
		storeDataFreeMB  int
		storeIndexFreeMB int
	}{}
)

func init() {
	flag.StringVar(&myArgs.address, "vxfsAddress", ":1750", "network bind address, [host:]port")
	flag.StringVar(&myArgs.safeCode, "vxfsSafeCode", "", "validate http header VXFS-SAFE-CODE on PUT & DELETE")
	flag.BoolVar(&myArgs.noDigMime, "vxfsNoDigMime", false, "disable http content type deep guess on PUT")
	flag.IntVar(&myArgs.statsRefresh, "vxfsStatsRefresh", 5, "stats refresh interval, second")
	flag.IntVar(&myArgs.nameDataFreeMB, "vxfsNameDataFree", 100, "require <name server> data free space, MB")
	flag.IntVar(&myArgs.storeDataFreeMB, "vxfsStoreDataFree", 200, "require <sotre server> data free space, MB")
	flag.IntVar(&myArgs.storeIndexFreeMB, "vxfsStoreIndexFree", 60, "require <sotre server> index free space, MB")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "The vxfs proxy server, Version: %s\n"+
			"\n%s <machine id> <name server> <store server list>\n"+
			"\nFormats:\n"+
			"  <machine id> 1 ~ "+fmt.Sprintf("%d\n", libs.MaxMachineId)+
			"  <name server> host:port\n"+
			"  <sotre server list> id1/host1:port1,id2/host2:port2..., the id must gt 0\n"+
			"\nOptions:\n", myVer, myName)
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if flag.NArg() < 3 {
		fmt.Println("incorrect parameter count")
		flag.Usage()
		return
	}

	machineIdStr := flag.Args()[0]
	nameServerAddress := flag.Args()[1]
	storeServerGroup := flag.Args()[2]

	if !libs.IsHostPort(myArgs.address) {
		fmt.Println("incorrect option: vxfsAddress")
		flag.Usage()
		return
	}

	machineId := 0
	if libs.IsIntegerText(machineIdStr) {
		machineId, _ = strconv.Atoi(machineIdStr)
	}
	if machineId < 1 || machineId > libs.MaxMachineId {
		fmt.Println("incorrect parameter: machine id")
		flag.Usage()
		return
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

	keyMaker, _ := libs.NewSnowFlake(int64(machineId))
	server, err := proxy.NewProxyServer(myArgs.address, myArgs.safeCode, myArgs.noDigMime, keyMaker, serviceManager)
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
