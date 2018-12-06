package main

import (
	"flag"
	"fmt"
	"os"
	"vxfs/libs"
	"vxfs/libs/glog"
	"vxfs/store"
)

var (
	myName = "vxfs-stored"
	myVer  = "1.1"
	myArgs = struct {
		address string

		dataFreeMB   int
		indexFreeMB  int
		statsRefresh int
	}{}
)

func init() {
	flag.StringVar(&myArgs.address, "vxfsAddress", ":1730", "network bind address, [host:]port")
	flag.IntVar(&myArgs.dataFreeMB, "vxfsDataFree", 100, "require data store free space, MB")
	flag.IntVar(&myArgs.indexFreeMB, "vxfsIndexFree", 30, "require index store free space, MB")
	flag.IntVar(&myArgs.statsRefresh, "vxfsStatsRefresh", 10, "stats refresh interval, second")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "The vxfs store server, version: %s\n"+
			"\n%s <data store path> <index store path>\n"+
			"\nOptions:\n", myVer, myName)
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Println("incorrect parameter count")
		flag.Usage()
		return
	}

	dataDir := flag.Args()[0]
	indexDir := flag.Args()[1]

	if !libs.IsHostPort(myArgs.address) {
		fmt.Println("incorrect option: vxfsAddress")
		flag.Usage()
		return
	}

	publicAddress, err := libs.GetPublicHostPort(myArgs.address)
	if err != nil {
		glog.Exitln(err)
	}

	volumeGroup, err := store.NewVolumeGroup(dataDir, indexDir, myArgs.dataFreeMB, myArgs.indexFreeMB, myArgs.statsRefresh)
	if err != nil {
		glog.Exitln(err)
	}

	server, err := libs.NewRpcServer(myArgs.address, store.NewStoreService(volumeGroup))
	if err != nil {
		glog.Exitln(err)
	}

	glog.Infof("Run net/rpc server at (%s) %% (%s)\n", myArgs.address, publicAddress)

	go server.Serve()
	libs.WaitProcessExit(func() {
		server.Close()
		volumeGroup.Close()
		glog.Infof("Stop net/rpc server at (%s) %% (%s)\n", myArgs.address, publicAddress)
	})
}
