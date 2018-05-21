package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/childoftheuniverse/filesystem-file"
	rados "github.com/childoftheuniverse/filesystem-rados"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/coreos/etcd/pkg/transport"
)

var cancel context.CancelFunc

/*
Stop etcd listener when receiving SIGINT or SIGTERM.
*/
func handleSignals(sigChannel chan os.Signal) {
	for {
		var s = <-sigChannel
		switch s {
		case syscall.SIGINT:
			cancel()
			os.Exit(1)
		case syscall.SIGTERM:
			cancel()
			os.Exit(1)
		}
	}
}

func main() {
	var configPath string
	var bindAddress string
	var etcdServerList string
	var etcdServers []string
	var etcdTimeout time.Duration
	var etcdConfig etcd.Config

	var privateKeyPath string
	var publicKeyPath string
	var serverCA string
	var radosConfigPath string

	var contentService ContentService
	var sigChannel = make(chan os.Signal)
	var client *etcd.Client
	var configUpdateChannel etcd.WatchChan
	var configUpdate etcd.WatchResponse
	var getResp *etcd.GetResponse
	var kv *mvccpb.KeyValue
	var rev int64
	var tlsConfig *tls.Config
	var tlsInfo transport.TLSInfo
	var ctx context.Context
	var err error

	flag.StringVar(&configPath, "config", "",
		"Path to config file inside etcd")
	flag.StringVar(&etcdServerList, "etcd-servers", "",
		"Comma separated list of etcd servers to contact")
	flag.DurationVar(&etcdTimeout, "etcd-timeout", 2*time.Second,
		"Maximum amount of time to wait for etcd server connection")
	flag.StringVar(&bindAddress, "bind", "[::]:0",
		"Address to bind to in order to serve requests")

	/* TLS configuration */
	flag.StringVar(&privateKeyPath, "tls-key", "",
		"TLS private key of server certificate. If unset, TLS will not be used.")
	flag.StringVar(&publicKeyPath, "tls-cert", "",
		"TLS server certificate. If unset, TLS will not be used.")
	flag.StringVar(&serverCA, "server-ca", "",
		"Path to a server CA certificate file. If unset, servers will not be "+
			"authenticated.")

	/* Filesystem configuration */
	flag.StringVar(&radosConfigPath, "rados-config", "",
		"Path of a Rados configuration to read")
	flag.Parse()

	/* Cancel etcd watcher when we stop the process. */
	signal.Notify(sigChannel, syscall.SIGINT, syscall.SIGTERM)
	go handleSignals(sigChannel)

	if radosConfigPath != "" {
		err = rados.RegisterRadosConfig(radosConfigPath)
		if err != nil {
			fmt.Println("error reading RADOS configuration ", radosConfigPath, ": ",
				err)
		}
	}

	etcdServers = strings.Split(etcdServerList, ",")

	etcdConfig.Endpoints = etcdServers
	etcdConfig.DialTimeout = etcdTimeout

	if publicKeyPath != "" && privateKeyPath != "" {
		tlsInfo.CertFile = publicKeyPath
		tlsInfo.KeyFile = privateKeyPath
	}
	if serverCA != "" {
		tlsInfo.TrustedCAFile = serverCA
	}
	if (publicKeyPath != "" && privateKeyPath != "") || serverCA != "" {
		tlsConfig, err = tlsInfo.ClientConfig()
		if err != nil {
			log.Fatal("Error setting up TLS config: ", err)
		}
		etcdConfig.TLS = tlsConfig
	}

	if client, err = etcd.New(etcdConfig); err != nil {
		log.Fatal("Error connecting to etcd: ", err)
	}
	defer client.Close()

	ctx, cancel = context.WithTimeout(context.Background(), etcdTimeout)

	getResp, err = client.Get(ctx, configPath)
	cancel()
	if err != nil {
		log.Fatal("Error reading configuration from ", configPath, ": ", err)
	}
	if len(getResp.Kvs) < 1 {
		log.Print("Cannot find current version of ", configPath)
	}
	for _, kv = range getResp.Kvs {
		rev = kv.ModRevision
	}

	go contentService.MainLoop(bindAddress)

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	configUpdateChannel = client.Watch(
		ctx, configPath, etcd.WithCreatedNotify(),
		etcd.WithRev(rev))
	for configUpdate = range configUpdateChannel {
		var ev *etcd.Event
		for _, ev = range configUpdate.Events {
			contentService.ServingConfig(ev.Kv.Value)
		}
	}
}
