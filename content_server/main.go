package main

import (
	"context"
	"crypto/tls"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/childoftheuniverse/filesystem-file"
	rados "github.com/childoftheuniverse/filesystem-rados"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
)

var cancel context.CancelFunc

func cleanup() {
	cancel()
	os.Exit(1)
}

/*
Stop etcd listener when receiving SIGINT or SIGTERM.
*/
func handleSignals(sigChannel chan os.Signal) {
	for {
		var s = <-sigChannel
		switch s {
		case syscall.SIGINT:
			cleanup()
		case syscall.SIGTERM:
			cleanup()
		}
	}
}

func main() {
	var configPath string
	var bindAddress string
	var etcdServerList string
	var etcdTimeout time.Duration
	var etcdConfig etcd.Config

	var privateKeyPath string
	var publicKeyPath string
	var serverCA string

	var contentService ContentService
	var sigChannel = make(chan os.Signal)
	var client *etcd.Client
	var tlsConfig *tls.Config
	var tlsInfo transport.TLSInfo
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
	flag.Parse()

	/* Cancel etcd watcher when we stop the process. */
	signal.Notify(sigChannel, syscall.SIGINT, syscall.SIGTERM)
	go handleSignals(sigChannel)

	if err = rados.InitRados(); err != nil {
		log.Fatal("Error initializing rados: ", err)
	}

	etcdConfig.Endpoints = strings.Split(etcdServerList, ",")
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

	go contentService.WatchForConfigChanges(client, etcdTimeout, configPath)

	if err = contentService.MainLoop(bindAddress); err != nil {
		log.Print("Error in MainLoop: ", err)
	}
	cleanup()
}
