package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/childoftheuniverse/filesystem"
	_ "github.com/childoftheuniverse/filesystem-file"
	rados "github.com/childoftheuniverse/filesystem-rados"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
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
	var rpcBindAddress string
	var etcdServerList string
	var etcdTimeout time.Duration
	var etcdConfig etcd.Config

	var privateKeyPath string
	var publicKeyPath string
	var clientCA string
	var radosConfigPath string

	var sigChannel = make(chan os.Signal)
	var client *etcd.Client
	var uploadService UploadService
	var configUpdateChannel etcd.WatchChan
	var configUpdate etcd.WatchResponse
	var getResp *etcd.GetResponse
	var kv *mvccpb.KeyValue
	var rev int64
	var listener net.Listener
	var rpcListener net.Listener
	var tlsConfig *tls.Config
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
	flag.StringVar(&rpcBindAddress, "rpc-bind", "[::]:0",
		"Additional address to bind for GRPC requests only")

	/* TLS configuration */
	flag.StringVar(&privateKeyPath, "tls-key", "",
		"TLS private key of server certificate. If unset, TLS will not be used.")
	flag.StringVar(&publicKeyPath, "tls-cert", "",
		"TLS server certificate. If unset, TLS will not be used.")
	flag.StringVar(&clientCA, "client-ca", "",
		"Path to a client CA certificate file. If unset, clients will not be "+
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

	if privateKeyPath != "" && publicKeyPath != "" {
		var cert tls.Certificate
		tlsConfig = new(tls.Config)

		cert, err = tls.LoadX509KeyPair(publicKeyPath, privateKeyPath)
		if err != nil {
			log.Fatal("Error loading X.509 certificates: ", err)
		}

		tlsConfig.Certificates = append(tlsConfig.Certificates, cert)

		if clientCA != "" {
			var certContents []byte
			tlsConfig.ClientCAs = x509.NewCertPool()

			if certContents, err = ioutil.ReadFile(clientCA); err != nil {
				log.Fatal("Error reading X.509 client CA file: ", err)
			}

			if !tlsConfig.ClientCAs.AppendCertsFromPEM(certContents) {
				log.Fatal("Error loading X.509 client CA from ", clientCA)
			}

			tlsConfig.RootCAs = tlsConfig.ClientCAs
		}
		if rpcBindAddress != "" {
			if listener, err = net.Listen("tcp", bindAddress); err != nil {
				log.Fatal("Unable to start listening to ", bindAddress, ": ", err)
			}
			rpcListener, err = net.Listen("tcp", rpcBindAddress)
			if err != nil {
				log.Fatal("Unable to start listening to ", rpcBindAddress, ": ",
					err)
			}
		} else {
			listener, err = tls.Listen("tcp", bindAddress, tlsConfig)
			if err != nil {
				log.Fatal("Unable to start listening to ", bindAddress, ": ", err)
			}
		}
	} else {
		if listener, err = net.Listen("tcp", bindAddress); err != nil {
			log.Fatal("Unable to start listening to ", bindAddress, ": ", err)
		}
		if rpcBindAddress != "" {
			rpcListener, err = net.Listen("tcp", rpcBindAddress)
			if err != nil {
				log.Fatal("Unable to start listening to ", rpcBindAddress, ": ",
					err)
			}
		}
	}
	defer listener.Close()
	if rpcListener != nil {
		defer rpcListener.Close()
	}

	etcdConfig.Endpoints = strings.Split(etcdServerList, ",")
	etcdConfig.DialTimeout = etcdTimeout

	if tlsConfig != nil {
		etcdConfig.TLS = tlsConfig
	}

	if client, err = etcd.New(etcdConfig); err != nil {
		log.Fatal("Error connecting to etcd: ", err)
	}
	defer client.Close()

	go uploadService.MainLoop(listener, rpcListener, tlsConfig)

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

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	configUpdateChannel = client.Watch(
		ctx, configPath, etcd.WithCreatedNotify(),
		etcd.WithRev(rev))
	for configUpdate = range configUpdateChannel {
		var ev *etcd.Event
		for _, ev = range configUpdate.Events {
			uploadService.UploadConfig(ev.Kv.Value)
		}
	}
}
