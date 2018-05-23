package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
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
)

var cancel context.CancelFunc
var listener net.Listener
var rpcListener net.Listener

/*
Combined function for all cleanup tasks to be done in the signal handler.
*/
func cleanup() {
	if cancel != nil {
		cancel()
	}
	if listener != nil {
		listener.Close()
	}
	if rpcListener != nil {
		rpcListener.Close()
	}
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

/*
Reports an error from the parent function, then cleans up.
*/
func reportErrorAndCleanup(err error) {
	log.Print("Error in MainLoop: ", err)
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

	var sigChannel = make(chan os.Signal)
	var client *etcd.Client
	var uploadService UploadService
	var tlsConfig *tls.Config
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
	flag.Parse()

	/* Cancel etcd watcher when we stop the process. */
	signal.Notify(sigChannel, syscall.SIGINT, syscall.SIGTERM)
	go handleSignals(sigChannel)

	if err = rados.InitRados(); err != nil {
		log.Fatal("Error initializing Rados: ", err)
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

	go uploadService.WatchForConfigChanges(client, etcdTimeout, configPath)

	err = uploadService.MainLoop(listener, rpcListener, tlsConfig)
	if err != nil {
		log.Print("Error in MainLoop: ", err)
	}
	cleanup()
}
