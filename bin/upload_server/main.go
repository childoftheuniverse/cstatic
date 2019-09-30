package main

import (
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

	"contrib.go.opencensus.io/exporter/zipkin"
	"github.com/childoftheuniverse/cstatic/upload"
	_ "github.com/childoftheuniverse/filesystem"
	_ "github.com/childoftheuniverse/filesystem-file"
	rados "github.com/childoftheuniverse/filesystem-rados"
	openzipkin "github.com/openzipkin/zipkin-go"
	openzipkinModel "github.com/openzipkin/zipkin-go/model"
	zipkinReporter "github.com/openzipkin/zipkin-go/reporter"
	zipkinHTTP "github.com/openzipkin/zipkin-go/reporter/http"
	etcd "go.etcd.io/etcd/clientv3"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
)

/*
Stop etcd listener when receiving SIGINT or SIGTERM.
*/
func handleSignals(
	uploadService *upload.UploadService, sigChannel chan os.Signal) {
	for {
		var s = <-sigChannel
		switch s {
		case syscall.SIGINT:
			fallthrough
		case syscall.SIGTERM:
			uploadService.Cleanup()
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
	var zipkinEndpoint string
	var thisHost string
	var traceProbability float64

	var privateKeyPath string
	var publicKeyPath string
	var clientCA string

	var sigChannel = make(chan os.Signal)
	var client *etcd.Client
	var uploadService upload.UploadService
	var listener net.Listener
	var rpcListener net.Listener
	var tlsConfig *tls.Config
	var err error

	if thisHost, err = os.Hostname(); err != nil {
		log.Printf("Warning: cannot determine host name: %s", err)
	}

	flag.StringVar(&zipkinEndpoint, "zipkin-endpoint",
		fmt.Sprintf("%s:9411", thisHost),
		"host:port pair to send Zipkin traces to")
	flag.Float64Var(&traceProbability, "trace-probability", -1.0,
		"If set to a value above 0, probability for traces to be sent")

	flag.StringVar(&configPath, "config", "",
		"Path to config file inside etcd")
	flag.StringVar(&etcdServerList, "etcd-servers",
		fmt.Sprintf("%s:2379", thisHost),
		"Comma separated list of etcd servers to contact")
	flag.DurationVar(&etcdTimeout, "etcd-timeout", 2*time.Second,
		"Maximum amount of time to wait for etcd server connection")
	flag.StringVar(&bindAddress, "bind", fmt.Sprintf("%s:0", thisHost),
		"Address to bind to in order to serve requests")
	flag.StringVar(&rpcBindAddress, "rpc-bind", fmt.Sprintf("%s:0", thisHost),
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

	if err = view.Register(ocgrpc.DefaultClientViews...); err != nil {
		log.Fatal("Error registering default gRPC client views: ", err)
	}
	if err = view.Register(ocgrpc.DefaultServerViews...); err != nil {
		log.Fatal("Error registering default gRPC server views: ", err)
	}

	if zipkinEndpoint != "" {
		var localEndpoint *openzipkinModel.Endpoint
		var reporter zipkinReporter.Reporter
		var zipkinExporter trace.Exporter

		localEndpoint, err = openzipkin.NewEndpoint(
			"cstatic_content_server", bindAddress)
		if err != nil {
			log.Fatalf("Failed to create the local zipkin endpoint: %s", err)
		}
		reporter = zipkinHTTP.NewReporter(fmt.Sprintf("http://%s/api/v2/spans",
			zipkinEndpoint))
		zipkinExporter = zipkin.NewExporter(reporter, localEndpoint)
		trace.RegisterExporter(zipkinExporter)

		if traceProbability >= 1.0 {
			trace.ApplyConfig(trace.Config{
				DefaultSampler: trace.AlwaysSample(),
			})
		} else if traceProbability >= 0.0 {
			trace.ApplyConfig(trace.Config{
				DefaultSampler: trace.ProbabilitySampler(traceProbability),
			})
		}
	}

	/* Cancel etcd watcher when we stop the process. */
	signal.Notify(sigChannel, syscall.SIGINT, syscall.SIGTERM)
	go handleSignals(&uploadService, sigChannel)

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
	uploadService.Cleanup()
}
