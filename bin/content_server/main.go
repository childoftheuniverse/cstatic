package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"contrib.go.opencensus.io/exporter/prometheus"
	"contrib.go.opencensus.io/exporter/zipkin"
	"github.com/childoftheuniverse/cstatic/server"
	_ "github.com/childoftheuniverse/filesystem-file"
	rados "github.com/childoftheuniverse/filesystem-rados"
	openzipkin "github.com/openzipkin/zipkin-go"
	openzipkinModel "github.com/openzipkin/zipkin-go/model"
	zipkinReporter "github.com/openzipkin/zipkin-go/reporter"
	zipkinHTTP "github.com/openzipkin/zipkin-go/reporter/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	etcd "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
)

/*
Stop etcd listener when receiving SIGINT or SIGTERM.
*/
func handleSignals(
	contentService *server.ContentService, sigChannel chan os.Signal) {
	for {
		var s = <-sigChannel
		switch s {
		case syscall.SIGINT:
			fallthrough
		case syscall.SIGTERM:
			contentService.Cleanup()
			os.Exit(1)
		}
	}
}

func main() {
	var configPath string
	var bindAddress string
	var etcdServerList string
	var etcdTimeout time.Duration
	var etcdConfig etcd.Config
	var zipkinEndpoint string
	var thisHost string
	var traceProbability float64

	var privateKeyPath string
	var publicKeyPath string
	var serverCA string

	var contentService server.ContentService
	var sigChannel = make(chan os.Signal)
	var client *etcd.Client
	var tlsConfig *tls.Config
	var tlsInfo transport.TLSInfo
	var pe *prometheus.Exporter
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

	/* TLS configuration */
	flag.StringVar(&privateKeyPath, "tls-key", "",
		"TLS private key of server certificate. If unset, TLS will not be used.")
	flag.StringVar(&publicKeyPath, "tls-cert", "",
		"TLS server certificate. If unset, TLS will not be used.")
	flag.StringVar(&serverCA, "server-ca", "",
		"Path to a server CA certificate file. If unset, servers will not be "+
			"authenticated.")
	flag.Parse()

	if err = view.Register(ocgrpc.DefaultClientViews...); err != nil {
		log.Fatal("Error registering default gRPC client views: ", err)
	}
	if err = view.Register(ocgrpc.DefaultServerViews...); err != nil {
		log.Fatal("Error registering default gRPC server views: ", err)
	}

	if pe, err = prometheus.NewExporter(prometheus.Options{
		Namespace: "cstatic_content_server",
	}); pe != nil {
		log.Fatal("Error creating prometheus exporter: ", err)
	}
	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/metrics-grpc", pe)

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
	go handleSignals(&contentService, sigChannel)

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
	contentService.Cleanup()
}
