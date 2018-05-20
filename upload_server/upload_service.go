package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/childoftheuniverse/cstatic"
	"github.com/childoftheuniverse/cstatic/config"
	"github.com/childoftheuniverse/filesystem"
	"github.com/gocql/gocql"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

var isInitialized = prometheus.NewGauge(prometheus.GaugeOpts{
	Subsystem: "uploadservice",
	Name:      "initialized",
	Help:      "Whether the service is initialized.",
})
var lastConfigReceived = prometheus.NewGauge(prometheus.GaugeOpts{
	Subsystem: "uploadservice",
	Name:      "last_config_received",
	Help:      "Timestamp at which the most recent configuration was read.",
})
var configIsValid = prometheus.NewGauge(prometheus.GaugeOpts{
	Subsystem: "uploadservice",
	Name:      "config_is_valid",
	Help:      "Whether the configuration last read from etcd is valid.",
})
var requestsLostDueToUnhealthy = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "uploadservice",
	Name:      "requests_lost_due_to_unhealthy",
	Help:      "Number of requests lost because we were unhealthy",
})
var numRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "uploadservice",
	Name:      "num_requests",
	Help:      "Number of upload requests received",
}, []string{"rpc_command"})
var requestSizes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "uploadservice",
	Name:      "request_size",
	Help:      "Size of the uploads received",
	Buckets:   prometheus.ExponentialBuckets(100, 1.25, 20),
}, []string{"rpc_command"})
var requestLatencies = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "uploadservice",
	Name:      "request_latency",
	Help:      "Latency of upload requests",
	Buckets:   prometheus.ExponentialBuckets(0.001, 5, 20),
}, []string{"rpc_command"})
var cassandraLatencies = prometheus.NewHistogram(prometheus.HistogramOpts{
	Subsystem: "uploadservice",
	Name:      "cassandra_latency",
	Help:      "Latency of Cassandra requests",
	Buckets:   prometheus.ExponentialBuckets(0.001, 5, 20),
})
var cassandraRetries = prometheus.NewHistogram(prometheus.HistogramOpts{
	Subsystem: "uploadservice",
	Name:      "cassandra_retries",
	Help:      "Number of retries of Cassandra requests",
	Buckets:   prometheus.LinearBuckets(1.0, 1.0, 20),
})

/*
UploadService implements the upload functionality for cstatic.
*/
type UploadService struct {
	/* Listener configuration */
	listener    net.Listener
	rpcListener net.Listener
	tlsConfig   *tls.Config
	mux         *http.ServeMux
	grpcServer  *grpc.Server

	/* Config settings */
	knownSiteIDs     []string
	uploadPath       *url.URL
	cassandraServers []string
	settingsLock     sync.RWMutex

	/* Cassandra connection */
	cluster *gocql.ClusterConfig
	session *gocql.Session

	/* Writer and metadata */
	currentFile     filesystem.WriteCloser
	currentFileLock sync.Mutex
	currentFilePath *url.URL
	currentFileSize uint64

	/* Internal status */
	initialized bool
}

/*
UploadConfig receives a new service configuration from etcd (via handler)
and sets the internal data structures so we can start serving requests
with the new configuration.
*/
func (service *UploadService) UploadConfig(contents []byte) {
	var cluster *gocql.ClusterConfig
	var session *gocql.Session
	var configProto config.CStaticConfig
	var site *config.WebsiteConfig
	var cassandraUpdated bool

	var knownSiteIDs = make([]string, 0)
	var currentFile filesystem.WriteCloser
	var currentFilePath *url.URL
	var uploadPath *url.URL
	var err error

	lastConfigReceived.Set(float64(time.Now().Unix()))

	err = proto.Unmarshal(contents, &configProto)
	if err != nil {
		log.Print("Unable to parse received configuration: ", err)
		return
	}

	/* Check whether we need to update our Cassandra connection. */
	if len(configProto.CassandraServer) == 0 {
		log.Print("No Cassandra servers specified in configuration.")
		configIsValid.Set(0.0)
		return
	}
	if !reflect.DeepEqual(service.cassandraServers, configProto.CassandraServer) {
		cluster = gocql.NewCluster(
			configProto.CassandraServer...)
		cluster.Keyspace = configProto.Keyspace

		session, err = cluster.CreateSession()
		if err != nil {
			log.Print("Unable to connect to Cassandra servers ",
				configProto.CassandraServer, ": ", err)
			return
		}
		session.SetConsistency(gocql.Quorum)
		cassandraUpdated = true
	}

	uploadPath, err = url.Parse(configProto.UploadPath)
	if err != nil {
		log.Print("Unparseable upload path: ", configProto.UploadPath, ": ", err)
		return
	}

	/* Update internal state on websites */
	for _, site = range configProto.SiteConfigs {
		knownSiteIDs = append(knownSiteIDs, site.Identifier)
	}

	if service.uploadPath == nil || service.uploadPath.String() != uploadPath.String() {
		var now = time.Now()
		var ctx context.Context
		var cancelOpen context.CancelFunc

		ctx, cancelOpen = context.WithTimeout(context.Background(),
			time.Duration(configProto.UploadOpenTimeout)*time.Millisecond)
		defer cancelOpen()

		currentFilePath = new(url.URL)
		*currentFilePath = *uploadPath
		currentFilePath.Path = fmt.Sprintf("%s.%d", uploadPath.Path, now.UnixNano())

		currentFile, err = filesystem.OpenWriter(ctx, currentFilePath)
		if err != nil {
			log.Print("Error opening ", currentFilePath, " for writing: ", err)
			return
		}
	}

	/* Apply new configuration */
	service.settingsLock.Lock()
	service.knownSiteIDs = knownSiteIDs
	service.uploadPath = uploadPath
	if cassandraUpdated {
		service.cassandraServers = configProto.CassandraServer
		service.cluster = cluster
		service.session = session
	}

	if currentFilePath != nil {
		service.currentFile = currentFile
		service.currentFilePath = currentFilePath
		service.currentFileSize = 0
	}
	service.initialized = true
	service.settingsLock.Unlock()

	/* Declare success. */
	isInitialized.Set(1.0)
	configIsValid.Set(1.0)
}

/*
IsHealthy determines whether the service is configured and ready to handle
requests. If IsHealthy is false, any requests should be rejected.
*/
func (service *UploadService) IsHealthy() bool {
	return service.initialized && !service.session.Closed()
}

/*
MainLoop starts the HTTP server, registers the RPC handlers and serves any
incoming requests.
*/
func (service *UploadService) MainLoop(
	listener net.Listener, rpcListener net.Listener,
	tlsConfig *tls.Config) error {
	var opts []grpc.ServerOption
	var proto = "http"
	var err error

	if tlsConfig != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
		service.tlsConfig = tlsConfig
		proto = "https"
	}

	service.mux = http.NewServeMux()
	service.listener = listener
	service.rpcListener = rpcListener
	service.knownSiteIDs = make([]string, 0)
	service.cassandraServers = make([]string, 0)

	/* health check handler */
	service.mux.HandleFunc("/health",
		func(w http.ResponseWriter, r *http.Request) {
			if service.IsHealthy() {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte{'O', 'K'})
			} else {
				w.WriteHeader(http.StatusExpectationFailed)
				w.Write([]byte{'B', 'A', 'D'})
			}
		})

	/* Prometheus metrics */
	err = prometheus.Register(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Subsystem: "uploadservice",
			Name:      "healthy",
			Help:      "Whether the service is currently healthy.",
		},
		func() float64 {
			if service.IsHealthy() {
				return 1.0
			}
			return 0.0
		},
	))
	if err != nil {
		return err
	}
	if err = prometheus.Register(isInitialized); err != nil {
		return err
	}
	if err = prometheus.Register(configIsValid); err != nil {
		return err
	}
	if err = prometheus.Register(lastConfigReceived); err != nil {
		return err
	}
	if err = prometheus.Register(requestsLostDueToUnhealthy); err != nil {
		return err
	}
	if err = prometheus.Register(numRequests); err != nil {
		return err
	}
	if err = prometheus.Register(requestSizes); err != nil {
		return err
	}
	if err = prometheus.Register(requestLatencies); err != nil {
		return err
	}
	if err = prometheus.Register(cassandraLatencies); err != nil {
		return err
	}
	if err = prometheus.Register(cassandraRetries); err != nil {
		return err
	}
	service.mux.Handle("/metrics", promhttp.Handler())

	service.grpcServer = grpc.NewServer(opts...)
	cstatic.RegisterUploadServiceServer(service.grpcServer, service)
	reflection.Register(service.grpcServer)

	if rpcListener != nil {
		log.Print("Listening for RPCs on ", rpcListener.Addr())
		go service.grpcServe(rpcListener)
	}
	log.Print("Listening for connections on ", listener.Addr())
	log.Print("Health checks: ", proto, "://", listener.Addr(), "/health")
	log.Print("Metrics: ", proto, "://", listener.Addr(), "/metrics")
	return http.Serve(listener, service)
	// return grpcServer.Serve(listener)
}

/*
Start listening for gRPC connections on the specified listener.
*/
func (service *UploadService) grpcServe(listener net.Listener) {
	var err error
	if err = service.grpcServer.Serve(listener); err != nil {
		cancel()
		var err2 error
		if err2 = service.listener.Close(); err2 != nil {
			log.Print("Error shutting down service listener: ", err2)
		}
		log.Fatal("Error serving connections on ", listener.Addr(), ": ", err)
	}
}

/*
ServeHTTP proxies the incoming HTTP request onto the correct handler for gRPC,
Prometheus or health checks.
*/
func (service *UploadService) ServeHTTP(
	w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor == 2 && (strings.HasPrefix(r.Header.Get("Content-Type"),
		"application/grpc") || r.RequestURI == "*") {
		service.grpcServer.ServeHTTP(w, r)
	} else {
		service.mux.ServeHTTP(w, r)
	}
}

/*
UploadSingleFile uploads a single file to the store of the specified website.
*/
func (service *UploadService) UploadSingleFile(
	stream cstatic.UploadService_UploadSingleFileServer) error {
	var startTime = time.Now()
	defer requestLatencies.With(prometheus.Labels{
		"rpc_command": "UploadSingleFile"}).Observe(
		time.Now().Sub(startTime).Seconds())

	service.settingsLock.RLock()
	defer service.settingsLock.RUnlock()
	service.currentFileLock.Lock()
	defer service.currentFileLock.Unlock()

	var parentContext = stream.Context()
	var query *gocql.Query
	var siteID string
	var path string
	var offset = service.currentFileSize
	var length uint64
	var err error

	numRequests.With(prometheus.Labels{"rpc_command": "UploadSingleFile"}).Inc()

	/* Before we do anything, check that this RPC is still relevant. */
	if parentContext.Err() != nil {
		return parentContext.Err()
	}

	if !service.IsHealthy() {
		if err = stream.SendAndClose(
			&cstatic.UploadSingleFileResponse{}); err != nil {
			log.Print("Error closing upload stream for error reporting: ", err)
		}
		requestsLostDueToUnhealthy.Inc()
		return status.Error(codes.Unavailable, "Server is not healthy")
	}

	for {
		var req *cstatic.UploadSingleFileRequest
		var readLength int

		req, err = stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if len(req.Path) > 0 {
			path = req.Path
		}
		if len(req.WebsiteIdentifier) > 0 {
			var id string
			var found bool

			for _, id = range service.knownSiteIDs {
				if id == req.WebsiteIdentifier {
					found = true
					break
				}
			}

			if !found {
				return status.Error(codes.NotFound, "Site not configured")
			}

			siteID = req.WebsiteIdentifier
		}

		readLength, err = service.currentFile.Write(parentContext, req.Contents)
		service.currentFileSize += uint64(readLength)
		if err != nil {
			return err
		}

		length += uint64(readLength)

		if parentContext.Err() != nil {
			return parentContext.Err()
		}
	}

	query = service.session.Query(
		"UPDATE file_contents SET file_location = ?, offset = ?, length = ?, "+
			"modified = ? WHERE site = ? AND path = ?",
		service.currentFilePath.String(), offset, length, time.Now(), siteID, path)
	if err = query.Exec(); err != nil {
		return err
	}

	cassandraLatencies.Observe(
		(time.Duration(query.Latency()) * time.Nanosecond).Seconds())
	cassandraRetries.Observe(float64(query.Attempts()))

	if err = stream.SendAndClose(
		&cstatic.UploadSingleFileResponse{}); err != nil {
		return err
	}

	requestSizes.With(prometheus.Labels{
		"rpc_command": "UploadSingleFile"}).Observe(float64(length))

	return nil
}
