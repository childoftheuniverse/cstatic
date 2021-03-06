package upload

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

	prom_exporter "contrib.go.opencensus.io/exporter/prometheus"
	"github.com/childoftheuniverse/cstatic"
	"github.com/childoftheuniverse/cstatic/config"
	"github.com/childoftheuniverse/filesystem"
	"github.com/gocql/gocql"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	etcd "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.opencensus.io/trace"
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
	initialized           bool
	configWatchCancelFunc context.CancelFunc
	configWatcherRunning  sync.Mutex
}

/*
Cleanup cancels all config watch operations currently in progress, closes all
listeners and initiates an os.Exit call. It should be invoked in response to
termination signals being received.
*/
func (service *UploadService) Cleanup() {
	if service.configWatchCancelFunc != nil {
		service.configWatchCancelFunc()
	}
	service.initialized = false // We just stopped listening for config updates.
	if service.listener != nil {
		service.listener.Close()
	}
	if service.rpcListener != nil && service.rpcListener != service.listener {
		service.rpcListener.Close()
	}
}

/*
WatchForConfigChanges watches the configured etcd location for updates and
invokes UploadConfig as required.

This function should only ever be called once, during initialization, and will
continue running in the background. Parallel invocations of this fuction on
the same ContentService will block until the previous incarnation has exited.
*/
func (service *UploadService) WatchForConfigChanges(
	client *etcd.Client, etcdTimeout time.Duration, configPath string) {
	var configUpdateChannel etcd.WatchChan
	var configUpdate etcd.WatchResponse
	var getResp *etcd.GetResponse
	var kv *mvccpb.KeyValue
	var rev int64
	var ctx context.Context
	var err error

	service.configWatcherRunning.Lock()
	defer service.configWatcherRunning.Unlock()

	ctx, service.configWatchCancelFunc =
		context.WithTimeout(context.Background(), etcdTimeout)

	getResp, err = client.Get(ctx, configPath)
	service.configWatchCancelFunc()
	if err != nil {
		log.Print("Error reading configuration from ", configPath, ": ", err)
		service.Cleanup()
		return
	}
	if len(getResp.Kvs) < 1 {
		log.Print("Cannot find current version of ", configPath)
	}
	for _, kv = range getResp.Kvs {
		rev = kv.ModRevision
		service.UploadConfig(kv.Value)
	}

	ctx, service.configWatchCancelFunc =
		context.WithCancel(context.Background())
	defer service.configWatchCancelFunc()

	configUpdateChannel = client.Watch(
		ctx, configPath, etcd.WithCreatedNotify(),
		etcd.WithRev(rev+1))
	for configUpdate = range configUpdateChannel {
		var ev *etcd.Event
		for _, ev = range configUpdate.Events {
			service.UploadConfig(ev.Kv.Value)
		}
	}
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
	var pe *prom_exporter.Exporter
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

	if pe, err = prom_exporter.NewExporter(prom_exporter.Options{
		Namespace: "cstatic_content_server",
	}); pe != nil {
		return fmt.Errorf("Error creating prometheus exporter: %s", err)
	}
	service.mux.Handle("/metrics", promhttp.Handler())
	service.mux.Handle("/metrics-grpc", pe)

	service.grpcServer = grpc.NewServer(opts...)
	cstatic.RegisterUploadServiceServer(service.grpcServer, service)
	reflection.Register(service.grpcServer)

	if rpcListener != nil {
		go service.grpcServe(rpcListener)
	}
	log.Print("Listening for connections on ", listener.Addr())
	log.Print("Health checks: ", proto, "://", listener.Addr(), "/health")
	log.Print("Metrics: ", proto, "://", listener.Addr(), "/metrics")
	return http.Serve(listener, service)
}

/*
Start listening for gRPC connections on the specified listener.
*/
func (service *UploadService) grpcServe(listener net.Listener) {
	var err error
	log.Print("Listening for RPCs on ", listener.Addr())
	if err = service.grpcServer.Serve(listener); err != nil {
		service.configWatchCancelFunc()
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

	var parentContext = stream.Context()
	var ctx context.Context
	var span *trace.Span

	ctx, span = trace.StartSpan(parentContext, "UploadService.UploadSingleFile")
	defer span.End()

	service.settingsLock.RLock()
	defer service.settingsLock.RUnlock()
	service.currentFileLock.Lock()
	defer service.currentFileLock.Unlock()

	var query *gocql.Query
	var siteID string
	var path string
	var contentType string
	var offset = service.currentFileSize
	var length uint64
	var err error

	numRequests.With(prometheus.Labels{"rpc_command": "UploadSingleFile"}).Inc()

	/* Before we do anything, check that this RPC is still relevant. */
	if parentContext.Err() != nil {
		span.Annotate(
			[]trace.Attribute{
				trace.StringAttribute("error", parentContext.Err().Error()),
			}, "Context expired")
		return parentContext.Err()
	}

	span.AddAttributes(
		trace.BoolAttribute("healthy", service.IsHealthy()))

	if !service.IsHealthy() {
		if err = stream.SendAndClose(
			&cstatic.UploadSingleFileResponse{}); err != nil {
			log.Print("Error closing upload stream for error reporting: ", err)
		}
		requestsLostDueToUnhealthy.Inc()
		span.Annotate(nil, "Server not healthy")
		return status.Error(codes.Unavailable, "Server is not healthy")
	}

	for {
		var writeTrace *trace.Span
		var writeCtx context.Context
		var req *cstatic.UploadSingleFileRequest
		var readLength int

		writeCtx, writeTrace = trace.StartSpan(
			ctx, "UploadService.UploadSingleFile.Write")
		defer writeTrace.End()

		req, err = stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			writeTrace.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
			}, "Error receiving write request")
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
				writeTrace.Annotate([]trace.Attribute{
					trace.StringAttribute("site-id", req.WebsiteIdentifier),
				}, "Unknown website identifier")
				return status.Error(codes.NotFound, "Site not configured")
			}

			siteID = req.WebsiteIdentifier
		}
		if len(req.ContentType) > 0 {
			contentType = req.ContentType
		}

		readLength, err = service.currentFile.Write(writeCtx, req.Contents)
		service.currentFileSize += uint64(readLength)
		if err != nil {
			writeTrace.Annotate([]trace.Attribute{
				trace.StringAttribute("error", err.Error()),
			}, "Error writing data")
			return err
		}

		length += uint64(readLength)

		if parentContext.Err() != nil {
			span.Annotate(
				[]trace.Attribute{
					trace.StringAttribute("error",
						parentContext.Err().Error()),
				}, "Context expired")
			return parentContext.Err()
		}
	}

	span.AddAttributes(
		trace.StringAttribute("site-id", siteID),
		trace.StringAttribute("request-uri", path),
		trace.StringAttribute("content-url",
			service.currentFilePath.String()),
		trace.StringAttribute("content-type", contentType),
		trace.Int64Attribute("content-length", int64(length)))

	span.Annotate(nil, "Updating Cassandra metadata")

	query = service.session.Query(
		"UPDATE file_contents SET content_type = ?, file_location = ?, "+
			"offset = ?, length = ?, modified = ? WHERE site = ? AND path = ?",
		contentType, service.currentFilePath.String(), offset, length,
		time.Now(), siteID, path)
	defer query.Release()
	if err = query.WithContext(ctx).Exec(); err != nil {
		return err
	}

	cassandraLatencies.Observe(
		(time.Duration(query.Latency()) * time.Nanosecond).Seconds())
	cassandraRetries.Observe(float64(query.Attempts()))

	if err = stream.SendAndClose(
		&cstatic.UploadSingleFileResponse{}); err != nil {
		span.Annotate([]trace.Attribute{
			trace.StringAttribute("error", err.Error()),
		}, "Error sending result")
		return err
	}

	requestSizes.With(prometheus.Labels{
		"rpc_command": "UploadSingleFile"}).Observe(float64(length))

	return nil
}
