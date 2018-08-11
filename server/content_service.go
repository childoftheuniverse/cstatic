package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/childoftheuniverse/cstatic/config"
	"github.com/childoftheuniverse/filesystem"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/gocql/gocql"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var isInitialized = prometheus.NewGauge(prometheus.GaugeOpts{
	Subsystem: "contentservice",
	Name:      "initialized",
	Help:      "Whether the service is initialized.",
})
var lastConfigReceived = prometheus.NewGauge(prometheus.GaugeOpts{
	Subsystem: "contentservice",
	Name:      "last_config_received",
	Help:      "Timestamp at which the most recent configuration was read.",
})
var configIsValid = prometheus.NewGauge(prometheus.GaugeOpts{
	Subsystem: "contentservice",
	Name:      "config_is_valid",
	Help:      "Whether the configuration last read from etcd is valid.",
})
var requestForUnknownSite = prometheus.NewCounter(prometheus.CounterOpts{
	Subsystem: "contentservice",
	Name:      "request_for_unknown_site",
	Help:      "Number of requests received for a site that is not configured.",
})
var requestLatencies = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "contentservice",
	Name:      "request_latency",
	Help:      "Latency of upload requests",
	Buckets:   prometheus.ExponentialBuckets(0.001, 2, 50),
}, []string{"rpc_command"})
var cassandraLatencies = prometheus.NewHistogram(prometheus.HistogramOpts{
	Subsystem: "contentservice",
	Name:      "cassandra_latency",
	Help:      "Latency of Cassandra requests",
	Buckets:   prometheus.ExponentialBuckets(0.001, 2, 50),
})
var cassandraRetries = prometheus.NewHistogram(prometheus.HistogramOpts{
	Subsystem: "contentservice",
	Name:      "cassandra_retries",
	Help:      "Number of retries of Cassandra requests",
	Buckets:   prometheus.LinearBuckets(1.0, 1.0, 50),
})
var backendReadsPerRequest = prometheus.NewHistogram(prometheus.HistogramOpts{
	Subsystem: "contentservice",
	Name:      "backend_reads_per_request",
	Help:      "Number of reads performed per request",
	Buckets:   prometheus.LinearBuckets(1.0, 1.0, 50),
})
var bytesPerRead = prometheus.NewHistogram(prometheus.HistogramOpts{
	Subsystem: "contentservice",
	Name:      "backend_bytes_per_read",
	Help:      "Number of bytes returned per read request",
	Buckets:   prometheus.ExponentialBuckets(1.0, 1.5, 50),
})

func init() {
	prometheus.MustRegister(isInitialized)
	prometheus.MustRegister(lastConfigReceived)
	prometheus.MustRegister(configIsValid)
	prometheus.MustRegister(requestForUnknownSite)
	prometheus.MustRegister(cassandraLatencies)
	prometheus.MustRegister(cassandraRetries)
	prometheus.MustRegister(backendReadsPerRequest)
	prometheus.MustRegister(bytesPerRead)
}

func logError(prefix string, err error) {
	if err != nil {
		log.Printf("%s: %v", prefix, err)
	}
}

/*
ContentService is an HTTP handler which looks up the relevant static files and
serves them to the client.
*/
type ContentService struct {
	/* Listener configuration */
	mux       *http.ServeMux
	tlsConfig *tls.Config

	/* Config settings */
	vhostSiteMap     map[string]string
	cassandraServers []string
	settingsLock     sync.RWMutex

	/* Cassandra connection */
	cluster *gocql.ClusterConfig
	session *gocql.Session

	/* Internal status */
	initialized           bool
	configWatchCancelFunc context.CancelFunc
	configWatcherRunning  sync.Mutex
}

/*
Cleanup cancels all config watch operations currently in progress. It should be
invoked in response to termination signals being received.
*/
func (service *ContentService) Cleanup() {
	if service.configWatchCancelFunc != nil {
		service.configWatchCancelFunc()
	}
	service.initialized = false // We just stopped listening for config updates.
}

/*
WatchForConfigChanges watches the configured etcd location for updates and
invokes ServingConfig as required.

This function should only ever be called once, during initialization, and will
continue running in the background. Parallel invocations of this fuction on
the same ContentService will block until the previous incarnation has exited.
*/
func (service *ContentService) WatchForConfigChanges(
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
		service.ServingConfig(kv.Value)
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
			service.ServingConfig(ev.Kv.Value)
		}
	}
}

/*
ServingConfig receives a new configuration protocol buffer in binary format and
attempts to parse and enact it. If there were problems applying the new
configuration, the old configuration will be used.
*/
func (service *ContentService) ServingConfig(contents []byte) {
	var cluster *gocql.ClusterConfig
	var session *gocql.Session
	var configProto config.CStaticConfig
	var site *config.WebsiteConfig
	var cassandraUpdated bool

	var vhostSiteMap = make(map[string]string)
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
		session.SetConsistency(gocql.One)
		cassandraUpdated = true
	}

	/* Update internal state on websites */
	for _, site = range configProto.SiteConfigs {
		var vhost string
		for _, vhost = range site.VhostName {
			vhostSiteMap[vhost] = site.Identifier
		}
	}

	/* Apply new configuration */
	service.settingsLock.Lock()
	service.vhostSiteMap = vhostSiteMap
	if cassandraUpdated {
		service.cassandraServers = configProto.CassandraServer
		service.cluster = cluster
		service.session = session
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
func (service *ContentService) IsHealthy() bool {
	return service.initialized && !service.session.Closed()
}

/*
MainLoop creates the HTTP listener and starts listening to connections.
The function only exits if a listener could not be created, or once the listener
has shut down.
*/
func (service *ContentService) MainLoop(listenAddr string) error {
	var err error
	service.mux = http.NewServeMux()

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

	service.mux.Handle("/metrics", promhttp.Handler())
	service.mux.Handle("/", service)

	return http.ListenAndServe(listenAddr, service.mux)
}

/*
ServeHTTP retrieves the requested file from the static config store and
returns it to the requestor.
*/
func (service *ContentService) ServeHTTP(
	w http.ResponseWriter, r *http.Request) {
	var startTime = time.Now()
	defer requestLatencies.With(prometheus.Labels{
		"rpc_command": "UploadSingleFile"}).Observe(
		time.Now().Sub(startTime).Seconds())

	var location string
	var contentType string
	var offset int64
	var length int64
	var myURL *url.URL
	var contentURL *url.URL
	var parentContext = r.Context()
	var reader filesystem.ReadCloser
	var limitedReader filesystem.ReadCloser
	var seeker filesystem.Seeker
	var siteID string
	var query *gocql.Query
	var buf = make([]byte, 1<<10)
	var numReadsRequired uint64
	var hasWritten bool
	var ok bool
	var err error

	service.settingsLock.RLock()
	defer service.settingsLock.RUnlock()

	/* Before we do anything, check that this RPC is still relevant. */
	if parentContext.Err() != nil {
		/* TODO: increment a counter with the category of error */
		return
	}

	if siteID, ok = service.vhostSiteMap[r.Host]; !ok {
		requestForUnknownSite.Inc()
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte("No vhost configured for this URL"))
		return
	}

	if myURL, err = url.Parse("http://" + siteID + r.RequestURI); err != nil {
		log.Print("Unable to parse original request URI: ", r.RequestURI)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error parsing request URI"))
		return
	}

	query = service.session.Query(
		"SELECT content_type, file_location, offset, length FROM "+
			"file_contents WHERE site = ? AND path = ?",
		siteID, myURL.Path).WithContext(parentContext)
	defer query.Release()
	if err = query.Scan(&contentType, &location, &offset, &length); err != nil {
		log.Print("Unable to scan for ", myURL.Path, ": ", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error fetching content metadata"))
		return
	}

	cassandraLatencies.Observe(
		(time.Duration(query.Latency()) * time.Nanosecond).Seconds())
	cassandraRetries.Observe(float64(query.Attempts()))

	if contentURL, err = url.Parse(location); err != nil {
		log.Print("Unable to parse location for ", location, ": ", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error processing content metadata"))
		return
	}

	if reader, err = filesystem.OpenReader(parentContext, contentURL); err != nil {
		log.Print("Unable to open ", contentURL.String(), ": ", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error processing content metadata"))
		return
	}
	defer logError(fmt.Sprintf("Error closing reader for %s",
		contentURL.String()), reader.Close(parentContext))

	if seeker, ok = reader.(filesystem.Seeker); !ok {
		log.Print("Filesystem type for ", contentURL.String(),
			" does not support seeking")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Backend not supported"))
		return
	}

	if _, err = seeker.Seek(parentContext, offset, io.SeekStart); err != nil {
		log.Print("Error seeking in ", contentURL.String(), ": ", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Read error"))
		return
	}

	limitedReader = &filesystem.LimitedReadCloser{R: reader, N: length}

	for {
		var readLength int

		if parentContext.Err() != nil {
			/* TODO: increment a counter with the category of error */
			break
		}

		numReadsRequired++
		readLength, err = limitedReader.Read(parentContext, buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Print("Error reading from ", contentURL.String(), ": ", err)
			if !hasWritten {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Read error"))
			}
			break
		}

		bytesPerRead.Observe(float64(readLength))

		if !hasWritten {
			w.Header().Set("Content-Type", contentType)
			w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
			w.WriteHeader(http.StatusOK)
			hasWritten = true
		}

		if _, err = w.Write(buf[:readLength]); err != nil {
			log.Print("Unable to write client response: ", err)
		}
	}

	backendReadsPerRequest.Observe(float64(numReadsRequired))
}
