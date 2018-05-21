package main

import (
	"crypto/tls"
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
	"github.com/gocql/gocql"
	"github.com/gogo/protobuf/proto"
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

func init() {
	prometheus.MustRegister(isInitialized)
	prometheus.MustRegister(lastConfigReceived)
	prometheus.MustRegister(configIsValid)
	prometheus.MustRegister(requestForUnknownSite)
	prometheus.MustRegister(cassandraLatencies)
	prometheus.MustRegister(cassandraRetries)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
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
	initialized bool
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
	var offset int64
	var length int64
	var myURL *url.URL
	var contentURL *url.URL
	var parentContext = r.Context()
	var reader filesystem.ReadCloser
	var seeker filesystem.Seeker
	var siteID string
	var query *gocql.Query
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
		"SELECT file_location, offset, length FROM file_contents "+
			"WHERE site = ? AND path = ?", siteID, myURL.Path)
	defer query.Release()
	if err = query.Scan(&location, &offset, &length); err != nil {
		log.Print("Unable to scan for ", myURL.Path, ": ", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error fetching content metadata"))
		return
	}

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

	for length > 0 {
		var buf = make([]byte, maxInt64(length, 1<<10))
		var readLength int
		if parentContext.Err() != nil {
			/* TODO: increment a counter with the category of error */
			return
		}

		if readLength, err = reader.Read(parentContext, buf); err != nil {
			if err == io.EOF {
				break
			}
			log.Print("Error reading from ", contentURL.String(), ": ", err)
			if !hasWritten {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Read error"))
			}
			return
		}

		if !hasWritten {
			w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
			w.WriteHeader(http.StatusOK)
			hasWritten = true
		}

		if _, err = w.Write(buf[:readLength]); err != nil {
			log.Print("Unable to write client response: ", err)
		}

		length -= int64(readLength)
	}

	cassandraLatencies.Observe(
		(time.Duration(query.Latency()) * time.Nanosecond).Seconds())
	cassandraRetries.Observe(float64(query.Attempts()))
}
