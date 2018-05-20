package main

import (
	"context"
	"crypto/tls"
	"flag"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"github.com/childoftheuniverse/cstatic/config"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/gogo/protobuf/proto"
)

func main() {
	var localConfigPath string
	var remoteConfigPath string
	var etcdServerList string
	var etcdClientCert string
	var etcdClientKey string
	var etcdCA string
	var etcdTimeout time.Duration
	var etcdConfig etcd.Config

	var configBytes []byte
	var configProto config.CStaticConfig

	var client *etcd.Client
	var tlsInfo transport.TLSInfo
	var tlsConfig *tls.Config
	var ctx context.Context
	var cancel context.CancelFunc
	var err error

	flag.StringVar(&localConfigPath, "local-config", "",
		"Local path to config file")
	flag.StringVar(&remoteConfigPath, "remote-config", "",
		"Path to config file inside etcd")
	flag.StringVar(&etcdServerList, "etcd-servers", "",
		"Comma separated list of etcd servers to contact")
	flag.DurationVar(&etcdTimeout, "etcd-timeout", 2*time.Second,
		"Maximum amount of time to wait for etcd server connection")
	flag.StringVar(&etcdClientCert, "cert", "",
		"Path to TLS client certificate. If unset, TLS will not be used")
	flag.StringVar(&etcdClientKey, "key", "",
		"Path to TLS client private key. If unset, TLS will not be used")
	flag.StringVar(&etcdCA, "ca", "",
		"Path to TLS CA certificate. If unset, the system default will be used")
	flag.Parse()

	etcdConfig.Endpoints = strings.Split(etcdServerList, ",")
	etcdConfig.DialTimeout = etcdTimeout

	if etcdClientCert != "" && etcdClientKey != "" {
		tlsInfo.CertFile = etcdClientCert
		tlsInfo.KeyFile = etcdClientKey
	}
	if etcdCA != "" {
		tlsInfo.TrustedCAFile = etcdCA
	}
	if (etcdClientCert != "" && etcdClientKey != "") || etcdCA != "" {
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

	if configBytes, err = ioutil.ReadFile(localConfigPath); err != nil {
		log.Fatal("Unable to read local configuration ", localConfigPath, ": ", err)
	}
	if err = proto.UnmarshalText(string(configBytes), &configProto); err != nil {
		log.Fatal("Unable to parse config proto ", localConfigPath, ": ", err)
	}
	if configBytes, err = proto.Marshal(&configProto); err != nil {
		log.Fatal("Unable to marshal binary proto from ", localConfigPath, ": ",
			err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), etcdTimeout)
	_, err = client.Put(ctx, remoteConfigPath, string(configBytes))
	if err != nil {
		cancel()
		log.Fatal("Error putting ", remoteConfigPath, ": ", err)
	}
	cancel()
}
