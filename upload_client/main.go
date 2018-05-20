package main

import (
	"context"
	"crypto/tls"
	"flag"
	"io"
	"log"
	"os"
	"time"

	"github.com/childoftheuniverse/cstatic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	var clientCert string
	var clientKey string
	var rootCA string
	var serverAddr string
	var localPath string
	var remotePath string
	var serviceName string
	var failFast bool

	var tlsConfig *tls.Config
	var file *os.File
	var ctx context.Context
	var conn *grpc.ClientConn
	var client cstatic.UploadServiceClient
	var uploadClient cstatic.UploadService_UploadSingleFileClient
	var req cstatic.UploadSingleFileRequest
	var timeout time.Duration
	var opts []grpc.DialOption
	var cancel context.CancelFunc
	var buf []byte
	var err error

	flag.StringVar(&clientCert, "cert", "",
		"TLS client certificate. If unset, TLS is disabled.")
	flag.StringVar(&clientKey, "key", "",
		"TLS client private key. If unset, TLS is disabled.")
	flag.StringVar(&rootCA, "ca", "",
		"Root CA certificate to check against. If unset, system default is used.")
	flag.StringVar(&serverAddr, "server-addr", "",
		"Address of the upload server")
	flag.DurationVar(&timeout, "timeout", time.Minute,
		"Timeout for individual gRPC operations")
	flag.BoolVar(&failFast, "fail-fast", true,
		"Fail fast on connection errors rather than keep trying")

	flag.StringVar(&localPath, "local-path", "",
		"Path to the file to upload")
	flag.StringVar(&remotePath, "remote-path", "",
		"Path of the file on the server")
	flag.StringVar(&serviceName, "service-name", "",
		"Name of the website service to upload to")
	flag.Parse()

	if clientCert != "" && clientKey != "" {
		var cert tls.Certificate
		tlsConfig = new(tls.Config)

		if cert, err = tls.LoadX509KeyPair(clientCert, clientKey); err != nil {
			log.Fatal("Error loading X.509 client certificates: ", err)
		}

		tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
		opts = append(opts, grpc.WithTransportCredentials(
			credentials.NewTLS(tlsConfig)))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	opts = append(opts, grpc.WithTimeout(timeout), grpc.WithBlock())

	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if conn, err = grpc.Dial(serverAddr, opts...); err != nil {
		log.Fatal("Error connecting to ", serverAddr, ": ", err)
	}
	defer conn.Close()
	client = cstatic.NewUploadServiceClient(conn)

	uploadClient, err = client.UploadSingleFile(ctx, grpc.FailFast(failFast))
	if err != nil {
		log.Fatal("Error creating upload stream : ", err)
	}

	if file, err = os.Open(localPath); err != nil {
		log.Fatal("Unable to open ", localPath, ": ", err)
	}
	defer file.Close()

	req.WebsiteIdentifier = serviceName
	req.Path = remotePath
	buf = make([]byte, 1048576)

	for {
		var n int
		n, err = file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal("Error reading ", localPath, ": ", err)
		}

		req.Contents = buf[:n]

		if err = uploadClient.Send(&req); err != nil {
			if err == io.EOF {
				/* Get the actual error message. */
				_, err = uploadClient.CloseAndRecv()
			}
			cancel()
			log.Fatal("Error submitting upload request: ", err)
		}
	}

	if _, err = uploadClient.CloseAndRecv(); err != nil {
		log.Fatal("Error receiving upload confirmation: ", err)
	}
}
