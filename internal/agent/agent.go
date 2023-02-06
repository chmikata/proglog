package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	api "github.com/chmikata/proglog/api/v1"
	"github.com/chmikata/proglog/internal/auth"
	"github.com/chmikata/proglog/internal/discovery"
	"github.com/chmikata/proglog/internal/log"
	"github.com/chmikata/proglog/internal/server"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Agent struct {
	Config

	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModeFile     string
	ACLPolicyFile   string
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		a.setupLogger,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

func (a *Agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(
		a.DataDir,
		log.Config{},
	)
	return err
}

func (a *Agent) setupServer() error {
	authorizer := auth.New(
		a.ACLModeFile,
		a.ACLPolicyFile,
	)
	serverConfig := &server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}
	var opts []grpc.ServerOption
	if a.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	ep, _ := stdouttrace.New(stdouttrace.WithPrettyPrint())
	tp := trace.NewTracerProvider(
		trace.WithBatcher(ep),
		trace.WithSampler(trace.AlwaysSample()),
	)
	var err error
	a.server, err = server.NewGRPCServer(serverConfig, tp, opts...)
	if err != nil {
		return err
	}
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	go func() {
		if err := a.server.Serve(ln); err != nil {
			// TODO
		}
	}()
	return err
}

func (a *Agent) setupMembership() error {
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}
	var opts []grpc.DialOption
	if a.PeerTLSConfig != nil {
		opts = append(opts, grpc.WithTransportCredentials(
			credentials.NewTLS(a.PeerTLSConfig),
		))
	}
	conn, err := grpc.Dial(rpcAddr, opts...)
	if err != nil {
		return err
	}
	client := api.NewLogClient(conn)
	a.replicator = &log.Replicator{
		DialOptions: opts,
		LocalServer: client,
	}
	a.membership, err = discovery.New(
		a.replicator,
		discovery.Config{
			NodeName: a.NodeName,
			BindAddr: a.BindAddr,
			Tags: map[string]string{
				"rpc_addr": rpcAddr,
			},
			StartJoinAddrs: a.StartJoinAddrs,
		},
	)
	return err
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)

	shutdown := []func() error{
		a.membership.Leave,
		a.replicator.Close,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
