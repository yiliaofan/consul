package consul

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/agent/metadata"
	"github.com/hashicorp/consul/agent/pool"
	"github.com/hashicorp/consul/tlsutil"
	"google.golang.org/grpc"
)

const (
	grpcBasePath = "/consul"
)

func dialGRPC(addr string, _ time.Duration) (net.Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	_, err = conn.Write([]byte{pool.RPCGRPC})
	if err != nil {
		return nil, err
	}

	return conn, nil
}

type connSlot struct {
	conn *grpc.ClientConn
	err  error
	done chan struct{}
}

type RPCClient struct {
	rpcPool       *pool.ConnPool
	grpcConns     map[string]*connSlot
	grpcConnsLock sync.RWMutex
	logger        *log.Logger
}

func NewRPCClient(logger *log.Logger, config *Config, tlsConfigurator *tlsutil.Configurator, maxConns int, maxIdleTime time.Duration) *RPCClient {
	return &RPCClient{
		rpcPool: &pool.ConnPool{
			SrcAddr:    config.RPCSrcAddr,
			LogOutput:  config.LogOutput,
			MaxTime:    maxIdleTime,
			MaxStreams: maxConns,
			TLSWrapper: tlsConfigurator.OutgoingRPCWrapper(),
			ForceTLS:   config.VerifyOutgoing,
		},
		grpcConns: make(map[string]*connSlot),
		logger:    logger,
	}
}

func (c *RPCClient) Call(dc string, server *metadata.Server, method string, args, reply interface{}) error {
	if !server.GRPCEnabled || !grpcAbleEndpoints[method] {
		c.logger.Printf("[TRACE] Using RPC for method %s", method)
		return c.rpcPool.RPC(dc, server.Addr, server.Version, method, server.UseTLS, args, reply)
	}

	conn, err := c.grpcConn(server)
	if err != nil {
		return err
	}

	c.logger.Printf("[TRACE] Using GRPC for method %s", method)
	return conn.Invoke(context.Background(), c.grpcPath(method), args, reply)
}

func (c *RPCClient) Ping(dc string, addr net.Addr, version int, useTLS bool) (bool, error) {
	return c.rpcPool.Ping(dc, addr, version, useTLS)
}

func (c *RPCClient) Shutdown() error {
	// Close the connection pool
	c.rpcPool.Shutdown()
	return nil
}

func (c *RPCClient) grpcConn(server *metadata.Server) (*grpc.ClientConn, error) {
	host, _, _ := net.SplitHostPort(server.Addr.String())
	addr := fmt.Sprintf("%s:%d", host, server.Port)

	var isFirstRequest bool

	c.grpcConnsLock.Lock()
	existing, ok := c.grpcConns[addr]
	if !ok {
		existing = &connSlot{
			done: make(chan struct{}),
		}
		c.grpcConns[addr] = existing
		isFirstRequest = true
	}
	c.grpcConnsLock.Unlock()

	if !isFirstRequest {
		<-existing.done
		return existing.conn, existing.err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithInsecure(),
		grpc.WithDialer(dialGRPC),
		grpc.WithDisableRetry(),
		grpc.WithBlock(),
	)
	existing.conn = conn
	existing.err = err
	close(existing.done)

	return conn, err

}

func (c *RPCClient) grpcPath(p string) string {
	return grpcBasePath + "." + strings.Replace(p, ".", "/", -1)
}
