package libs

import (
	"net"
	"net/rpc"
	"time"
)

const (
	RpcDialTimeout = 30 * time.Second
)

type RpcServer struct {
	server   *rpc.Server
	listener net.Listener
}

func NewRpcServer(address string, rcvr interface{}) (s *RpcServer, err error) {
	s = &RpcServer{}
	s.server = rpc.NewServer()
	if s.listener, err = net.Listen("tcp", address); err != nil {
		s.Close()
		s = nil
		return
	}
	s.server.Register(rcvr)
	return
}

func (s *RpcServer) Serve() {
	s.server.Accept(s.listener)
}

func (s *RpcServer) Close() {
	if s.listener != nil {
		s.listener.Close()
		s.listener = nil
	}
}

type RpcClient struct {
	Address string
	rpcPool *VxPool
}

func NetRpcClient(address string) (c *RpcClient) {
	c = &RpcClient{
		Address: address,
		rpcPool: NewVxPool(10, func(x interface{}) {
			client := x.(*rpc.Client)
			client.Close()
		}),
	}
	return
}

func (c *RpcClient) newClient() (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", c.Address, RpcDialTimeout)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}

func (c *RpcClient) Call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	var client *rpc.Client
	for {
		node := c.rpcPool.Get()
		if node != nil {
			client = node.(*rpc.Client)
		} else if client, err = c.newClient(); err != nil {
			break
		}
		err = client.Call(serviceMethod, args, reply)
		if err == nil {
			break
		} else if err == rpc.ErrShutdown {
			client.Close()
			client = nil
		} else {
			client.Close()
			client = nil
			break
		}
	}
	if client != nil {
		c.rpcPool.Put(client)
	}
	return
}

func (c *RpcClient) Close() {
	for {
		node := c.rpcPool.Get()
		if node == nil {
			break
		}
		client := node.(*rpc.Client)
		client.Close()
	}
}
