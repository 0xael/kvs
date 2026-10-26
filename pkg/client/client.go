// Package client provides a reusable Go client for the GhostKV service.
package client

import (
	"context"

	pb "github.com/alyelalwany/ghostkv/gen/ghostkv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client wraps a gRPC connection to a GhostKV node.
type Client struct {
	conn *grpc.ClientConn
	kv   pb.KVServiceClient
}

// New dials the GhostKV node at addr using insecure credentials
// and returns a ready-to-use Client.
func New(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		kv:   pb.NewKVServiceClient(conn),
	}, nil
}

// Put stores a key-value pair and returns the number of acks received.
func (c *Client) Put(ctx context.Context, key string, value []byte) (int32, error) {
	resp, err := c.kv.Put(ctx, &pb.PutRequest{Key: key, Value: value})
	if err != nil {
		return 0, err
	}
	return resp.GetAcks(), nil
}

// Get retrieves the value for key. The boolean indicates whether the key was found.
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	resp, err := c.kv.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, false, err
	}
	return resp.GetValue(), resp.GetFound(), nil
}

// Delete removes a key. The boolean indicates whether the key existed.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	resp, err := c.kv.Delete(ctx, &pb.DeleteRequest{Key: key})
	if err != nil {
		return false, err
	}
	return resp.GetDeleted(), nil
}

// Status returns cluster status information.
func (c *Client) Status(ctx context.Context) (*pb.StatusResponse, error) {
	return c.kv.Status(ctx, &pb.StatusRequest{})
}

// Close tears down the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
