package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/crazywolf132/conduit"
)

// Common errors
var (
	ErrNotConnected = errors.New("client not connected to server")
	ErrClientClosed = errors.New("client is closed")
)

// Handler is a function type that handles incoming messages of a specific type.
type Handler func(*Client, *conduit.Message) error

// Client represents a Unix domain socket client. It supports sending and receiving
// JSON-encoded messages and optionally reconnecting on connection loss.
type Client struct {
	config    *conduit.ClientConfig
	conn      net.Conn
	handlers  map[string]Handler
	mu        sync.RWMutex
	done      chan struct{}
	closeOnce sync.Once
	context   map[string]interface{}
	contextMu sync.RWMutex
}

// NewClient creates a new Unix domain socket client with the given configuration.
//
// The provided config must not be nil. The returned client is not connected yet.
// Use Connect() or ConnectWithRetry() to establish a connection.
func NewClient(config *conduit.ClientConfig) *Client {
	if config == nil {
		panic("config cannot be nil")
	}
	return &Client{
		config:   config,
		handlers: make(map[string]Handler),
		done:     make(chan struct{}),
		context:  make(map[string]interface{}),
	}
}

// Connect attempts to establish a connection to the Unix domain socket server.
// It returns an error if the connection fails.
//
// Once connected, the client starts a background goroutine to listen for incoming messages.
func (c *Client) Connect() error {
	if c.IsClosed() {
		return ErrClientClosed
	}

	conn, err := net.Dial("unix", c.config.SocketPath)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	c.config.Logger.Infof("Connected to server at %s", c.config.SocketPath)

	go c.handleMessages(conn)
	return nil
}

// ConnectWithRetry continuously attempts to connect until successful if Reconnect is true.
// If Reconnect is false, it behaves like Connect.
//
// This method blocks until a connection is established or the client is closed.
func (c *Client) ConnectWithRetry() error {
	for {
		if err := c.Connect(); err == nil {
			return nil
		} else if !c.config.Reconnect {
			return err
		}

		c.config.Logger.Warnf("Failed to connect, retrying in %v...", c.config.ReconnectDelay)
		select {
		case <-c.done:
			return ErrClientClosed
		case <-time.After(c.config.ReconnectDelay):
			// retry
		}
	}
}

// Close closes the client connection and stops all background operations.
// Subsequent calls to Close are no-ops.
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.done)
		c.mu.Lock()
		if c.conn != nil {
			err = c.conn.Close()
			c.conn = nil
		}
		c.mu.Unlock()
		c.config.Logger.Info("Client closed")
	})
	return err
}

// Handle registers a handler for a given message type.
// Handlers should be registered before connecting.
func (c *Client) Handle(msgType string, handler Handler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[msgType] = handler
}

// Send sends a message to the server with the given type and payload.
// Returns ErrNotConnected if the client is not currently connected.
func (c *Client) Send(msgType string, payload interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.conn == nil {
		return ErrNotConnected
	}

	msg, err := conduit.NewMessage(msgType, payload)
	if err != nil {
		return fmt.Errorf("failed to create message: %w", err)
	}

	if c.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	}

	if err := json.NewEncoder(c.conn).Encode(msg); err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

func (c *Client) handleMessages(conn net.Conn) {
	defer func() {
		if c.config.Reconnect && !c.IsClosed() {
			c.config.Logger.Info("Connection lost, attempting to reconnect...")
			c.mu.Lock()
			c.conn = nil
			c.mu.Unlock()
			if err := c.ConnectWithRetry(); err != nil {
				c.config.Logger.Errorf("Failed to reconnect: %v", err)
			}
		}
	}()

	limited := conduit.NewLimitedReader(conn, c.config.MaxMessageSize)
	decoder := json.NewDecoder(limited)

	for {
		select {
		case <-c.done:
			return
		default:
			// Per-message limits:
			//  1. Reset the MaxMessageSize budget.
			//  2. Block in More, with no deadline, until a message starts (idle conns stay open).
			//  3. Give the rest of the message ReadTimeout.
			// If More fails, Decode returns the same error.
			limited.Reset()
			conn.SetReadDeadline(time.Time{})
			decoder.More()
			if c.config.ReadTimeout > 0 {
				conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
			}

			var msg conduit.Message
			if err := decoder.Decode(&msg); err != nil {
				if err != io.EOF && !c.IsClosed() {
					c.config.Logger.Errorf("Failed to decode message: %v", err)
				}
				return
			}

			c.mu.RLock()
			handler, exists := c.handlers[msg.Type]
			c.mu.RUnlock()

			if !exists {
				c.config.Logger.Warnf("No handler for message type '%s'", msg.Type)
				continue
			}

			if err := handler(c, &msg); err != nil {
				c.config.Logger.Errorf("Handler error for message type '%s': %v", msg.Type, err)
			}
		}
	}
}

// IsConnected returns true if the client currently has a live connection to the server.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil
}

// IsClosed returns true if the client has been closed.
func (c *Client) IsClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// GetContext retrieves a context value from the client's key-value store.
func (c *Client) GetContext(key string) (interface{}, bool) {
	c.contextMu.RLock()
	defer c.contextMu.RUnlock()
	val, ok := c.context[key]
	return val, ok
}

// SetContext sets a context value in the client's key-value store.
func (c *Client) SetContext(key string, value interface{}) {
	c.contextMu.Lock()
	defer c.contextMu.Unlock()
	c.context[key] = value
}
