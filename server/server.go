package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/crazywolf132/conduit"
)

// Handler is a function type that processes incoming messages of a specific type.
// The 'conn' parameter provides context about the client connection and methods to send responses.
// The 'msg' parameter is the incoming message.
//
// Handlers should return nil on success or an error if processing fails.
type Handler func(*Connection, *conduit.Message) error

// Server represents a Unix domain socket server that can accept multiple client connections
// and exchange JSON-encoded messages with them.
//
// It supports registering handlers for specific message types and broadcasting messages
// to all connected clients.
type Server struct {
	config    *conduit.ServerConfig
	listener  net.Listener
	handlers  map[string]Handler
	mu        sync.RWMutex
	conns     map[*Connection]struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// Connection represents a single client connection to the server.
//
// Each Connection:
//   - Has a unique ID
//   - Allows message sends back to the client
//   - Supports context storage for per-connection metadata
type Connection struct {
	conn    net.Conn
	server  *Server
	done    chan struct{}
	id      string
	context map[string]interface{}
	mu      sync.RWMutex
}

// NewServer creates a new Server using the provided configuration.
// Panics if config is nil.
//
// Example:
//
//	cfg := conduit.DefaultServerConfig("/tmp/app.sock")
//	s := server.NewServer(cfg)
//	if err := s.Start(); err != nil { ... }
func NewServer(config *conduit.ServerConfig) *Server {
	if config == nil {
		panic("config cannot be nil")
	}
	return &Server{
		config:   config,
		handlers: make(map[string]Handler),
		conns:    make(map[*Connection]struct{}),
		done:     make(chan struct{}),
	}
}

// Handle registers a handler function for a given message type.
// If a message with the specified type is received, the handler is invoked.
func (s *Server) Handle(msgType string, handler Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[msgType] = handler
}

// Start begins listening on the configured Unix domain socket and accepts client connections.
//
// The server runs in the background, accepting connections and processing messages. To stop,
// call Stop(). If Start fails (e.g., unable to listen on the socket), it returns an error.
func (s *Server) Start() error {
	// Remove existing socket file if present
	if err := os.RemoveAll(s.config.SocketPath); err != nil {
		return fmt.Errorf("failed to remove existing socket: %w", err)
	}

	listener, err := net.Listen("unix", s.config.SocketPath)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	// Apply permissions to the socket file
	if err := os.Chmod(s.config.SocketPath, os.FileMode(s.config.SocketPermissions)); err != nil {
		listener.Close()
		return fmt.Errorf("failed to set socket permissions: %w", err)
	}

	s.listener = listener
	s.config.Logger.Infof("Server started on %s", s.config.SocketPath)

	go s.acceptConnections()
	return nil
}

// Stop stops the server, closes all active connections, and removes the socket file.
// It is safe to call multiple times; subsequent calls will have no effect.
func (s *Server) Stop() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.done)

		s.mu.Lock()
		if s.listener != nil {
			err = s.listener.Close()
		}

		for conn := range s.conns {
			conn.Close()
		}
		s.mu.Unlock()

		if err2 := os.RemoveAll(s.config.SocketPath); err2 != nil && err == nil {
			err = err2
		}
	})
	return err
}

func (s *Server) acceptConnections() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
				s.config.Logger.Errorf("Failed to accept connection: %v", err)
				continue
			}
		}

		clientConn := &Connection{
			conn:    conn,
			server:  s,
			done:    make(chan struct{}),
			id:      generateConnID(),
			context: make(map[string]interface{}),
		}

		s.mu.Lock()
		s.conns[clientConn] = struct{}{}
		s.mu.Unlock()

		s.config.Logger.Infof("New connection established: %s", clientConn.id)
		go s.handleConnection(clientConn)
	}
}

func (s *Server) handleConnection(conn *Connection) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.config.Logger.Infof("Connection closed: %s", conn.id)
	}()

	limited := conduit.NewLimitedReader(conn.conn, s.config.MaxMessageSize)
	decoder := json.NewDecoder(limited)

	for {
		select {
		case <-s.done:
			return
		case <-conn.done:
			return
		default:
			// Per-message limits:
			//  1. Reset the MaxMessageSize budget.
			//  2. Block in More, with no deadline, until a message starts (idle conns stay open).
			//  3. Give the rest of the message ReadTimeout.
			// If More fails, Decode returns the same error.
			limited.Reset()
			conn.conn.SetReadDeadline(time.Time{})
			decoder.More()
			if s.config.ReadTimeout > 0 {
				conn.conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
			}

			var msg conduit.Message
			if err := decoder.Decode(&msg); err != nil {
				if err != io.EOF {
					s.config.Logger.Errorf("Failed to decode message from %s: %v", conn.id, err)
				}
				return
			}

			s.mu.RLock()
			handler, exists := s.handlers[msg.Type]
			s.mu.RUnlock()

			if !exists {
				s.config.Logger.Warnf("No handler for message type '%s' from %s", msg.Type, conn.id)
				continue
			}

			if err := handler(conn, &msg); err != nil {
				s.config.Logger.Errorf("Handler error for message type '%s' from %s: %v", msg.Type, conn.id, err)
			}
		}
	}
}

// Send sends a message of the given type and payload back to the client of this connection.
// Returns an error if the message could not be encoded or sent.
func (c *Connection) Send(msgType string, payload interface{}) error {
	msg, err := conduit.NewMessage(msgType, payload)
	if err != nil {
		return err
	}

	if c.server.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.server.config.WriteTimeout))
	}

	return json.NewEncoder(c.conn).Encode(msg)
}

// Close terminates the client connection. Safe to call multiple times.
func (c *Connection) Close() error {
	var err error
	c.mu.Lock()
	select {
	case <-c.done:
		// already closed
	default:
		close(c.done)
		err = c.conn.Close()
	}
	c.mu.Unlock()
	return err
}

// Broadcast sends a message of the given type and payload to all connected clients.
// Returns an error if the message payload cannot be marshaled.
func (s *Server) Broadcast(msgType string, payload interface{}) error {
	msg, err := conduit.NewMessage(msgType, payload)
	if err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	for conn := range s.conns {
		if err := conn.Send(msg.Type, msg.Payload); err != nil {
			s.config.Logger.Errorf("Failed to broadcast to %s: %v", conn.id, err)
		}
	}

	return nil
}

// GetContext retrieves a value associated with 'key' from the connection's context store.
func (c *Connection) GetContext(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.context[key]
	return val, ok
}

// SetContext associates a value with 'key' in the connection's context store.
func (c *Connection) SetContext(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.context[key] = value
}

// ID returns the unique identifier of this connection.
func (c *Connection) ID() string {
	return c.id
}

func generateConnID() string {
	return fmt.Sprintf("conn_%d", time.Now().UnixNano())
}
