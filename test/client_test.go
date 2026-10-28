package test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/crazywolf132/conduit"
	"github.com/crazywolf132/conduit/client"
	"github.com/crazywolf132/conduit/server"
)

// TestClientServerInteraction tests a simple interaction between client and server.
func TestClientServerInteraction(t *testing.T) {
	socketPath := "/tmp/conduit_test.sock"
	defer os.RemoveAll(socketPath)

	serverCfg := conduit.DefaultServerConfig(socketPath)
	serverCfg.Logger = conduit.NewLogger(conduit.LogError, nil) // silent for test
	srv := server.NewServer(serverCfg)

	// server handler echoes back messages of type "echo"
	srv.Handle("echo", func(conn *server.Connection, msg *conduit.Message) error {
		var payload string
		if err := msg.UnmarshalPayload(&payload); err != nil {
			return err
		}
		return conn.Send("echo_response", payload+"_response")
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer srv.Stop()

	clientCfg := conduit.DefaultClientConfig(socketPath)
	clientCfg.Logger = conduit.NewLogger(conduit.LogError, nil) // silent for test
	c := client.NewClient(clientCfg)

	received := make(chan string, 1)
	c.Handle("echo_response", func(_ *client.Client, msg *conduit.Message) error {
		var resp string
		if err := msg.UnmarshalPayload(&resp); err != nil {
			return err
		}
		received <- resp
		return nil
	})

	if err := c.Connect(); err != nil {
		t.Fatalf("Client failed to connect: %v", err)
	}
	defer c.Close()

	// Send a message
	if err := c.Send("echo", "hello"); err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	select {
	case resp := <-received:
		if resp != "hello_response" {
			t.Errorf("Expected 'hello_response', got '%s'", resp)
		}
	case <-time.After(time.Second):
		t.Error("Timeout waiting for response")
	}
}

// TestClientNotConnected ensures sending before connecting returns an error.
func TestClientNotConnected(t *testing.T) {
	cfg := conduit.DefaultClientConfig("/tmp/does_not_exist.sock")
	cfg.Logger = conduit.NewLogger(conduit.LogError, nil)
	c := client.NewClient(cfg)

	if err := c.Send("test", "payload"); err == nil {
		t.Error("Expected error when sending on not connected client, got none")
	}
}

// TestClientReconnect tests that the client reconnects automatically if Reconnect is enabled.
func TestClientReconnect(t *testing.T) {
	socketPath := "/tmp/conduit_reconnect_test.sock"
	defer os.RemoveAll(socketPath)

	serverCfg := conduit.DefaultServerConfig(socketPath)
	serverCfg.Logger = conduit.NewLogger(conduit.LogError, nil)
	srv := server.NewServer(serverCfg)

	srv.Handle("test", func(conn *server.Connection, msg *conduit.Message) error {
		return conn.Send("test_response", "ok")
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	clientCfg := conduit.DefaultClientConfig(socketPath)
	clientCfg.Logger = conduit.NewLogger(conduit.LogError, nil)
	clientCfg.Reconnect = true
	clientCfg.ReconnectDelay = 100 * time.Millisecond
	c := client.NewClient(clientCfg)

	received := make(chan string, 1)
	c.Handle("test_response", func(_ *client.Client, msg *conduit.Message) error {
		var resp string
		if err := msg.UnmarshalPayload(&resp); err != nil {
			return err
		}
		received <- resp
		return nil
	})

	// Connect the client
	if err := c.Connect(); err != nil {
		t.Fatalf("Client failed to connect: %v", err)
	}

	// Close the server to force a client disconnect
	if err := srv.Stop(); err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}

	// Restart the server to simulate server coming back online
	srv = server.NewServer(serverCfg)
	srv.Handle("test", func(conn *server.Connection, msg *conduit.Message) error {
		return conn.Send("test_response", "ok_reconnected")
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server again: %v", err)
	}
	defer srv.Stop()

	// The client should reconnect automatically
	time.Sleep(500 * time.Millisecond) // wait for reconnection attempts

	// Send a message again
	if err := c.Send("test", "ping"); err != nil {
		t.Fatalf("Failed to send message after reconnection: %v", err)
	}

	select {
	case resp := <-received:
		if resp != "ok_reconnected" {
			t.Errorf("Expected 'ok_reconnected', got '%s'", resp)
		}
	case <-time.After(time.Second):
		t.Error("Timeout waiting for response after reconnection")
	}

	c.Close()
}

// TestClientContext tests that setting context on the client works as expected.
func TestClientContext(t *testing.T) {
	cfg := conduit.DefaultClientConfig("/tmp/does_not_exist.sock")
	cfg.Logger = conduit.NewLogger(conduit.LogError, nil)
	c := client.NewClient(cfg)

	c.SetContext("user_id", 42)
	val, ok := c.GetContext("user_id")
	if !ok {
		t.Error("Expected to get a value from context, got none")
	}
	if val != 42 {
		t.Errorf("Expected context value 42, got %v", val)
	}
}

// TestConnectionLimitsArePerMessage checks that ReadTimeout and MaxMessageSize apply per message:
//  1. An idle connection outlives ReadTimeout.
//  2. Messages may add up to more than MaxMessageSize.
//  3. A message too big for one decoder read, but under the limit, gets through.
//  4. A message over MaxMessageSize is rejected.
func TestConnectionLimitsArePerMessage(t *testing.T) {
	socketPath := "/tmp/conduit_limits_test.sock"
	defer os.RemoveAll(socketPath)

	serverCfg := conduit.DefaultServerConfig(socketPath)
	serverCfg.Logger = conduit.NewLogger(conduit.LogError, nil)
	serverCfg.ReadTimeout = 100 * time.Millisecond
	serverCfg.MaxMessageSize = 16 * 1024
	srv := server.NewServer(serverCfg)

	srv.Handle("echo", func(conn *server.Connection, msg *conduit.Message) error {
		var payload string
		if err := msg.UnmarshalPayload(&payload); err != nil {
			return err
		}
		return conn.Send("echo_response", payload)
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer srv.Stop()

	clientCfg := conduit.DefaultClientConfig(socketPath)
	clientCfg.Logger = conduit.NewLogger(conduit.LogError, nil)
	clientCfg.ReadTimeout = 100 * time.Millisecond
	clientCfg.MaxMessageSize = 16 * 1024
	clientCfg.Reconnect = false // a dropped connection must fail the test, not be reconnected
	c := client.NewClient(clientCfg)

	received := make(chan string, 1)
	c.Handle("echo_response", func(_ *client.Client, msg *conduit.Message) error {
		var resp string
		if err := msg.UnmarshalPayload(&resp); err != nil {
			return err
		}
		received <- resp
		return nil
	})

	if err := c.Connect(); err != nil {
		t.Fatalf("Client failed to connect: %v", err)
	}
	defer c.Close()

	// 1. Idle for 3x ReadTimeout
	time.Sleep(300 * time.Millisecond)

	// 2. ~23KB of small messages. 3. One 15KB message that needs several reads.
	var payloads []string
	for i := 0; i < 100; i++ {
		payloads = append(payloads, strings.Repeat("x", 200))
	}
	payloads = append(payloads, strings.Repeat("y", 15000))

	for i, payload := range payloads {
		if err := c.Send("echo", payload); err != nil {
			t.Fatalf("Failed to send message %d: %v", i, err)
		}
		select {
		case resp := <-received:
			if resp != payload {
				t.Fatalf("Unexpected response to message %d", i)
			}
		case <-time.After(time.Second):
			t.Fatalf("Timeout waiting for response to message %d", i)
		}
	}

	// 4. Over MaxMessageSize: must be rejected
	if err := c.Send("echo", strings.Repeat("z", 20000)); err != nil {
		t.Fatalf("Failed to send oversized message: %v", err)
	}
	select {
	case <-received:
		t.Error("Message over MaxMessageSize was accepted")
	case <-time.After(300 * time.Millisecond):
	}
}
