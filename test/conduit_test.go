package test

import (
	"encoding/json"
	"testing"

	"github.com/crazywolf132/conduit"
)

func TestNewMessage(t *testing.T) {
	payload := map[string]interface{}{
		"hello": "world",
	}
	msg, err := conduit.NewMessage("greeting", payload)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if msg.Type != "greeting" {
		t.Errorf("Expected message type 'greeting', got '%s'", msg.Type)
	}

	var decoded map[string]interface{}
	if err := json.Unmarshal(msg.Payload, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal payload: %v", err)
	}

	if decoded["hello"] != "world" {
		t.Errorf("Expected payload[hello] = 'world', got '%v'", decoded["hello"])
	}
}

func TestUnmarshalPayload(t *testing.T) {
	msg, err := conduit.NewMessage("test", map[string]string{"key": "value"})
	if err != nil {
		t.Fatalf("Failed to create message: %v", err)
	}

	var data map[string]string
	if err := msg.UnmarshalPayload(&data); err != nil {
		t.Fatalf("Failed to unmarshal payload: %v", err)
	}

	if data["key"] != "value" {
		t.Errorf("Expected data[key] = 'value', got '%s'", data["key"])
	}
}

// TestMessageSizeLimit tests that messages exceeding the MaxMessageSize limit result in an error.
func TestMessageSizeLimit(t *testing.T) {
	data := make([]byte, 20)
	reader := conduit.NewLimitedReader(&mockReader{size: 20}, 15) // limit 15 bytes

	if n, _ := reader.Read(data); n != 15 {
		t.Errorf("Expected read to stop at the 15 byte limit, got %d bytes", n)
	}
	// Must be 0 bytes with the error. jsonv2 ignores errors that come with data.
	if n, err := reader.Read(data); n != 0 || err == nil {
		t.Errorf("Expected 0 bytes and an error past the limit, got %d bytes, err %v", n, err)
	}

	reader.Reset()
	if _, err := reader.Read(data); err != nil {
		t.Errorf("Expected Reset to restore the limit, got %v", err)
	}
}

type mockReader struct {
	size int
}

func (m *mockReader) Read(p []byte) (int, error) {
	for i := 0; i < len(p) && i < m.size; i++ {
		p[i] = byte('a')
	}
	if m.size <= len(p) {
		return m.size, nil
	}
	return len(p), nil
}
