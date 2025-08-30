package websocket

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestNewClient(t *testing.T) {
	hub := NewHub()
	conn := &websocket.Conn{}
	client := NewClient(hub, conn)

	if client.hub != hub {
		t.Error("expected client.hub to be the provided hub")
	}
	if client.conn != conn {
		t.Error("expected client.conn to be the provided conn")
	}
	if client.send == nil {
		t.Error("expected client.send channel to be initialized")
	}
}

func TestClientTrySend(t *testing.T) {
	tests := []struct {
		desc        string
		setup       func() *Client
		wantSuccess bool
	}{
		{
			desc: "Send with capacity",
			setup: func() *Client {
				return &Client{send: make(chan []byte, 1)}
			},
			wantSuccess: true,
		},
		{
			desc: "Send to full channel",
			setup: func() *Client {
				client := &Client{send: make(chan []byte, 1)}
				client.send <- []byte("first message") // Pre-fill the channel
				return client
			},
			wantSuccess: false,
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			client := test.setup()
			if got := client.TrySend([]byte("test message")); got != test.wantSuccess {
				t.Errorf("Client.TrySend() = %v, want %v", got, test.wantSuccess)
			}
		})
	}
}

// newFakeServer creates a fake server for testing the messages that get written on the client connection.
func newFakeServer(t *testing.T, serverReceivedMsg chan []byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("server upgrade error: %v", err)
			return
		}
		defer conn.Close()

		// Read one message and send it to the channel for assertion.
		// This proves the write pump is working.
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return // Connection closed by client, which is expected later.
		}
		serverReceivedMsg <- msg

		// Keep the connection open to test pump shutdown by reading until error.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	}))
}

// TestWritePump verifies that the write pump correctly sends messages and terminates.
func TestWritePump(t *testing.T) {
	// server that reads one message and then waits to be closed.
	serverReceivedMsg := make(chan []byte, 1)
	s := newFakeServer(t, serverReceivedMsg)
	defer s.Close()

	// Connect to the 
	wsURL := "ws" + strings.TrimPrefix(s.URL, "http")
	clientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}

	// We don't need a running hub, just an instance to create the client.
	hub := NewHub()
	client := NewClient(hub, clientConn)

	// Run writePump in a goroutine and use a channel to signal when it's done.
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		client.WritePump()
	}()
 
	// 1. Test that a message sent to the client's `send` channel is written to the connection.
	testMessage := []byte("hello world")
	sentSuccessfully := client.TrySend(testMessage)
	if !sentSuccessfully {
		t.Fatal("failed to send message to client, expected to succeed")
	}
	// Verify that the sent message got received by the connected server successfully. 
	select {
	case received := <-serverReceivedMsg:
		if string(received) != string(testMessage) {
			t.Errorf("expected server to receive message %q, got %q", testMessage, received)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for server to receive message")
	}

	// 2. Test that closing the `send` channel terminates the pump and closes the connection.
	close(client.send)
	// Wait for the pump to finish, proving it terminates correctly.
	select {
	case <-pumpDone:
		// pump exited cleanly
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for write pump to terminate")
	}
	// Verify the connection is actually closed by trying to write to it, which should fail.
	err = client.conn.WriteMessage(websocket.TextMessage, []byte("should fail"))
	if err == nil {
		t.Error("expected an error when writing to a closed connection, but got nil")
	}
}