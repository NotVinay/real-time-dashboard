package websocket

import (
	"testing"
	"time"
)

func TestNewHub(t *testing.T) {
	hub := NewHub()
	if hub.clients == nil {
		t.Fatal("clients map not initialized")
	}
	if hub.broadcast == nil {
		t.Fatal("broadcast channel not initialized")
	}
	if hub.register == nil {
		t.Fatal("register channel not initialized")
	}
	if hub.unregister == nil {
		t.Fatal("unregister channel not initialized")
	}
	if hub.done == nil {
		t.Fatal("done channel not initialized")
	}
}

func TestRun(t *testing.T) {
	tests := []struct {
		desc string
		test func(*testing.T, *Hub, *Client)
		wantMessage string
		wantSendChannelClosed bool
	}{
		{
			desc: "Broadcast",
			test: func(t *testing.T, hub *Hub, client *Client) {
				msg := []byte("hello")
				hub.Broadcast(msg)
			},
			wantMessage: "hello",
			wantSendChannelClosed: false,
		},
		{
			desc: "Unregister",
			test: func(t *testing.T, hub *Hub, client *Client) {
				hub.Unregister(client)
				// Give client time to unregister.
				time.Sleep(10 * time.Millisecond)
			},
			wantMessage: "",
			wantSendChannelClosed: true,
		},
		{
			desc: "Slow client gets unregistered",
			test: func(t *testing.T, hub *Hub, client *Client) {
				// Fill buffer
				hub.Broadcast([]byte("first"))

				// This broadcast should trigger unregistration, since client's send channel
				// capacity is already filled.
				hub.Broadcast([]byte("second"))
				time.Sleep(20 * time.Millisecond)

				// Drain the buffered message, this is needed because without this the test will
				// check the first buffered message against the wantMessage.
				// The test assertion will then check for channel closure.
				<-client.send
			},
			wantMessage: "",
			wantSendChannelClosed: true,
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			hub := NewHub()
			t.Cleanup(func() {
				hub.Stop()
			})
			go hub.Run()
			
			// Register client to the hub
			client := &Client{hub: hub, send: make(chan []byte, 1)}
			hub.Register(client)
			time.Sleep(10 * time.Millisecond) // allow registration

			test.test(t, hub, client)

			select {
			case received, ok := <-client.send:
				if string(received) != test.wantMessage {
					t.Fatalf("expected message %q, got %q", test.wantMessage, received)
				}
				if ok == test.wantSendChannelClosed {
					t.Fatalf("expected send channel status to be %v, got: %v", test.wantSendChannelClosed, ok)
				}
			case <-time.After(100 * time.Millisecond):
				t.Fatal("timed out waiting for message")
			}
		})
	}
}