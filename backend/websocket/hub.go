package websocket

import "log"

// Hub maintains the set of active clients and broadcasts messages to the clients.
type Hub struct {
	// Registered clients.
	clients map[*Client]bool

	// Inbound messages from the external stock API.
	broadcast chan []byte

	// Register requests for clients.
	register chan *Client

	// Unregister requests for clients.
	unregister chan *Client

	// A channel to signal the hub to shut down.
	done chan struct{}
}

func NewHub() *Hub {
	return &Hub{
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
		done:       make(chan struct{}),
	}
}

// Register registers a new client with the hub.
func (h *Hub) Register(client *Client) {
	h.register <- client
}

// Unregister unregisters a client from the hub.
func (h *Hub) Unregister(client *Client) {
	h.unregister <- client
}

func (h *Hub) Broadcast(message []byte) {
	h.broadcast <- message
}

// Stop signals the hub to shut down and clean up resources.
func (h *Hub) Stop() {
	close(h.done)
}

// Run starts the hub's event loop.
// It's designed to be run in its own dedicated goroutine (go hub.Run() in ../main.go).
// Using channels for all interactions (register, unregister, broadcasts) ensures that the clients
// map is only ever accessed by a single goroutine (the one running hub.Run()), which prevents race
// conditions without needing explicit locks.
func (h *Hub) Run() {
	for {
		select {
		case <-h.done:
			for client := range h.clients {
				close(client.send)
			}
			log.Println("Hub shutting down.")
			return

		case client := <-h.register:
			h.clients[client] = true
			log.Println("New client registered.")

		case client := <-h.unregister:
			h.unregisterClient(client, "client disconnected")

		case message := <-h.broadcast:
			for client := range h.clients {
				if !client.TrySend(message) {
					h.unregisterClient(client, "Client's send channel blocked, client unregistered.")
				}
			}
		}
	}
}

// unregisterClient handles the cleanup for a disconnected client.
// Only to be used by Run(), to ensure only a single go routine has access to the clients map.
func (h *Hub) unregisterClient(client *Client, reason string) {
	// Ensure the client exists to prevent a double-unregister.
	if _, ok := h.clients[client]; ok {
		close(client.send)
		delete(h.clients, client)
		log.Printf("Client unregistered: %s", reason)
	}
}
