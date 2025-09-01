package main

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// We'll handle CORS properly later, but for now, allow all origins
		return true
	},
}

// Handles WebSocket connections
func wsHandler(w http.ResponseWriter, r *http.Request) {
	// Upgrade the HTTP connection to a WebSocket connection
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade failed:", err)
		return
	}
	defer conn.Close()

	log.Println("Client connected!")

	// For now, the handler will do nothing else.
	// In the next phase, we'll add logic to send data.
	for {
		// Keep the connection alive
		// Read messages from the client to prevent the handler from exiting
		_, _, err := conn.ReadMessage()
		if err != nil {
			log.Println("Read error:", err)
			break
		}
	}
}

func main() {
	// Register our WebSocket handler
	http.HandleFunc("/ws", wsHandler)

	log.Println("Server starting on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}