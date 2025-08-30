package main

import (
	"log"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
	rtws "real-time-dashboard/backend/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func serveWs(hub *rtws.Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	client := rtws.NewClient(hub, conn)
	hub.Register(client)

	// Allow collection of memory and other resources.
	go client.WritePump()
	go client.ReadPump()
}

func main() {
	// Create a new Hub instance.
	hub := rtws.NewHub()
	go hub.Run()
	defer hub.Stop()

	// Get Finnhub API key from environment variables.
	// In your terminal, run `export FINNHUB_API_KEY="YOUR_API_KEY"`
	apiKey := os.Getenv("FINNHUB_API_KEY")
	if apiKey == "" {
		log.Fatal("Finnhub API key not found in environment variables. Please run `export FINNHUB_API_KEY=\"YOUR_API_KEY\"` in your terminal to set it.")
	}

	// Define the stocks to track.
	stocks := []string{"AAPL", "AMZN", "MSFT", "GOOG", "NVDA", "META", "TSLA", "NFLX",  "BINANCE:BTCUSDT"}

	// Create a new Subscriber and start it.
	subscriber := rtws.NewSubscriber(hub, apiKey, stocks)
	subscriber.Start()

	// Register our WebSocket handler.
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	log.Println("Server starting on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}