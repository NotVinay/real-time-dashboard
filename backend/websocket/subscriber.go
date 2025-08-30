package websocket

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/gorilla/websocket"
)

// finnhubWsURL is the Finnhub Websocket URL for real-time stock data.
var finnhubWsURL = "wss://ws.finnhub.io"

// Stock represents a data point for a stock.
type Stock struct {
	Price     float64 `json:"price"`
	Symbol    string  `json:"symbol"`
	Timestamp int64   `json:"timestamp"`
	Volume    float64 `json:"volume"`
}

// Subscriber manages the connection to the Alpha Vantage API.
type Subscriber struct {
	hub        *Hub
	apiKey     string
	stocks     []string
	wsConn     *websocket.Conn
	done       chan struct{}
	stopOnce   sync.Once
}

func NewSubscriber(hub *Hub, apiKey string, stocks []string) *Subscriber {
	return &Subscriber{
		hub:    hub,
		apiKey: apiKey,
		stocks: stocks,
		done:   make(chan struct{}),
	}
}

// Start connects to the Alpha Vantage WebSocket and begins streaming data.
func (s *Subscriber) Start() {
	go s.connectAndStream()
}

// Stop gracefully closes the subscriber.
func (s *Subscriber) Stop() {
	s.stopOnce.Do(func() {
		close(s.done)
		if s.wsConn != nil {
			s.wsConn.Close()
		}
	})
}

func (s *Subscriber) connectAndStream() {
	// Connect to the Finnhub WebSocket.
	wsURL := fmt.Sprintf("%s?token=%s", finnhubWsURL, s.apiKey)
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Fatal("Could not connect to Finnhub WebSocket:", err)
	}
	s.wsConn = conn
	defer s.Stop()
	log.Println("Connected to Finnhub WebSocket.")
	
	// Send subscription messages for each stock.
	for _, stock := range s.stocks {
		msg, _ := json.Marshal(map[string]string{"type": "subscribe", "symbol": stock})
		s.wsConn.WriteMessage(websocket.TextMessage, msg)
	}

	for {
		select {
		case <-s.done:
			return
		default:
			// Example response doc https://finnhub.io/docs/api/websocket-trades.
			var msg map[string]interface{}
			if err := s.wsConn.ReadJSON(&msg); err != nil {
				// Graceful shutdown on close error
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("Finnhub connection closed: %v", err)
				}
				return
			}

			// Finnhub sends different message types. We only care about trades.
			// It can also send `ping` messages, which we can ignore.
			msgType, ok := msg["type"].(string)
			if !ok || msgType != "trade" {
				continue
			}

			payload, ok := msg["data"].([]interface{})
			if !ok {
				continue
			}

			for _, item := range payload {
				trade, ok := item.(map[string]interface{})
				if !ok {
					continue
				}

				// Finnhub timestamp is in milliseconds, convert to seconds.
				timestamp, _ := trade["t"].(float64)
				stock := Stock{
					Price:     trade["p"].(float64),
					Symbol:    trade["s"].(string),
					Timestamp: int64(timestamp / 1000),
					Volume:    trade["v"].(float64),
				}

				stockJSON, err := json.Marshal(stock)
				if err != nil {
					log.Printf("Failed to marshal stock data: %v", err)
					continue
				}
				// Broadcast the new data to the hub.
				s.hub.broadcast <- stockJSON
			}
		}
	}
}
