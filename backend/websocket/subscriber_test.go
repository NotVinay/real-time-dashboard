package websocket

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/gorilla/websocket"
)

// mockFinnhubServer creates a mock Finnhub websocket server.
func mockFinnhubServer(t *testing.T, wg *sync.WaitGroup, stockDataToSend Stock) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer wg.Done()
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("mockFinnhubServer upgrade error: %v", err)
			return
		}
		defer conn.Close()

		// 1. Expect a subscription message
		var subMsg map[string]string
		if err := conn.ReadJSON(&subMsg); err != nil {
			t.Logf("mockFinnhubServer failed to read subscription message: %v", err)
			return
		}
		if subMsg["type"] != "subscribe" || subMsg["symbol"] != stockDataToSend.Symbol {
			t.Errorf("mockFinnhubServer received unexpected subscription message: %+v", subMsg)
			return
		}

		// 2. Send some trade data
		tradePayload := []interface{}{
			map[string]interface{}{
				"p": stockDataToSend.Price,
				"s": stockDataToSend.Symbol,
				"t": float64(stockDataToSend.Timestamp * 1000), // Finnhub sends ms
				"v": stockDataToSend.Volume,
			},
		}
		tradeMsg := map[string]interface{}{
			"type": "trade",
			"data":  tradePayload,
		}
		if err := conn.WriteJSON(tradeMsg); err != nil {
			t.Logf("mockFinnhubServer failed to write trade message: %v", err)
		}
	}
}

func TestSubscriber(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	testStock := Stock{
		Symbol:    "TEST",
		Price:     123.45,
		Timestamp: time.Now().Unix(),
		Volume:    0.01134,
	}

	server := httptest.NewServer(mockFinnhubServer(t, &wg, testStock))
	defer server.Close()

	originalFinnhubWsURL := finnhubWsURL
	finnhubWsURL = "ws" + strings.TrimPrefix(server.URL, "http")
	defer func() { finnhubWsURL = originalFinnhubWsURL }()

	hub := NewHub()
	go hub.Run()
	defer hub.Stop()

	subscriber := NewSubscriber(hub, "test_api_key", []string{testStock.Symbol})
	subscriber.Start()
	defer subscriber.Stop()

	select {
	case msg := <-hub.broadcast:
		var receivedStock Stock
		if err := json.Unmarshal(msg, &receivedStock); err != nil {
			t.Fatalf("failed to unmarshal stock data from hub: %v", err)
		}
		if diff := cmp.Diff(testStock, receivedStock); diff != "" {
			t.Errorf("stock data diff (-want +got):\n%s", diff)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("hub did not receive message in time")
	}

	wg.Wait()
}
