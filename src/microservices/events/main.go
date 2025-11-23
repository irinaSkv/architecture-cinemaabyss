package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
)

type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Description string   `json:"description,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	UserID      int      `json:"user_id,omitempty"`
	Action      string   `json:"action"`
	Rating      float64  `json:"rating,omitempty"`
}

type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  string    `json:"username,omitempty"`
	Email     string    `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	MethodType string    `json:"method_type,omitempty"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
}

var (
	producer sarama.SyncProducer
	consumer sarama.Consumer
)

func main() {
	initProducer()
	defer producer.Close()

	initConsumer()
	defer consumer.Close()

	go consumeMessages("movie-events")
	go consumeMessages("user-events")
	go consumeMessages("payment-events")

	router := mux.NewRouter()
	router.HandleFunc("/api/events/health", handleHealth).Methods("GET")
	router.HandleFunc("/api/events/movie", handleMovieEvent).Methods("POST")
	router.HandleFunc("/api/events/user", handleUserEvent).Methods("POST")
	router.HandleFunc("/api/events/payment", handlePaymentEvent).Methods("POST")

	port := "8082"
	log.Printf("Starting events service on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, router))
}

func initProducer() {
	brokers := []string{os.Getenv("KAFKA_BROKERS")}
	if brokers[0] == "" {
		brokers[0] = "kafka:9092"
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	var err error
	producer, err = sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}

  log.Printf("Message sent to topic")
	log.Println("Kafka producer initialized successfully")
}

func initConsumer() {
	brokers := []string{os.Getenv("KAFKA_BROKERS")}
	if brokers[0] == "" {
		brokers[0] = "kafka:9092"
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	var err error
	consumer, err = sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	log.Println("Kafka consumer initialized successfully")
}

func consumeMessages(topic string) {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("Failed to create partition consumer for topic %s: %v", topic, err)
		return
	}
	defer partitionConsumer.Close()

	log.Printf("Started consuming messages from topic: %s", topic)
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Received message from topic %s: %s", topic, string(msg.Value))
			processMessage(topic, msg.Value)
		case err := <-partitionConsumer.Errors():
			log.Printf("Error consuming from topic %s: %v", topic, err)
		}
	}
}

func processMessage(topic string, message []byte) {
	switch topic {
	case "movie-events":
		var event Event
		if err := json.Unmarshal(message, &event); err != nil {
			log.Printf("Error unmarshaling movie event: %v", err)
			return
		}
		log.Printf("Processing movie event: %+v", event)
	case "user-events":
		var event Event
		if err := json.Unmarshal(message, &event); err != nil {
			log.Printf("Error unmarshaling user event: %v", err)
			return
		}
		log.Printf("Processing user event: %+v", event)
	case "payment-events":
		var event Event
		if err := json.Unmarshal(message, &event); err != nil {
			log.Printf("Error unmarshaling payment event: %v", err)
			return
		}
		log.Printf("Processing payment event: %+v", event)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	var movieEvent MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&movieEvent); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("movie-%d-%s", movieEvent.MovieID, movieEvent.Action),
		Type:      "movie",
		Timestamp: time.Now(),
		Payload:   movieEvent,
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: "movie-events",
		Value: sarama.StringEncoder(eventJSON),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Movie event sent to partition %d at offset %d", partition, offset)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
		"event":     event,
	})
}

func handleUserEvent(w http.ResponseWriter, r *http.Request) {
	var userEvent UserEvent
	if err := json.NewDecoder(r.Body).Decode(&userEvent); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("user-%d-%s", userEvent.UserID, userEvent.Action),
		Type:      "user",
		Timestamp: time.Now(),
		Payload:   userEvent,
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: "user-events",
		Value: sarama.StringEncoder(eventJSON),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("User event sent to partition %d at offset %d", partition, offset)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
		"event":     event,
	})
}

func handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	var paymentEvent PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&paymentEvent); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("payment-%d-%s", paymentEvent.PaymentID, paymentEvent.Status),
		Type:      "payment",
		Timestamp: time.Now(),
		Payload:   paymentEvent,
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: "payment-events",
		Value: sarama.StringEncoder(eventJSON),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Payment event sent to partition %d at offset %d", partition, offset)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"partition": partition,
		"offset":    offset,
		"event":     event,
	})
}