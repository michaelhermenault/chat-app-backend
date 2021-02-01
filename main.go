// Golang HTML5 Server Side Events Example
//
// Run this code like:
//  > go run server.go
//
// Then open up your browser to http://localhost:8000
// Your browser must support HTML5 SSE, of course.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
)

const (
	historyOnLoad   = 100
	messageListName = "messages"
	channelListName = "channels"
	messageType     = "message"
	channelType     = "channel"
)

var ctx = context.Background()

type (
	broker struct {

		// Create a map of clients, the keys of the map are the channels
		// over which we can push messages to attached clients.  (The values
		// are just booleans and are meaningless.)
		//
		clients map[chan string]struct{}

		// Channel into which new clients can be pushed
		//
		newClients chan chan string

		// Channel into which disconnected clients should be pushed
		//
		defunctClients chan chan string

		// Channel into which messages are pushed to be broadcast out
		// to attahed clients.
		//
		messages chan string

		// Redis connection to save messasges
		rdb *redis.Client
	}
	message struct {
		Text     string `json:"text"`
		UserName string `json:"userName"`
		Channel  string `json:"channel"`
	}
	outBoundMessage struct {
		Type      string `json:"type"`
		Text      string `json:"text"`
		UserName  string `json:"userName"`
		Channel   string `json:"channel"`
		TimeStamp string `json:"time"`
	}
	channelMessage struct {
		Type    string `json:"type"`
		Channel string `json:"channel"`
	}
)

func (b *broker) Start() {

	// Start a goroutine
	//
	go func() {

		// Loop endlessly
		//
		for {

			// Block until we receive from one of the
			// three following channels.
			select {

			case s := <-b.newClients:

				// There is a new client attached and we
				// want to start sending them messages.
				b.clients[s] = struct{}{}

				// Push 100 latest messages from each channel to the client
				go func() {
					latestMessages, err := b.rdb.LRange(ctx, messageListName, -historyOnLoad, historyOnLoad).Result()

					if err != nil {
						fmt.Printf("Error retreiving messages\n")
						return
					}

					channels, err := b.rdb.LRange(ctx, channelListName, 0, -1).Result()

					if err != nil {
						fmt.Printf(("Error retreiving channel list\n"))
					}

					// List current channels
					for _, channel := range channels {
						rawChannel, _ := json.Marshal(channelMessage{channelType, channel})
						s <- string(rawChannel)
					}

					// Playback message history
					for _, message := range latestMessages {
						if _, ok := b.clients[s]; ok {
							s <- message
						}
					}
				}()

				log.Println("Added new client")

			case s := <-b.defunctClients:

				// A client has dettached and we want to
				// stop sending them messages.
				delete(b.clients, s)
				close(s)

				log.Println("Removed client")

			case msg := <-b.messages:

				// There is a new message to send.  For each
				// attached client, push the new message
				// into the client's message channel.
				for s := range b.clients {
					s <- msg
				}
				log.Printf("Broadcast message to %d clients", len(b.clients))
			}
		}
	}()
}

func (b *broker) handleMessage(w http.ResponseWriter, r *http.Request) {
	var p message
	err := json.NewDecoder(r.Body).Decode(&p)
	if err != nil {
		http.Error(w, "Bad JSON body", http.StatusBadRequest)
		return
	}

	rawMessage, _ := json.Marshal(outBoundMessage{messageType, p.Text, p.UserName, p.Channel, strconv.Itoa(int(time.Now().Unix()))})
	message := string(rawMessage)

	// Store the message for replay by future joining clients
	_, err = b.rdb.RPush(ctx, messageListName, message).Result()
	if err != nil {
		http.Error(w, "Error persisting message to database", http.StatusInternalServerError)
		return
	}
	// Push the message to all current clients
	b.messages <- message
	fmt.Fprintf(w, "Message sent")
}

// This broker method handles and HTTP request at the "/events/" URL.
//
func (b *broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	// Make sure that the writer supports flushing.
	//
	f, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	// Create a new channel, over which the broker can
	// send this client messages.
	messageChan := make(chan string)

	// Add this client to the map of those that should
	// receive updates
	b.newClients <- messageChan

	// Listen to the closing of the http connection via the CloseNotifier
	notify := w.(http.CloseNotifier).CloseNotify()
	go func() {
		<-notify
		// Remove this client from the map of attached clients
		// when `EventHandler` exits.
		b.defunctClients <- messageChan
		log.Println("HTTP connection just closed.")
	}()

	// Set the headers related to event streaming.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Transfer-Encoding", "chunked")

	// Don't close the connection, instead loop endlessly.
	for {

		// Read from our messageChan.
		msg, open := <-messageChan

		if !open {
			// If our messageChan was closed, this means that the client has
			// disconnected.
			break
		}

		// Write to the ResponseWriter, `w`.
		fmt.Fprintf(w, "data:%s\n\n", msg)

		// Flush the response.  This is only possible if
		// the repsonse supports streaming.
		f.Flush()
	}

	// Done.
	log.Println("Finished HTTP request at ", r.URL.Path)
}

// Handler for the main page, which we wire up to the
// route at "/" below in `main`.
//

// Main routine
//
func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Make a new broker instance
	b := &broker{
		make(map[chan string]struct{}),
		make(chan (chan string)),
		make(chan (chan string)),
		make(chan string),
		rdb,
	}

	// Start processing events
	b.Start()

	r := mux.NewRouter()
	r.Handle("/messages", b).Methods("GET")
	r.HandleFunc("/messages", b.handleMessage).Methods("POST")

	http.Handle("/", r)

	// Start the server and listen forever on port 8000.
	http.ListenAndServe(":8081", nil)
}
