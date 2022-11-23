package main

import (
	"Kafka-web-Socket/database"
	"Kafka-web-Socket/helper"
	"context"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"log"
	"net/http"
)

// upgrader for upgrading an HTTP connection to websocket connection
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

//Basic Server Structure
//Client for keeping the record of sender-id and sender-connection-object
// msg for socket message
// kafka reader and kafka writer  for reading and writing messages on kafka

type Server struct {
	Clients map[string]*websocket.Conn
	Msg     string
	Reader  *kafka.Reader
	Writer  *kafka.Writer
}

func main() {

	//initializing the server

	var Server = Server{
		Clients: make(map[string]*websocket.Conn),
		Msg:     "",
		//initialize  a  new reader that consume from the given topic
		Reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{"localhost:9093"},
			Topic:   "kafka-topic",
		}),
		//initialize  a  new writer that produce from the given topic
		Writer: &kafka.Writer{
			Addr:     kafka.TCP("localhost:9093"),
			Topic:    "kafka-topic",
			Balancer: &kafka.LeastBytes{},
		},
	}

	err := database.ConnectAndMigrate("localhost", "5432", "chatapp", "local", "local", database.SSLModeDisable)
	if err != nil {
		logrus.Fatal(err)
		return
	}
	fmt.Println("connected")

	// kafka last offset to overcome duplicacy,encountered when messages are sent in continuous fashion
	Server.Reader.SetOffset(kafka.LastOffset)
	http.HandleFunc("/ws", Server.WebSocket)
	if err := http.ListenAndServe(":8084", nil); err != nil {
		logrus.Fatal(err)
	}
}

// WebSocket a http handler function wrapped with Server to make websocket work

func (srv Server) WebSocket(w http.ResponseWriter, r *http.Request) {
	// Creating a Connection object
	connectionObject, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	defer connectionObject.Close()
	//storing the connection object corresponding to sender-id
	srv.Clients[r.URL.Query().Get("sender-id")] = connectionObject
	receiverId := r.URL.Query().Get("receiver-id")
	senderId := r.URL.Query().Get("sender-id")

	//Execution of  threads
	go func() {
		for {
			// Reading the message from socket
			err := connectionObject.ReadJSON(&srv.Msg)
			if err != nil {
				logrus.Error("Error in reading msg %s", err)
				break
			}
			// Storing the Message in Kafka Message
			msg := kafka.Message{
				Key:   []byte(receiverId),
				Value: []byte(srv.Msg),
			}
			// Writing the message to kafka
			err = srv.Writer.WriteMessages(context.Background(), msg)
			if err != nil {
				logrus.Error("Error in writing msg %s", err)
				break
			}
		}
	}()

	for {
		//Reading the Message from kafka
		message, err := srv.Reader.ReadMessage(context.Background())
		{
			if err != nil {
				logrus.Error("Error in reading msg from kafka %s", err)
				break
			}
		}

		//Check Condition for receiver Connection object
		writerConn, ok := srv.Clients[string(message.Key)]
		if ok {
			//Writing data to Socket
			err := writerConn.WriteJSON(string(message.Value))
			if err != nil {
				logrus.Error("Error in writing msg %s", err)
				break
			}

			err = helper.InsertMessage(senderId, string(message.Key), string(message.Value))
			if err != nil {
				logrus.Error("Error in writing msg to database %s", err)
				break
			}
		}

	}

}
