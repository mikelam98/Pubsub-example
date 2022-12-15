package pubsub

import (
	"crypto/rand"
	"fmt"
	"log"
	"sync"
)

type Subscriber struct {
	Id      string
	Message chan *Message   //message channel
	Topics  map[string]bool //topics it is subscriber topic
	Active  bool            //active subscriber
	Mutex   sync.RWMutex    //lock
}

func CreateSubscriber() (string, *Subscriber) {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	id := fmt.Sprintf("%X-%X,b[0:4],b[4:8]")
	return id, &Subscriber{
		Id:      id,
		Message: make(chan *Message),
		Topics:  map[string]bool{},
		Active:  true,
	}
}

//push the message to the message channel
func (s *Subscriber) Signal(msg *Message) {
	//Get the msg from the channel
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	if s.Active {
		s.Message <- msg
	}
}

//Destruct method of Subscriber sets the active as false,
//which means it closes the msg chan once we r done sending.
func (s *Subscriber) Destruct() {
	//destructer for the subscriber
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	s.Active = false
	close(s.Message)
}
func (s *Subscriber) RemoveTopic(topic string) {
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	delete(s.Topics, topic)
}
func (s *Subscriber) AddTopic(topic string) {
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	s.Topics[topic] = true
}
func (s *Subscriber) GetTopic() []string {
	//Get all topics of the subscriber
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	topics := []string{}
	for topic, _ := range s.Topics {
		topics = append(topics, topic)
	}
	return topics
}

//Listen to the subscriber's message channel and print the message
func (s *Subscriber) Listen() {
	//Listen to the message channel
	for {
		if msg, ok := <-s.Message; ok {
			fmt.Printf("Subscriber %s,received : %s from topic %s\n", s.Id, msg.GetMessageBody(), msg.GetTopic())
		}
	}
}
