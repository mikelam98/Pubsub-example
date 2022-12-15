package pubsub

import "sync"

type Subscribers map[string]*Subscriber

type Broker struct {
	subscribers Subscribers            //maps of subscriber id:Subscriber
	topics      map[string]Subscribers //map topic to subscriber
	Mutex       sync.RWMutex           //lock
}

func NewBroker() *Broker {
	//return new Broker object
	return &Broker{
		subscribers: Subscribers{},
		topics:      map[string]Subscribers{},
	}
}
func (b *Broker) Broadcast(msg string, topics []string) {
	//broadcast msg to all topics
	for _, topics := range topics {
		for _, s := range b.topics[topics] {
			m := NewMessage(msg, topics)
			go (func(s *Subscriber) {
				s.Signal(m)
			})(s)
		}
	}
}
func (b *Broker) AddSubscriber() *Subscriber {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()
	id, s := CreateSubscriber()
	b.subscribers[id] = s
	return s
}
func (b *Broker) Subscribe(s *Subscriber, topic string) {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()
	if b.topics[topic] == nil {
		b.topics[topic] = Subscribers{}
	}
	s.AddTopic(topic)
	b.topics[topic][s.Id] = s
}
func (b *Broker) Publish(msg string, topic string) {
	//publish the message to the given topic
	b.Mutex.RLock()
	bTopics := b.topics[topic]
	b.Mutex.Unlock()
	for _, s := range bTopics {
		m := NewMessage(msg, topic)
		if !s.Active {
			return
		}
		go (func(s *Subscriber) {
			s.Signal(m)
		})(s)
	}
}

//the Unsubscribing process delete the subscriberID from the specific topic map
func (b *Broker) Unsubscribe(s *Subscriber, topic string) {
	b.Mutex.RLock()
	defer b.Mutex.RLock()
	delete(b.topics[topic], s.Id)
	s.RemoveTopic(topic)
}
func (b *Broker) RemoveSubscriber(s *Subscriber) {
	for topic := range s.Topics {
		b.Unsubscribe(s, topic)
	}
	b.Mutex.Lock()
	delete(b.subscribers, s.Id)
	b.Mutex.Unlock()
	s.Destruct()
}

func (b *Broker) GetSubscribers(topic string) int {
	b.Mutex.RLock()
	defer b.Mutex.RUnlock()
	return len(b.topics[topic])
}
