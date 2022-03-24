package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

type Event struct {
	Meta    Meta     `json:"meta"`
	Payload UserData `json:"payload"`
}

type Meta struct {
	Type      string `json:"type"`
	EventId   string `json:"event_id"`
	CreatedAt int64  `json:"created_at"`
	TraceId   string `json:"trace_id"`
	ServiceId string `json:"service_id"`
}

type UserData struct {
	Id         uint64 `json:"id"`
	Username   string `json:"login"`
	FirstName  string `json:"first_name"`
	LastName   string `json:"last_name"`
	Email      string `json:"email"`
	TimeZoneID string `json:"time_zone_id"`
	Followers  []struct {
		Id       uint64 `json:"id"`
		Username string `json:"login"`
	} `json:"followers"`
	Repos []struct {
		Id       uint64 `json:"id"`
		RepoName string `json:"name"`
	} `json:"repos"`
}

type ModifyData struct {
	DataBody ModifyDataBody `json:"data"`
}

type ModifyDataBody struct {
	Email      string `json:"email"`
	FirstName  string `json:"first_name"`
	LastName   string `json:"last_name"`
	TimeZoneID string `json:"time_zone_id"`
}

func get_user_data(user *UserData) {

	resp, err := http.Get("https://api.github.com/users/shasikaud")

	if err != nil {
		log.Println("Error on request: ", err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Error on reading the resoponse: ", err)
	}

	//bodyData := string(body)
	//fmt.Println(bodyData)

	json.Unmarshal(body, &user)
	//fmt.Println(*user)

}

func get_user_followers(user *UserData) {

	resp, err := http.Get("https://api.github.com/users/shasikaud/followers")

	if err != nil {
		log.Println("Error on request: ", err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Error on reading the resoponse: ", err)
	}

	bodyStr := string(body)
	//fmt.Println(bodyStr)

	bodyStr = `{"followers":` + bodyStr + "}"
	//fmt.Println(bodyStr)
	bodyByte := []byte(bodyStr)
	json.Unmarshal(bodyByte, &user)
	//fmt.Println(*user)

}

func get_user_repos(user *UserData) {

	resp, err := http.Get("https://api.github.com/users/shasikaud/repos")

	if err != nil {
		log.Println("Error on request: ", err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Error on reading the resoponse: ", err)
	}

	bodyStr := string(body)
	//fmt.Println(bodyStr)

	bodyStr = `{"repos":` + bodyStr + "}"
	//fmt.Println(bodyStr)
	bodyByte := []byte(bodyStr)
	json.Unmarshal(bodyByte, &user)
	//fmt.Println(*user)

}

func getTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func wrapFunc(users map[uint64]UserData) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		//fmt.Println("wrapFunc called")
		var user_id uint64
		var modify_req ModifyData
		//var temp_user UserData
		var new_event Event

		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			panic(err)
		}

		bodyStr := string(body)
		//fmt.Println(bodyStr)

		json.Unmarshal([]byte(bodyStr), &modify_req)
		//fmt.Println(modify_req)

		for name, headers := range req.Header {
			if name == "User_id" {
				user_id, _ = strconv.ParseUint(headers[0], 10, 64)
			}
		}

		//fmt.Println(user_id)
		//json.Unmarshal([]byte(userStr), users[user_id])
		//fmt.Println(user)
		//users[user_id].Email = modify_req.DataBody.Email
		//users[user_id].Username = "shasika"

		new_event.Meta.Type = "UserInfoChanged"
		new_event.Meta.CreatedAt = getTimestamp()
		new_event.Meta.EventId = "35fe5a58-c2c2-4f9c-b14d-92e3a08ded1d"
		new_event.Meta.TraceId = "a408294c-3fcc-4747-a45c-5c88ba9e804d"
		new_event.Meta.ServiceId = "user-service"

		new_event.Payload = users[user_id]
		new_event.Payload.Email = modify_req.DataBody.Email
		new_event.Payload.FirstName = modify_req.DataBody.FirstName
		new_event.Payload.LastName = modify_req.DataBody.LastName
		new_event.Payload.TimeZoneID = modify_req.DataBody.TimeZoneID
		users[user_id] = new_event.Payload
		//fmt.Println(users)

		fmt.Println(new_event)

		//push to msg queue
		eventInBytes, _ := json.Marshal(new_event)
		PushCommentToQueue("event", eventInBytes)

	}
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func PushCommentToQueue(topic string, message []byte) error {

	brokersUrl := []string{"localhost:9092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}

func main() {

	users := make(map[uint64]UserData)

	var user UserData
	get_user_data(&user)
	get_user_followers(&user)
	get_user_repos(&user)
	//fmt.Println(user)
	//fmt.Println(getTimestamp())

	users[user.Id] = user
	//fmt.Println(users)

	http.HandleFunc("/modify", wrapFunc(users))
	server_Err := http.ListenAndServe(":8091", nil)
	fmt.Println(server_Err)

	//fmt.Println("FLAG1")

}
