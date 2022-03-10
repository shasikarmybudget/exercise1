package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
)

type UserData struct {
	Email        string `json:"email"`
	First_name   string `json:"first_name"`
	Last_name    string `json:"last_name"`
	Time_zone_id string `json:"time_zone_id"`
}

type User struct {
	Data UserData `json:"data"`
}

var users_map = make(map[uint64]User)

func add_user(w http.ResponseWriter, req *http.Request) {

	var user_id uint64
	body, err := ioutil.ReadAll(req.Body)

	if err != nil {
		panic(err)
	}

	for name, headers := range req.Header {
		if name == "User_id" {
			user_id, _ = strconv.ParseUint(headers[0], 10, 64)
		}
	}

	userStr := string(body)
	//fmt.Println("Response: ", userStr)
	var user User
	json.Unmarshal([]byte(userStr), &user)

	users_map[user_id] = user
	fmt.Println(users_map)

}

func modify_user(w http.ResponseWriter, req *http.Request) {

	var user_id uint64
	body, err := ioutil.ReadAll(req.Body)

	if err != nil {
		panic(err)
	}

	for name, headers := range req.Header {
		if name == "User_id" {
			user_id, _ = strconv.ParseUint(headers[0], 10, 64)
		}
	}

	userStr := string(body)
	//fmt.Println("Response: ", userStr)
	var user User
	json.Unmarshal([]byte(userStr), &user)
	//fmt.Println(user)

	users_map[user_id] = user

	fmt.Println(users_map)

}

func main() {

	//usersArr := make(map[int]User)

	http.HandleFunc("/users", add_user)
	http.HandleFunc("/modify", modify_user)
	http.ListenAndServe(":8090", nil)
}
