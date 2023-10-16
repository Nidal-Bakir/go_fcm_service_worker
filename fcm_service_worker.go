package main

import (
	"bufio"
	"context"
	"encoding/json"
	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"
)

const (
	kTokensCountPerWorker = 500 // 0> && <= 500
	kSemaphoreCount       = 10  // the number of workers that can run at the same time
	kTokensFilePath       = "./tokens"
	kMessageFilePath      = "./message.json"
	kCredentialsFilePath  = "./credentials_file.json"
)

var TTL time.Duration = 60 * 60 * 24 * 7

func main() {
	client, err := initFirebaseClient()
	if err != nil {
		log.Fatalf("Error unable to init firebase client/app\n")
		return
	}

	tokensFile, err := os.Open(kTokensFilePath)
	if err != nil {
		log.Fatalf("Error reading/opening tokens file:\nFile path: %s\nError: %v\n", kTokensFilePath, err)
		return
	}

	notificationMessage, err := getMessageFromFile(kMessageFilePath)
	if err != nil {
		log.Fatalf("Error unable to get notification message\n")
		return
	}

	startWorker(client, tokensFile, notificationMessage)

	log.Println("=============================Done======================")

}

func initFirebaseClient() (*messaging.Client, error) {
	ctx := context.Background()
	app, err := firebase.NewApp(ctx, nil, option.WithCredentialsFile(kCredentialsFilePath))

	if err != nil {
		log.Fatalf("Error in initializing firebase app: %s\n", err)
		return nil, err
	}

	client, err := app.Messaging(ctx)
	if err != nil {
		log.Fatalf("error getting Messaging client: %v\n", err)

		return nil, err
	}

	return client, nil
}

func getMessageFromFile(filePath string) (*NotificationMessage, error) {
	var message NotificationMessage

	messageJsonBytes, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Error reading/opening message file:\nFile path: %s\nError: %v\n", filePath, err)
		return nil, err
	}

	err = json.Unmarshal(messageJsonBytes, &message)
	if err != nil {
		log.Fatalf("Error Unmarshal json message from file:\nFile path: %s\nError: %v\n", filePath, err)
		return nil, err
	}

	err = message.marshalNotificationData()
	if err != nil {
		log.Fatalf("Error marshal the notification data field `data` from file:\nFile path: %s\nError: %v\n", filePath, err)
		return nil, err
	}

	return &message, nil

}

func startWorker(client *messaging.Client, tokensFile *os.File, message *NotificationMessage) {

	scanner := bufio.NewScanner(tokensFile)

	var wg sync.WaitGroup

	tokenCount := 0
	registrationTokens := make([]string, 0, kTokensCountPerWorker)

	var sem = make(chan int, kSemaphoreCount)

	for scanner.Scan() {
		token := scanner.Text()

		if len(token) <= 100 {
			continue
		}

		registrationTokens = append(registrationTokens, token)
		tokenCount++

		if tokenCount == kTokensCountPerWorker {
			tokenCount = 0

			registrationTokensCopy := make([]string, kTokensCountPerWorker)
			copy(registrationTokensCopy, registrationTokens)
			registrationTokens = make([]string, 0, kTokensCountPerWorker)

			sem <- 1
			wg.Add(1)
			go func() {
				defer wg.Done()
				sendFCMMessages(client, registrationTokensCopy, message)
				<-sem
			}()

		}
	}

	// wait until all goroutine finish
	wg.Wait()

	// the remaining tokens that do not adds up to kTokensCountPerReq
	if tokenCount > 0 {
		sendFCMMessages(client, registrationTokens, message)
		tokenCount = 0
		registrationTokens = nil
	}

}

func sendFCMMessages(client *messaging.Client, registrationTokens []string, messageData *NotificationMessage) {

	message := &messaging.MulticastMessage{
		Tokens: registrationTokens,
		Data:   *messageData.Data,
		Notification: &messaging.Notification{
			Title:    messageData.Title,
			Body:     messageData.Body,
			ImageURL: messageData.ImageURL,
		},
		Android: &messaging.AndroidConfig{
			TTL:         &TTL,
			Priority:    "high",
			CollapseKey: messageData.CollapseKey,
			Notification: &messaging.AndroidNotification{
				Priority: messaging.PriorityMax,
			},
		},
		APNS: &messaging.APNSConfig{
			Payload: &messaging.APNSPayload{
				Aps: &messaging.Aps{
					ContentAvailable: true,
				},
			},
		},
	}

	var br *messaging.BatchResponse
	var err error

	if messageData.ValidateOnly {
		br, err = client.SendEachForMulticastDryRun(context.Background(), message)
	} else {
		br, err = client.SendEachForMulticast(context.Background(), message)
	}

	if err != nil {
		log.Fatalf("Error can not send notifications error in function: SendEachForMulticastDryRun: %s\n", err)
	}

	log.Printf("count of tokens that caused failures: %v\n", br.FailureCount)
	log.Printf("count of tokens that success: %v\n", br.SuccessCount)
	// log.Printf("IsUnregistered: %v\n", messaging.IsUnregistered(br.Responses[0].Error))

	// if br.FailureCount > 0 {
	// var failedTokens []string
	// var successTokens []string
	// for _, resp := range br.Responses {
	// if !resp.Success {

	// The order of responses corresponds to the order of the registration tokens.
	// failedTokens = append(failedTokens, registrationTokens[idx])
	// } else {
	// successTokens = append(successTokens, registrationTokens[idx])
	// }
	// }

	// log.Printf("List of tokens that caused failures: %v\n", failedTokens)

	// log.Printf("List of tokens that success: %v\n", successTokens)
	// }

}

type NotificationMessage struct {
	Title          string         `json:"title"`
	Body           string         `json:"body"`
	DataTempHolder map[string]any `json:"data"`
	Data           *map[string]string
	ImageURL       string `json:"image_url"`
	CollapseKey    string `json:"collapse_key"`
	ValidateOnly   bool   `json:"validate_only"`
}

func (n *NotificationMessage) marshalNotificationData() error {
	marshaledMap := make(map[string]string)

	for k, v := range n.DataTempHolder {
		if reflect.ValueOf(v).Kind() == reflect.String {
			marshaledMap[k] = v.(string)
			continue
		}

		if reflect.ValueOf(v).Kind() == reflect.Int {
			marshaledMap[k] = strconv.Itoa(v.(int))
			continue
		}

		if reflect.ValueOf(v).Kind() == reflect.Float64 {
			marshaledMap[k] = strconv.FormatFloat(v.(float64), 'f', -1, 64)
			continue
		}

		if reflect.ValueOf(v).Kind() == reflect.Bool {
			marshaledMap[k] = strconv.FormatBool(v.(bool))
			continue
		}

		if reflect.ValueOf(v).Kind() == reflect.Map {
			marshaledSubMap, errMarshalingSubMap := json.Marshal(v)

			if errMarshalingSubMap != nil {
				log.Fatal("can not marshal sub map["+k+"]:", v, "Error:", errMarshalingSubMap)
				n.Data = nil
				return errMarshalingSubMap
			}

			marshaledMap[k] = string(marshaledSubMap)
			continue
		}
	}

	n.Data = &marshaledMap

	return nil
}
