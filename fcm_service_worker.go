package main

import (
	"bufio"
	"context"
	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
	"log"
	"os"
	"sync"
	"time"
)

const kTokensCountPerReq = 500 // 0> && <= 500
const kSemaphoreCount = 1
const tokensFilePath = "./fake_tokens"

func main() {
	client, err := initFirebaseClient()
	if err != nil {
		log.Fatalf("Error unable to init firebase client/app\n")
		return
	}

	file, err := os.Open(tokensFilePath)
	if err != nil {
		log.Fatalf("Error reading/opening tokens file:\nFile path: %s\nError: %v\n", tokensFilePath, err)
		return
	}

	scanner := bufio.NewScanner(file)

	var wg sync.WaitGroup

	tokenCount := 0
	registrationTokens := make([]string, 0, kTokensCountPerReq)

	var sem = make(chan int, kSemaphoreCount)

	for scanner.Scan() {
		token := scanner.Text()

		if len(token) <= 100 {
			continue
		}

		registrationTokens = append(registrationTokens, token)
		tokenCount++

		if tokenCount == kTokensCountPerReq {
			tokenCount = 0

			registrationTokensCopy := make([]string, kTokensCountPerReq)
			copy(registrationTokensCopy, registrationTokens)
			registrationTokens = make([]string, 0, kTokensCountPerReq)

			sem <- 1
			wg.Add(1)
			go func() {
				defer wg.Done()
				sendFCMMessages(client, registrationTokensCopy)
				<-sem
			}()

		}
	}

	// wait until all goroutine finish
	wg.Wait()

	// the remaining tokens that do not adds up to kTokensCountPerReq
	if tokenCount > 0 {
		sendFCMMessages(client, registrationTokens)
		tokenCount = 0
		registrationTokens = nil
	}

	log.Println("=============================Done======================")

}

func initFirebaseClient() (*messaging.Client, error) {
	ctx := context.Background()
	app, err := firebase.NewApp(ctx, nil, option.WithCredentialsFile("./credentials_file.json"))

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

func sendFCMMessages(client *messaging.Client, registrationTokens []string) {

	var oneWeekDurationTTL time.Duration = 60 * 60 * 24 * 7

	message := &messaging.MulticastMessage{
		Tokens: registrationTokens,
		Data: map[string]string{
			"score": "850",
			"time":  "2:45",
		},

		Notification: &messaging.Notification{
			Title: "hi",
			Body:  "hi from body",
		},
		Android: &messaging.AndroidConfig{
			TTL:         &oneWeekDurationTTL,
			Priority:    "high",
			CollapseKey: "",
			Notification: &messaging.AndroidNotification{
				Priority: messaging.PriorityHigh,
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

	// TODO: remove the dryRun
	br, err := client.SendEachForMulticastDryRun(context.Background(), message)

	if err != nil {
		log.Fatalf("Error can not send notifications error in function: SendEachForMulticastDryRun: %s\n",err)
	}

	log.Printf("count of tokens that caused failures: %v\n", br.FailureCount)
	log.Printf("count of tokens that success: %v\n", br.SuccessCount)
	log.Printf("br.Responses[0].Error: %v\n", br.Responses[0].Error)
	log.Printf("IsUnregistered: %v\n", messaging.IsUnregistered(br.Responses[0].Error))

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
