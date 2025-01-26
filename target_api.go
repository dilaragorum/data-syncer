package datasyncer

import (
	"bytes"
	"fmt"
	"github.com/hashicorp/go-retryablehttp"
	"net"
	"net/http"
	"time"
)

type ErrorHandler func(data []byte, err error)

type ParseLineFuncHandler func(line []byte) []interface{}

type TargetApiOption func(*targetApi)

type targetApi struct {
	client           http.Client
	url              string
	errorHandlerFunc ErrorHandler

	template             string
	parseLineFuncHandler ParseLineFuncHandler
}

func NewTargetApi(url string, opts ...TargetApiOption) DataTarget {
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConns:        300,
		MaxIdleConnsPerHost: 300,
		MaxConnsPerHost:     300,
		DialContext: (&net.Dialer{
			Timeout:   60 * time.Second,
			KeepAlive: 150 * time.Second,
		}).DialContext,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	retryClient := retryablehttp.NewClient()
	retryClient.HTTPClient.Transport = transport
	retryClient.RetryMax = 5
	httpClient := *retryClient.StandardClient()

	t := &targetApi{
		client: httpClient,
		url:    url,
	}

	for _, opt := range opts {
		opt(t)
	}

	return t
}

func WithErrorHandler(handler ErrorHandler) TargetApiOption {
	return func(t *targetApi) {
		t.errorHandlerFunc = handler
	}
}

func WithTemplateHandler(template string, parseLineFuncHandler ...ParseLineFuncHandler) TargetApiOption {
	return func(t *targetApi) {
		t.template = template

		if len(parseLineFuncHandler) > 0 && parseLineFuncHandler[0] != nil {
			t.parseLineFuncHandler = parseLineFuncHandler[0]
		} else {
			t.parseLineFuncHandler = func(line []byte) []interface{} {
				return []interface{}{line}
			}
		}
	}
}

func (t *targetApi) Send(input <-chan []byte) error {
	for data := range input {
		var reqBody []byte

		if t.template != "" && t.parseLineFuncHandler != nil {
			args := t.parseLineFuncHandler(data)
			formattedReq := fmt.Sprintf(t.template, args...)
			reqBody = []byte(formattedReq)
		} else {
			reqBody = data
		}

		resp, err := t.client.Post(t.url, "application/json", bytes.NewBuffer(reqBody))
		if err != nil {
			if t.errorHandlerFunc != nil {
				t.errorHandlerFunc(data, err)
			} else {
				fmt.Printf("Error posting record: %v\n", err)
			}
			continue
		}
		resp.Body.Close()

		if resp.StatusCode >= 300 {
			if t.errorHandlerFunc != nil {
				t.errorHandlerFunc(data, err)
			} else {
				fmt.Printf("received non-success status code: %d\", resp.StatusCode")
			}
			continue
		}

		fmt.Println("Successfully sent message")
	}

	return nil
}
