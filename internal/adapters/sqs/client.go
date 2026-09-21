// Package sqs implements broker I/O only. Financial effects belong to the shared
// application port. AWS credentials are resolved by the official SDK chain.
package sqs

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"jungle/internal/platform/config"
)

type Message struct {
	ID, Body, Receipt string
	ReceiveCount      int
}
type Client struct {
	api                                *awssqs.Client
	config                             config.Config
	input, output, inputDLQ, outputDLQ string
}

func New(c config.Config) (*Client, error) {
	// The SDK retries are additionally bounded by each operation's context.
	options, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(c.AWSRegion), awsconfig.WithRetryMaxAttempts(2))
	if err != nil {
		return nil, fmt.Errorf("AWS config: %w", err)
	}
	api := awssqs.NewFromConfig(options, func(o *awssqs.Options) {
		if c.SQSEndpoint != "" {
			o.BaseEndpoint = aws.String(c.SQSEndpoint)
		}
	})
	return &Client{api: api, config: c}, nil
}

func (c *Client) Start(ctx context.Context) error {
	for _, q := range []struct {
		name   string
		target *string
	}{{c.config.InputQueue, &c.input}, {c.config.OutputQueue, &c.output}, {c.config.InputDLQ, &c.inputDLQ}, {c.config.OutputDLQ, &c.outputDLQ}} {
		result, err := c.api.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(q.name)})
		if err != nil {
			return fmt.Errorf("resolve queue %s: %w", q.name, err)
		}
		*q.target = aws.ToString(result.QueueUrl)
		// LocalStack can return hostnames intended for containers to host clients.
		// Only an explicit development endpoint enables this fixed-host rewrite.
		if c.config.SQSEndpoint != "" {
			u, err := url.Parse(*q.target)
			if err != nil {
				return err
			}
			base, err := url.Parse(c.config.SQSEndpoint)
			if err != nil {
				return err
			}
			u.Scheme, u.Host = base.Scheme, base.Host
			*q.target = u.String()
		}
	}
	return c.Check(ctx)
}
func (c *Client) Check(ctx context.Context) error {
	if c.input == "" || c.output == "" {
		return fmt.Errorf("SQS not initialized")
	}
	for _, q := range []string{c.input, c.output} {
		if _, err := c.api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(q), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}}); err != nil {
			return fmt.Errorf("SQS readiness: %w", err)
		}
	}
	return nil
}

// LongPollWait is the server-side hold; callers bound the whole call above it.
const LongPollWait = 20 * time.Second

func (c *Client) Receive(ctx context.Context, max int) ([]Message, error) {
	r, err := c.api.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: aws.String(c.input), MaxNumberOfMessages: int32(max), WaitTimeSeconds: int32(LongPollWait / time.Second), VisibilityTimeout: int32(c.config.VisibilityTimeout / time.Second), MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount}})
	if err != nil {
		return nil, err
	}
	messages := make([]Message, 0, len(r.Messages))
	for _, m := range r.Messages {
		n, _ := strconv.Atoi(m.Attributes["ApproximateReceiveCount"])
		messages = append(messages, Message{ID: aws.ToString(m.MessageId), Body: aws.ToString(m.Body), Receipt: aws.ToString(m.ReceiptHandle), ReceiveCount: n})
	}
	return messages, nil
}
func (c *Client) Delete(ctx context.Context, receipt string) error {
	_, err := c.api.DeleteMessage(ctx, &awssqs.DeleteMessageInput{QueueUrl: aws.String(c.input), ReceiptHandle: aws.String(receipt)})
	return err
}
func (c *Client) Visibility(ctx context.Context, receipt string, delay time.Duration) error {
	_, err := c.api.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(c.input), ReceiptHandle: aws.String(receipt), VisibilityTimeout: int32(delay / time.Second)})
	return err
}
func (c *Client) Publish(ctx context.Context, eventID, aggregateID string, payload []byte) error {
	_, err := c.api.SendMessage(ctx, &awssqs.SendMessageInput{QueueUrl: aws.String(c.output), MessageBody: aws.String(string(payload)), MessageGroupId: aws.String(aggregateID), MessageDeduplicationId: aws.String(eventID)})
	return err
}

// DLQDepths reports both dead letter queues. The input one says our consumer
// rejected something; the output one says the consumers of our events did, and
// nobody here would otherwise notice that the integration boundary is stuck.
func (c *Client) DLQDepths(ctx context.Context) (map[string]int64, error) {
	depths := make(map[string]int64, 2)
	for _, q := range []struct{ name, url string }{{"input", c.inputDLQ}, {"output", c.outputDLQ}} {
		if q.url == "" {
			continue
		}
		r, err := c.api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(q.url), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages}})
		if err != nil {
			return depths, err
		}
		n, err := strconv.ParseInt(r.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)], 10, 64)
		if err != nil {
			return depths, err
		}
		depths[q.name] = n
	}
	return depths, nil
}
