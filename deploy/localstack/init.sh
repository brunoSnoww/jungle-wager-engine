#!/usr/bin/env bash
set -euo pipefail
# Safe to repeat: CreateQueue returns the existing queue with equal attributes.
# The local emulator uses deliberately non-production credentials.
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
for name in wager-transactions-dlq.fifo wager-events-dlq.fifo; do
  awslocal sqs create-queue --queue-name "$name" --attributes FifoQueue=true,ContentBasedDeduplication=false >/dev/null
done
for stem in wager-transactions wager-events; do
  queue_url=$(awslocal sqs create-queue --queue-name "$stem.fifo" --attributes FifoQueue=true,ContentBasedDeduplication=false,VisibilityTimeout=60,ReceiveMessageWaitTimeSeconds=20 --query QueueUrl --output text)
  dead_url=$(awslocal sqs get-queue-url --queue-name "$stem-dlq.fifo" --query QueueUrl --output text)
  dead_arn=$(awslocal sqs get-queue-attributes --queue-url "$dead_url" --attribute-names QueueArn --query Attributes.QueueArn --output text)
  awslocal sqs set-queue-attributes --queue-url "$queue_url" --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$dead_arn\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}" >/dev/null
done
# External providers receive HTTP credentials only, never broker credentials.
# Production applies IAM roles in addition to these resource policies. LocalStack
# Community is not evidence that cloud IAM enforcement has been tested.
awslocal iam get-user --user-name jungle-producer >/dev/null 2>&1 || awslocal iam create-user --user-name jungle-producer >/dev/null
awslocal iam put-user-policy --user-name jungle-producer --policy-name trusted-wager-producer --policy-document file:///provision/producer-policy.json >/dev/null
awslocal iam get-user --user-name jungle-service >/dev/null 2>&1 || awslocal iam create-user --user-name jungle-service >/dev/null
awslocal iam put-user-policy --user-name jungle-service --policy-name jungle-runtime --policy-document file:///provision/runtime-policy.json >/dev/null
input_url=$(awslocal sqs get-queue-url --queue-name wager-transactions.fifo --query QueueUrl --output text)
awslocal sqs set-queue-attributes --queue-url "$input_url" --attributes file:///provision/queue-policy-attributes.json >/dev/null
printf 'Jungle FIFO queues, redrive and local IAM policy fixtures ready.\n'
