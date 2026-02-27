import boto3
import json
import time

sns = boto3.client('sns', region_name='us-east-1')
sqs = boto3.client('sqs', region_name='us-east-1')

# Create a temporary queue
queue_name = 'test-nbm-queue-' + str(int(time.time()))
q_res = sqs.create_queue(QueueName=queue_name)
q_url = q_res['QueueUrl']
q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=['QueueArn'])['Attributes']['QueueArn']

policy = {
    "Version": "2012-10-17",
    "Statement": [{
        "Sid": "AllowSNS",
        "Effect": "Allow",
        "Principal": {"Service": "sns.amazonaws.com"},
        "Action": "sqs:SendMessage",
        "Resource": q_arn,
        "Condition": {
            "ArnEquals": {"aws:SourceArn": "arn:aws:sns:us-east-1:123901341784:NewNBMCOGObject"}
        }
    }]
}
sqs.set_queue_attributes(QueueUrl=q_url, Attributes={'Policy': json.dumps(policy)})

# Subscribe without filter
sub_res = sns.subscribe(
    TopicArn="arn:aws:sns:us-east-1:123901341784:NewNBMCOGObject",
    Protocol='sqs',
    Endpoint=q_arn
)
sub_arn = sub_res['SubscriptionArn']

print(f"Subscribed to NBM. Queue: {queue_name}. Polling...")
messages = []
for _ in range(6):
    resp = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=10)
    messages.extend(resp.get("Messages", []))
    if messages:
        break

for m in messages:
    print(m['Body'])

sns.unsubscribe(SubscriptionArn=sub_arn)
sqs.delete_queue(QueueUrl=q_url)
print("Done")
