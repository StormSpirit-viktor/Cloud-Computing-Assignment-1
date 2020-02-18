import json
import boto3
import traceback

class SQS(object):
    def __init__(self, name):
        self.queue = boto3.resource('sqs').get_queue_by_name(QueueName=name)

    def send_message(self, data):
        try:
            response = self.queue.send_message(
                MessageBody=data,
                MessageGroupId='group1',
            )
        except Exception as e:
            traceback.print_exc()
            return None

        return response

    def get_messages(self):
        ret = []
        for message in self.queue.receive_messages():
            ret.append(message.body)
            message.delete()
        return ret


def lambda_handler(event, context):

    info = dict()

    slots = event['currentIntent']['slots']

    # get conversation info
    info['cuisine'] = slots['Cuisine']
    info['phone'] = slots['Phone']
    info['amount'] = slots['Amount']
    info['time'] = slots['Time']
    info['date'] = slots['Date']
    info['location'] = slots['Location']

    # put info in SQS
    fifo = SQS(name='chatbot-orders.fifo')
    fifo.send_message(json.dumps(info))

    # this is how to read
    # data = fifo.get_messages()
    # for d in data:
    #     print(json.loads(d))

    return {
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": "Fulfilled",
            "message": {
                "contentType": "PlainText",
                "content": "Great! You are all set! Expect my suggestions shortly and hope you enjoy you meal!"
            },
        }
    }