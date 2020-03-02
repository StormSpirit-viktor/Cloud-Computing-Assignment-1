import boto3
import traceback

max_message_len = 10

class SQS(object):
    def __init__(self, name):
        self.queue = boto3.resource('sqs').get_queue_by_name(QueueName=name)

    def send_message(self, data):
        try:
            response = self.queue.send_message(
                MessageBody=data
            )
        except Exception as e:
            traceback.print_exc()
            return None

        return response

    def get_messages(self):
        ret = []
        for message in self.queue.receive_messages(MaxNumberOfMessages=max_message_len):
            ret.append(message.body)
            message.delete()
        return ret


def lambda_handler(event, context):
    fifo = SQS(name='chatbot-orders.fifo')
    return fifo.get_messages()