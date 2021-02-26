#!/usr/bin/env python
from __future__ import print_function
import boto3
import os
import json

# default
source_queue_name = None
destination_queue_name = None
moveMessage = None
deleteMessage = None

def requeueAllFromEnviron():
    source_queue_name = os.environ['SOURCE_Q_NAME'] if 'SOURCE_Q_NAME' in os.environ else None
    destination_queue_name = os.environ['DEST_Q_NAME'] if 'DEST_Q_NAME' in os.environ else None
    moveMessage = everyMessage
    deleteMessage = moveMessage
    return source_queue_name, destination_queue_name, moveMessage, deleteMessage

def requeueAllFromDeadLetter():
    source_queue_name = 'worker-production2-dead-letter-queue'
    destination_queue_name = 'worker-production2-queue'
    moveMessage = everyMessage
    deleteMessage = moveMessage
    return source_queue_name, destination_queue_name, moveMessage, deleteMessage

def requeueAllFromDeadLetterInvitationList():
    source_queue_name = 'be-production2-invitation-list-messages-deadletter-queue'
    destination_queue_name = 'be-production2-invitation-list-messages-queue'
    moveMessage = everyMessage
    deleteMessage = moveMessage
    return source_queue_name, destination_queue_name, moveMessage, deleteMessage

def requeueAllFromDeadLetterEAS():
    source_queue_name = 'eas-production2-dead-letter-queue'
    destination_queue_name = 'eas-production2-queue'
    moveMessage = everyMessage
    deleteMessage = moveMessage
    return source_queue_name, destination_queue_name, moveMessage, deleteMessage

def requeueAllFromDeadLetterStaging1():
    source_queue_name = 'worker-staging1-dead-letter-queue'
    destination_queue_name = 'worker-staging1-queue'
    moveMessage = everyMessage
    deleteMessage = moveMessage
    return source_queue_name, destination_queue_name, moveMessage, deleteMessage

def requeueAllFromDeadLetterStaging1WithoutDeleting():
    source_queue_name = 'worker-staging1-dead-letter-queue'
    destination_queue_name = 'worker-staging1-queue'
    moveMessage = everyMessage
    deleteMessage = noMessage
    return source_queue_name, destination_queue_name, moveMessage, deleteMessage

def requeueOnlyWebhookPushFromDeadLetter():
    source_queue_name = 'worker-production2-dead-letter-queue'
    destination_queue_name = 'worker-production2-queue'
    moveMessage = onlyWebhookMessages
    deleteMessage = moveMessage
    return source_queue_name, destination_queue_name, moveMessage, deleteMessage

def requeueNonWebhookPushFromDeadLetter():
    source_queue_name = 'worker-production2-dead-letter-queue'
    destination_queue_name = 'worker-production2-queue'
    moveMessage = onlyNonWebhookMessages
    deleteMessage = moveMessage
    return source_queue_name, destination_queue_name, moveMessage, deleteMessage

def deleteRegenerateFromDeadLetter():
    source_queue_name = 'worker-production2-dead-letter-queue'
    destination_queue_name = 'worker-production2-queue'
    moveMessage = noMessage
    deleteMessage = onlyRegenerateMessages
    return source_queue_name, destination_queue_name, moveMessage, deleteMessage

def moveRegenerateFromWorkerQueueToDeadLetter():
    source_queue_name = 'worker-production2-queue'
    destination_queue_name = 'worker-production2-dead-letter-queue'
    moveMessage = onlyRegenerateMessages
    deleteMessage = moveMessage
    return source_queue_name, destination_queue_name, moveMessage, deleteMessage

def moveWebhookFromWorkerQueueToDeadLetter():
    source_queue_name = 'worker-production2-queue'
    destination_queue_name = 'worker-production2-dead-letter-queue'
    moveMessage = onlyWebhookMessages
    deleteMessage = moveMessage
    return source_queue_name, destination_queue_name, moveMessage, deleteMessage

def deleteWebhookFromDeadLetter():
    source_queue_name = 'worker-production2-dead-letter-queue'
    destination_queue_name = 'worker-production2-queue'
    moveMessage = noMessage
    deleteMessage = onlyWebhookMessages
    return source_queue_name, destination_queue_name, moveMessage, deleteMessage


#initialize = moveRegenerateFromWorkerQueueToDeadLetter
#initialize = moveWebhookFromWorkerQueueToDeadLetter
#initialize = deleteWebhookFromDeadLetter
initialize = requeueAllFromDeadLetter
#initialize = requeueNonWebhookPushFromDeadLetter
#initialize = requeueAllFromDeadLetterInvitationList
#initialize = requeueAllFromDeadLetterEAS


def noMessage(message):
    return False

def everyMessage(message):
    return True

def onlyWebhookMessages(message):
    message_json = json.loads(message.body)
    if 'task' in message_json and message_json['task'] == 'webhook/push':
        return True
    return False

def onlyNonWebhookMessages(message):
    message_json = json.loads(message.body)
    if 'task' in message_json and message_json['task'] == 'webhook/push':
        return False
    return True

def onlyRegenerateMessages(message):
    message_json = json.loads(message.body)
    if 'in' in message_json and 'regenerate' in message_json['in'] and message_json['in']['regenerate'] == True:
        return True
    return False


def handler(event, context):
    messages_moved = requeue_all_messages()

    response = {
        "statusCode": 200,
        "body": "Total messages moved: {}".format(messages_moved)
    }

    return response

def _get_sqs_queues():

    sqs = boto3.resource('sqs')

    destination_queue = sqs.get_queue_by_name(QueueName=destination_queue_name)
    source_queue = sqs.get_queue_by_name(QueueName=source_queue_name)

    return (destination_queue, source_queue)

def requeue_all_messages(max_messages_per_poll=10, poll_wait=20, visibility_timeout=20):
    destination_queue, source_queue = _get_sqs_queues()

    total_messages_moved = 0

    try:
        while True:
            messages = source_queue.receive_messages(
                                        MaxNumberOfMessages=max_messages_per_poll,
                                        WaitTimeSeconds=poll_wait,
                                        VisibilityTimeout=visibility_timeout)
            number_of_messages = len(messages)
            if number_of_messages == 0:
                print('No (more) messages in {} to check.'.format(source_queue_name))
                break
            else:
                print('Checking {:2d} from {}, '.format(number_of_messages, source_queue_name), end = "")
                requeued_messages_to_send = []
                requeued_messages_to_delete = []
                for message in messages:
                    if moveMessage(message):
                        requeued_messages_to_send.append({
                            'Id': message.message_id,
                            'MessageBody': message.body
                        })
                    if deleteMessage(message):
                        requeued_messages_to_delete.append({
                            'Id': message.message_id,
                            'ReceiptHandle': message.receipt_handle
                        })
                print('requeuing {:2d} to {}, '.format(len(requeued_messages_to_send), destination_queue_name), end = "")
                if requeued_messages_to_send:
                    #for i in range(1, 500): 
                        destination_queue.send_messages(Entries=requeued_messages_to_send)
                print('deleting {:2d} from {}.'.format(len(requeued_messages_to_delete), source_queue_name))
                if requeued_messages_to_delete:
                    source_queue.delete_messages(Entries=requeued_messages_to_delete)

            total_messages_moved += number_of_messages 
    except KeyboardInterrupt:
        pass

    return total_messages_moved

if __name__ == '__main__':
    source_queue_name, destination_queue_name, moveMessage, deleteMessage = initialize()
    requeue_all_messages()
