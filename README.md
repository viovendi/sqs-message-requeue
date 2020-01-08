# SQS Message Requeue

Python function for handling SQS Messages.

Heavily inspired by https://github.com/tomelliff/sqs-dead-letter-requeue

## Requirements

* Python 2.7

## Running it locally

### Local setup/configuration

Make sure you have AWS credentials configured and a default region specified.

This can be with environment variables:

```sh
export AWS_ACCESS_KEY_ID=<my-access-key>
export AWS_SECRET_ACCESS_KEY=<my-secret-key>
export AWS_DEFAULT_REGION=eu-central-1
```

or setting them in either an AWS credentials file (~/.aws/credentials) or AWS config file (~/.aws/config):

You will also need to set the name of the main queue to be requeued:

```sh
export DEST_Q_NAME=<my-queue-name>
export SOURCE_Q_NAME=<my-queue-name>
```

### Virtualenv/dependencies
```sh
virtualenv env
. env/bin/activate
pip install boto3
```

### Run it
check current definition of `initialize` in `requeue.py`
```sh
python requeue.py
```
