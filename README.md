# PyCon2022-Micron
Continuous High Volume of Data Exporting From BigQuery

## Project Structure
```
.
|-- micron                 
    |-- services                  # service class to simplify the GCP service api
    |-- utils                     # util tools
|-- download_single_session.py    # demo: download data with single session
|-- download_multiple_sessions.py # demo: download data with multiple sessions
|-- download_pubsub.py            # demo: download data with pubsub pull client
```

## Development Environment In Linux Setup
1. Install [poetry](https://python-poetry.org/docs/)
2. Install [conda](https://confluence.micron.com/confluence/display/~JUSTINHUANG/Python+Development+Environment+Setup#PythonDevelopmentEnvironmentSetup-Conda)
3. Create an python venv
```bash
conda create -n pycon python==3.7.6
```
4. Activate python venv
```bash
conda activate pycon
```
5. Install all packages
```bash
poetry install
```
6. Install Pub/Sub emulator
```
sudo apt-get update
sudo apt install default-jdk

gcloud components install beta
gcloud components install pubsub-emulator
gcloud components update
```
7. Start the Pub/Sub emulator
```
export PUBSUB_PROJECT_ID=local-dev
gcloud beta emulators pubsub start \
    --project=$PUBSUB_PROJECT_ID \
    --host-port=localhost:8085
```
8. Init the submodule if you have not done before
```
git submodule init
git submodule update
```
9. Local Topic/Subscription setup
```
export PUBSUB_PROJECT_ID=local-dev
export TOPIC_ID=pycon
export SUBSCRIPTION_ID=$TOPIC_ID-consumer
$(gcloud beta emulators pubsub env-init)

python python-pubsub/samples/snippets/publisher.py $PUBSUB_PROJECT_ID create $TOPIC_ID
python python-pubsub/samples/snippets/subscriber.py $PUBSUB_PROJECT_ID create $TOPIC_ID $SUBSCRIPTION_ID
```
10. Consumer init
```
export PUBSUB_PROJECT_ID=local-dev
export TOPIC_ID=pycon
export SUBSCRIPTION_ID=$TOPIC_ID-consumer
$(gcloud beta emulators pubsub env-init)

python download_pubsub.py
```
11. Producer init
```
export PUBSUB_PROJECT_ID=local-dev
export TOPIC_ID=pycon
$(gcloud beta emulators pubsub env-init)

python micron/utils/publish_msg.py --dataset crypto_bitcoin --table blocks
```