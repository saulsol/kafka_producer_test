cd /src/kafka-producer
source /src/kafka_venv/bin/activate
pip3 install poetry
poetry install
python3 /src/kafka-producer/deploy/replace_secret.py