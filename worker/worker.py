import json
import time
import logging
from kafka import KafkaConsumer, KafkaProducer
import redis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

KAFKA_BROKER = 'localhost:9092'
STAGE_READY_TOPIC = 'stage-ready'
STAGE_COMPLETED_TOPIC = 'stage-completed'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
LOCK_TTL = 30  # seconds
MAX_RETRIES = 3

# Connect to Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Kafka consumer
consumer = KafkaConsumer(
    STAGE_READY_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id='gopherflow-python-worker',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

log.info("Python worker started. Listening for stages...")

def acquire_lock(stage_id):
    lock_key = f"python:lock:{stage_id}"
    return r.set(lock_key, "1", nx=True, ex=LOCK_TTL)

def release_lock(stage_id):
    r.delete(f"python:lock:{stage_id}")

def get_retry_count(stage_id):
    val = r.get(f"stage:retry:{stage_id}")
    return int(val) if val else 0

def increment_retry(stage_id):
    r.incr(f"stage:retry:{stage_id}")

def clear_retry(stage_id):
    r.delete(f"stage:retry:{stage_id}")

def execute_stage(event):
    stage_id = event['stageId']
    stage_name = event['stageName']
    workflow_id = event['workflowId']

    # Try to acquire distributed lock
    if not acquire_lock(stage_id):
        log.warning(f"Stage {stage_name} already being processed — skipping")
        return

    attempt = get_retry_count(stage_id) + 1
    log.info(f"Executing stage: {stage_name} (attempt {attempt})")

    try:
        # Simulate stage execution
        time.sleep(0.2)

        # Success — clear retry counter and release lock
        clear_retry(stage_id)
        release_lock(stage_id)
        log.info(f"Stage completed: {stage_name}")

        # Publish completion event
        completed_event = {
            'workflowId': workflow_id,
            'stageId': stage_id,
            'stageName': stage_name
        }
        producer.send(STAGE_COMPLETED_TOPIC, completed_event)
        producer.flush()
        log.info(f"Published completion for stage: {stage_name}")

    except Exception as e:
        log.error(f"Stage {stage_name} failed: {e}")
        increment_retry(stage_id)
        release_lock(stage_id)

        if attempt < MAX_RETRIES:
            log.info(f"Retrying stage: {stage_name} (attempt {attempt}/{MAX_RETRIES})")
            # Re-queue
            producer.send(STAGE_READY_TOPIC, event)
            producer.flush()
        else:
            log.error(f"Stage {stage_name} exhausted retries — sending to DLQ")
            clear_retry(stage_id)
            dlq_event = {
                'workflowId': workflow_id,
                'stageId': stage_id,
                'stageName': stage_name,
                'errorMessage': str(e),
                'attemptNumber': attempt
            }
            producer.send('stage-dead-letter', dlq_event)
            producer.flush()

# Main loop
for message in consumer:
    event = message.value
    log.info(f"Received stage: {event.get('stageName')}")
    execute_stage(event)