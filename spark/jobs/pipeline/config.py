# kafka broker
KAFKA_BOOTSTRAP = "kafka:9092"

# input topics (raw messsages from crawlers)
TOPIC_RAW_REDDIT = "raw.reddit"
TOPIC_RAW_YOUTUBE = "raw.youtube"

# output topics (clean, validated messsages)
TOPIC_CLEAN = "clean.posts"

# dead-letter queue (bad records: malformed json, ...)
TOPIC_DLQ = "dlq.posts"

# checkpoint location (per query)
CHECKPOINT_DIR = "/tmp/spark-checkpoints/pipeline"

