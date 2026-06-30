import json
import os
from pathlib import Path

class KafkaError(Exception):
    pass

class Producer:
    def __init__(self, config):
        self.config = config
        self.messages = []
        self.callbacks = []

    def produce(self, topic, key=None, value=None, callback=None, **kwargs):
        # Decode key and value
        decoded_key = key.decode("utf-8") if isinstance(key, bytes) else key
        decoded_val = None
        if value:
            try:
                decoded_val = json.loads(value.decode("utf-8"))
            except Exception:
                decoded_val = value.decode("utf-8") if isinstance(value, bytes) else value

        msg = {
            "topic": topic,
            "key": decoded_key,
            "value": decoded_val
        }
        self.messages.append(msg)

        # Handle produce failure
        error_to_report = None
        if os.getenv("MOCK_KAFKA_PRODUCE_FAILURE") == "true":
            error_to_report = "Simulated produce delivery error"

        if callback:
            self.callbacks.append((callback, error_to_report, msg))

    def poll(self, timeout=0):
        # Trigger callbacks
        while self.callbacks:
            callback, error, msg = self.callbacks.pop(0)
            callback(error, msg)
        return 0

    def flush(self, timeout=None):
        self.poll(0)

        # Write messages to MOCK_DATA_DIR/kafka_messages.json
        mock_dir_env = os.getenv("MOCK_DATA_DIR")
        if mock_dir_env:
            mock_dir = Path(mock_dir_env)
            mock_dir.mkdir(parents=True, exist_ok=True)
            msg_file = mock_dir / "kafka_messages.json"
            
            # Read existing messages
            existing_messages = []
            if msg_file.exists():
                try:
                    with open(msg_file, "r", encoding="utf-8") as f:
                        existing_messages = json.load(f)
                        if not isinstance(existing_messages, list):
                            existing_messages = []
                except Exception:
                    pass
            
            existing_messages.extend(self.messages)
            
            with open(msg_file, "w", encoding="utf-8") as f:
                json.dump(existing_messages, f, indent=2, ensure_ascii=False)

        self.messages = []

        if os.getenv("MOCK_KAFKA_FLUSH_FAILURE") == "true":
            return 1
        return 0
