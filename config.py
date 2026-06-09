import json
import sys

class Config:
    def __init__(self, filepath="config.json"):
        self.filepath = filepath
        self.load_config()
    def load_config(self):
        try:
            with open(self.filepath, "r") as file:
                data = json.load(file)
        except Exception as error:
            print(f"Warning: Could not load {self.filepath}, using default values. Error: {error}", file=sys.stderr)
            data = {}
        self.app_name = data.get("app_name", "pyp2p-chat")
        self.rdv_host = data.get("rdv_host", "rdv.mfcaetano.cc")
        self.rdv_port = data.get("rdv_port", 8080)
        self.listen_host = data.get("listen_host", "0.0.0.0")
        self.listen_port = data.get("listen_port", 8081)
        self.tcp_port = data.get("tcp_port", 5000)
        self.discover_interval = data.get("discover_interval", 20)
        self.keepalive_interval = data.get("keepalive_interval", 30)
        self.rdv_ttl = data.get("rdv_ttl", 7200)
        self.fixed_msg_ttl = data.get("fixed_msg_ttl", 1)
        self.namespace = data.get("namespace", "CIC")
        self.name = data.get("name", "Grupo_2")
        self.type = data.get("type", ["REGISTER", "DISCOVER"])
        self.log_level = data.get("log_level", "INFO")
        self.features = data.get("features", ["ack", "metrics"])
        self.autonomous_mode = data.get("autonomous_mode", False)
        self.max_reconnect_attempts = data.get("max_reconnect_attempts", 5)