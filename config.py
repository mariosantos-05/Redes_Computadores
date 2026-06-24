import json
import sys
import os

class Config:
    def __init__(self, filepath=None):
        if filepath is None:
            filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
        self.filepath = filepath
        self.load_config()
    def load_config(self):
        try:
            with open(self.filepath, "r") as file:
                self.data = json.load(file)
        except Exception as error:
            print(f"Warning: Could not load {self.filepath}, using default values. Error: {error}", file=sys.stderr)
            self.data = {}
        self.app_name = self.data.get("app_name", "pyp2p-chat")
        self.rdv_host = self.data.get("rdv_host", "rdv.mfcaetano.cc")
        self.rdv_port = self.data.get("rdv_port", 8080)
        self.listen_host = self.data.get("listen_host", "0.0.0.0")
        self.listen_port = self.data.get("listen_port", 8081)
        self.tcp_port = self.data.get("tcp_port", 5000)
        self.discover_interval = self.data.get("discover_interval", 20)
        self.keepalive_interval = self.data.get("keepalive_interval", 30)
        self.rdv_ttl = self.data.get("rdv_ttl", 3600)
        self.fixed_msg_ttl = self.data.get("fixed_msg_ttl", 1)
        self.namespace = self.data.get("namespace", "CIC")
        self.name = self.data.get("name", "Grupo_2")
        self.type = self.data.get("type", ["REGISTER", "DISCOVER"])
        self.log_level = self.data.get("log_level", "INFO")
        self.features = self.data.get("features", ["ack", "metrics"])
        self.autonomous_mode = self.data.get("autonomous_mode", False)
        self.max_reconnect_attempts = self.data.get("max_reconnect_attempts", 5)