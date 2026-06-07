import json 


def encode_message(data : dict) -> str:
    """Tranfsorma dicionario em string json."""
    return (json.dumps(data) + "\n").encode()

def decode_message(raw : bytes) -> dict:
    return json.loads(raw.decode())

