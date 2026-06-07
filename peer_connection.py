import asyncio
from messages import decode_message
from messages import encode_message



class PeerConnection:

    def __init__(self, peer_id):
        self.peer_id = peer_id
        self.reader = None
        self.writer = None
        self.remote_peer_id = None

    async def send(self, msg):

        self.writer.write(
            encode_message(msg)
        )

        await self.writer.drain()

    async def listen(self):

        while True:
            data = await self.reader.readline()
    
            if not data:
                break
            
            msg = decode_message(data)
    
            print("Received:", msg)




    async def connect(self, host, port):

        self.reader, self.writer = await asyncio.open_connection(
            host,
            port
        )

        print("TCP connection established")

        hello_msg = {
            "type": "HELLO",
            "peer_id": self.peer_id,
            "version": "1.0",
            "features": [],
            "ttl": 1
        }

        ping_msg = {
            "type": "PING",
            "peer_id": self.peer_id,
            "version": "1.0",
            "features": [],
            "ttl": 1
        }
    
        await self.send(hello_msg)

        await self.send(ping_msg)

        await self.writer.drain()

        print("HELLO sent")
    

        data = await self.reader.readline()
        
        msg = decode_message(data)
        
        print(msg)
    

        if msg["type"] != "HELLO_OK":
            raise Exception(
                f"Expected HELLO_OK, got {msg['type']}"
            )
        
        self.remote_peer_id = msg["peer_id"]
    
        print(f"Connected to {self.remote_peer_id}")