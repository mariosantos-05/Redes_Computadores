import asyncio
from csv import writer
from messages import decode_message
from messages import encode_message

class PeerServer:

    def __init__(self, peer_id, port):
        self.peer_id = peer_id
        self.port = port


    async def handle_client(self, reader, writer):
        

        while True:
            data = await reader.readline()

            if not data:
                break 

            msg = decode_message(data)
            

            if msg["type"] == "HELLO":

                remote_peer = msg["peer_id"]

                print(f"Received HELLO from {remote_peer}")

                response = {
                    "type": "HELLO_OK",
                    "peer_id": self.peer_id
                }

                writer.write(
                    encode_message(response)
                )

                await writer.drain()



    async def start(self):
        server = await asyncio.start_server(
            self.handle_client,
            "0.0.0.0",
            self.port
        )


        print(f"listening on port {self.port}")
    
        async with server:
            await server.serve_forever()
