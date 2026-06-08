import asyncio
from messages import decode_message
from messages import encode_message

class RandezvousConnection:

    def __init__(self, host, port):
        self.host = host
        self.port = port

    async def request(self, msg):

        reader, writer = await asyncio.open_connection(
            self.host,
            self.port
        )

        writer.write(
            encode_message(msg)
        )

        await writer.drain()

        data = await reader.readline()

        response = decode_message(data)

        writer.close()
        await writer.wait_closed()

        return response