import asyncio 
from peer_server import PeerServer


async def main():
    server = PeerServer("teste", 5000)
    

    await server.start()

asyncio.run(main())