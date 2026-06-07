import asyncio

from peer_connection import PeerConnection


async def main():

    conn = PeerConnection("bob@CIC")
    
    print("Trying to connect...")

    await conn.connect(
        "127.0.0.1",
        5000
    )

    print("Connected!")

asyncio.run(main())