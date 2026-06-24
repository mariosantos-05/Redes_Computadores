import asyncio 
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import Config
from peer_connection import PeerConnection
from rendezvous_connection import RendezvousConnection

async def main():

    config = Config()

    rdv = RendezvousConnection(
        config.rdv_host, 
        config.rdv_port
    )

    register_msg = {
        "type": config.type[0],
        "namespace": config.namespace,
        "name": config.name,
        "port": config.tcp_port,
        "ttl": config.rdv_ttl
    }


    response = await rdv.request(
        register_msg
    )


    discorver_msg = {
        "type": config.type[1],
        "namespace": config.namespace,
    }

    response = await rdv.request(
        discorver_msg
    )

    print(response) # Isso aqui já é um dicionário Python, formatado json

    conn = PeerConnection(f"{config.name}@{config.namespace}")

    print("Trying to connect...")
    for peer in response["peers"]:
        ip = peer['ip']
        port = peer['port']
        name = peer['name']
        if name != config.name:
            await conn.connect(ip, port)

            asyncio.create_task(
                conn.listen()
            )
        
        await asyncio.sleep(100)

    print("Connected!")


asyncio.run(main())