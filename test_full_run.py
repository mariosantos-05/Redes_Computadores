import asyncio 
from peer_server import PeerServer
from config import Config
from peer_connection import PeerConnection
from rendezvous_connection import RendezvousConnection

async def main():
    rdv = RendezvousConnection(
        Config().rdv_host, 
        Config().rdv_port
    )

    register_msg = {
        "type": Config().type[0],
        "namespace": Config().namespace,
        "name": Config().name,
        "port": Config().tcp_port,
        "ttl": Config().rdv_ttl
    }


    response = await rdv.request(
        register_msg
    )


    discorver_msg = {
        "type": Config().type[1],
        "namespace": Config().namespace,
    }

    response = await rdv.request(
        discorver_msg
    )

    print(response) # Isso aqui já é um dicionário Python, formatado json

    conn = PeerConnection(f"{Config().name}@{Config().namespace}")

    print("Trying to connect...")
    for peer in response["peers"]:
        ip = peer['ip']
        port = peer['port']
        name = peer['name']
        if name != Config().name:
            await conn.connect(ip, port)

            asyncio.create_task(
                conn.listen()
            )
        
        await asyncio.sleep(100)

    print("Connected!")


asyncio.run(main())