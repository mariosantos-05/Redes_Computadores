import asyncio 
import json
from peer_server import PeerServer
from peer_connection import PeerConnection
from randezvour_connection import RandezvousConnection


#Tudo que está hardocoded aqui deve ser modificado e ajustado em relação aos arquivos de configuração json.

async def main():
    rdv = RandezvousConnection(
    "45.171.101.167",
    8080
    )

    register_msg = {
        "type": "REGISTER",
        "namespace": "CIC",
        "name": "teste",
        "port": 5000,
        "ttl": 3600
    }

    response = await rdv.request(
        register_msg
    )


    discorver_msg = {
        "type": "DISCOVER",
        "namespace": "CIC",
    }

    response = await rdv.request(
        discorver_msg
    )

    print(response) #Isso aqui já é um dicionário Python, formatado json

    conn = PeerConnection("teste@CIC")

    print("Trying to connect...")
    for peer in response["peers"]:
        ip = peer['ip']
        port = peer['port']
        name = peer['name']
        if name != "teste":
            await conn.connect(ip, port)

            asyncio.create_task(
                conn.listen()
            )
        
        await asyncio.sleep(100)

    print("Connected!")


asyncio.run(main())