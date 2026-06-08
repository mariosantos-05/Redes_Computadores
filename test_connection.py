import asyncio

from peer_connection import PeerConnection


async def main():
    
    print("Trying to connect...")

    conn = PeerConnection("teste@CIC")


    #Tentando conectar ao peer configurado do professor  (sabemos que ele esta em formato de "mirror")
    await conn.connect(
        "45.171.101.167",
        8081
    )

    asyncio.create_task(
        conn.listen()
    )
    print("Connected!")

asyncio.run(main())