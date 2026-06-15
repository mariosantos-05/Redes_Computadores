import asyncio
from config import Config
from peer_connection import PeerConnection

async def main():
    
    print("Trying to connect...")

    # Cria uma nova instância de conexão representando o peer local
    conn = PeerConnection(f"{Config().name}@{Config().namespace}")

    # Tentando conectar ao peer configurado do professor
    await conn.connect(Config().rdv_host, Config().listen_port)

    # Cria uma tarefa assíncrona em background para rodar o loop de escuta de novas mensagens deste peer
    asyncio.create_task(
        conn.listen()
    )
    print("Connected!")

    # Mantém o script rodando para dar tempo de receber as mensagens em background
    await asyncio.sleep(5)

# Executa o loop de eventos assíncrono com a função de teste
asyncio.run(main())