import asyncio
from config import Config
from peer_connection import PeerConnection

config = Config()

async def main():
    
    print("Trying to connect...")

    # Cria uma nova instância de conexão representando o peer local 'teste@CIC'
    conn = PeerConnection(f"{config.name}@{config.namespace}")

    # Tentando conectar ao peer configurado do professor (executa em modo "mirror" na porta 8081)
    await conn.connect(config.rdv_host, config.listen_port)

    # Cria uma tarefa assíncrona em background para rodar o loop de escuta de novas mensagens deste peer
    asyncio.create_task(
        conn.listen()
    )
    print("Connected!")

    # Mantém o script rodando para dar tempo de receber as mensagens em background
    await asyncio.sleep(5)

# Executa o loop de eventos assíncrono com a função de teste
asyncio.run(main())