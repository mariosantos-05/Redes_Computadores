import asyncio 
from config import Config
from peer_server import PeerServer

config = Config()

async def main():
    # Instancia o servidor de peer informando seu ID (teste) e a porta TCP local (5000)
    server = PeerServer(config.name, config.tcp_port)
    
    # Inicia a escuta do servidor assincronamente e aguarda novas conexões infinitamente
    await server.start()

# Executa o loop de eventos assíncrono do Python com a função principal 'main'
asyncio.run(main())