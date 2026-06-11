import asyncio 
from peer_server import PeerServer


async def main():
    # Instancia o servidor de peer informando seu ID (teste) e a porta TCP local (5000)
    server = PeerServer("teste", 5000)
    
    # Inicia a escuta do servidor assincronamente e aguarda novas conexões infinitamente
    await server.start()

# Executa o loop de eventos assíncrono do Python com a função principal 'main'
asyncio.run(main())