import asyncio
from config import Config
from rendezvous_connection import RendezvousConnection

async def main():

    # Instancia o gerenciador de conexão com o servidor Rendezvous local/remoto de testes
    rdv = RendezvousConnection(
        Config().rdv_host, 
        Config().rdv_port
    )

    # Mensagem de registro para o servidor Rendezvous
    register_msg = {
        "type": Config().type[0],
        "namespace": Config().namespace,
        "name": Config().name,
        "port": Config().tcp_port,
        "ttl": Config().rdv_ttl
    }

    # Envia a requisição de registro e aguarda a resposta (OK ou ERROR)
    response = await rdv.request(
        register_msg

    )
    print("Registration response:", response)


    # Mensagem de descoberta para encontrar outros peers ativos no mesmo namespace
    discorver_msg = {
        "type": Config().type[1],
        "namespace": Config().namespace,
    }

    # Envia a requisição de descoberta e aguarda a resposta contendo a lista de peers ativos
    response = await rdv.request(
        discorver_msg
    )

    # Exibe a lista de peers ativos retornada pelo servidor
    print("Discovery response:", response)


# Inicia a execução assíncrona do teste
asyncio.run(main())