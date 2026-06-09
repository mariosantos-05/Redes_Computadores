import asyncio
from config import Config
from randezvour_connection import RandezvousConnection

config = Config()

async def main():

    # Instancia o gerenciador de conexão com o servidor Rendezvous local/remoto de testes
    rdv = RandezvousConnection(config.rdv_host, config.rdv_port)

    # Mensagem de registro para o servidor Rendezvous
    register_msg = {
        "type": config.type[0],
        "namespace": config.namespace,
        "name": config.name,
        "port": config.tcp_port,
        "ttl": config.rdv_ttl # Tempo de vida da entrada no servidor (1 hora)
    }

    # Envia a requisição de registro e aguarda a resposta (OK ou ERROR)
    response = await rdv.request(
        register_msg
    )
    print("Registration response:", response)


    # Mensagem de descoberta para encontrar outros peers ativos no mesmo namespace
    discorver_msg = {
        "type": config.type[1],
        "namespace": config.namespace,
    }

    # Envia a requisição de descoberta e aguarda a resposta contendo a lista de peers ativos
    response = await rdv.request(
        discorver_msg
    )

    # Exibe a lista de peers ativos retornada pelo servidor
    print("Discovery response:", response)


# Inicia a execução assíncrona do teste
asyncio.run(main())