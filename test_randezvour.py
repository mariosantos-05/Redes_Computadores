import asyncio

from randezvour_connection import RandezvousConnection


async def main():

    # Instancia o gerenciador de conexão com o servidor Rendezvous local/remoto de testes
    rdv = RandezvousConnection(
    "45.171.101.167",
    8080
    )

    # Mensagem de registro para o servidor Rendezvous
    register_msg = {

        "type": "REGISTER",
        "namespace": "CIC",
        "name": "teste",
        "port": 5000,
        "ttl": 3600 # Tempo de vida da entrada no servidor (1 hora)
    }

    # Envia a requisição de registro e aguarda a resposta (OK ou ERROR)
    response = await rdv.request(
        register_msg

    )
    print("Registration response:", response)


    # Mensagem de descoberta para encontrar outros peers ativos no mesmo namespace
    discorver_msg = {
        "type": "DISCOVER",
        "namespace": "",
    }

    # Envia a requisição de descoberta e aguarda a resposta contendo a lista de peers ativos
    response = await rdv.request(
        discorver_msg
    )

    # Exibe a lista de peers ativos retornada pelo servidor
    print("Discovery response:", response)


# Inicia a execução assíncrona do teste
asyncio.run(main())