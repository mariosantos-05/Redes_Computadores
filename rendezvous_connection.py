import asyncio
from message_router import decode_message
from message_router import encode_message

class RendezvousConnection:
    """
    Classe que representa e gerencia a comunicação com o Servidor Rendezvous.
    Seguindo o protocolo especificado, cada interação com o Rendezvous é feita
    através de uma conexão TCP curta de comando único (envia uma linha e encerra).
    """

    def __init__(self, host, port):
        # Endereço IP/Host do servidor Rendezvous (ex: '45.171.101.167')
        self.host = host
        # Porta TCP do servidor Rendezvous (ex: 8080)
        self.port = port

    async def request(self, msg):
        """
        Envia uma requisição única para o servidor Rendezvous, aguarda o retorno,
        fecha a conexão imediatamente e retorna a resposta decodificada.
        """
        # Abre uma nova conexão TCP com o servidor Rendezvous
        reader, writer = await asyncio.open_connection(
            self.host,
            self.port
        )

        # Envia a mensagem codificada (JSON + '\n')
        writer.write(
            encode_message(msg)
        )

        # Garante o envio físico dos dados
        await writer.drain()

        # Lê a única linha de resposta enviada pelo servidor
        data = await reader.readline()

        # Decodifica a resposta JSON para dicionário Python
        response = decode_message(data)

        # Fecha a conexão TCP imediatamente após obter a resposta (requisito do protocolo)
        writer.close()
        await writer.wait_closed()

        return response