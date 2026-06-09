import asyncio
from messages import decode_message
from messages import encode_message

class PeerServer:
    """
    Classe responsável por representar o servidor TCP de um nó (Peer).
    Ele aguarda conexões de outros peers e responde a mensagens de controle
    como HELLO e PING.
    """

    def __init__(self, peer_id, port):
        # Identificador do peer local (ex: 'alice@CIC')
        self.peer_id = peer_id
        # Porta TCP na qual o servidor irá escutar conexões de entrada
        self.port = port


    async def handle_client(self, reader, writer):
        """
        Método assíncrono executado para cada novo cliente/peer que se conecta a este servidor.
        Lê mensagens indefinidamente até a conexão ser encerrada.
        """
        while True:
            # Lê uma linha de dados (terminada em \n) enviada pelo peer conectado
            data = await reader.readline()

            # Se não houver dados retornados, significa que o cliente fechou a conexão TCP
            if not data:
                break 

            # Decodifica os bytes JSON para um dicionário Python
            msg = decode_message(data)
            

            # Trata o handshake inicial (mensagem do tipo HELLO)
            if msg["type"] == "HELLO":
                remote_peer = msg["peer_id"]
                print(f"Received HELLO from {remote_peer}")

                # Prepara a resposta de confirmação (HELLO_OK)
                response = {
                    "type": "HELLO_OK",
                    "peer_id": self.peer_id
                }

                # Envia a resposta codificada de volta ao cliente
                writer.write(
                    encode_message(response)
                )
                # Garante que os dados no buffer de escrita foram realmente enviados pela rede
                await writer.drain()

            
            # Trata o teste de atividade/keep-alive (mensagem do tipo PING)
            elif msg["type"] == "PING":
                print("PING RECEIVED")

                # Prepara a resposta (PONG) correspondente
                pong = {
                    "type": "PONG",
                }

                # Envia o PONG de volta para o cliente
                writer.write(
                    encode_message(pong)
                )
                print("PONG SENT")

    async def start(self):
        """
        Inicia o servidor TCP, vinculando-o ao endereço 0.0.0.0 e à porta configurada.
        Ele executará handle_client para cada nova conexão recebida.
        """
        server = await asyncio.start_server(
            self.handle_client,
            "0.0.0.0",
            self.port
        )

        print(f"listening on port {self.port}")
    
        # Mantém o servidor em execução indefinidamente para escutar novas conexões
        async with server:
            await server.serve_forever()

