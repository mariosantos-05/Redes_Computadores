import asyncio
from message_router import decode_message
from message_router import encode_message

class PeerConnection:
    """
    Classe que gerencia uma conexão de saída ativa de um peer para outro.
    Controla o estabelecimento de socket TCP, envio de mensagens e
    recebimento de dados assíncronos.
    """

    def __init__(self, peer_id):
        # Identificador do peer local (ex: 'alice@CIC')
        self.peer_id = peer_id
        # Objeto StreamReader do asyncio, usado para ler dados do socket
        self.reader = None
        # Objeto StreamWriter do asyncio, usado para escrever dados no socket
        self.writer = None
        # Identificador do peer remoto com quem estamos conectados
        self.remote_peer_id = None

    async def send(self, msg):
        """
        Envia uma mensagem (dicionário Python) para o peer remoto.
        Codifica em JSON+bytes e envia pelo socket TCP.
        """
        self.writer.write(
            encode_message(msg)
        )
        # Força o envio dos dados pendentes no buffer do socket
        await self.writer.drain()

    async def listen(self):
        """
        Loop de escuta assíncrona. Lê continuamente do peer remoto até a conexão fechar.
        """
        while True:
            # Lê dados até encontrar uma quebra de linha '\n'
            data = await self.reader.readline()
    
            # Se não houver dados, o peer remoto fechou a conexão
            if not data:
                break
            
            # Decodifica a mensagem recebida
            msg = decode_message(data)
    
            print("Received:", msg)


    async def connect(self, host, port):
        """
        Estabelece conexão TCP com um peer remoto, inicia o handshake P2P (HELLO e PING)
        e aguarda a confirmação (HELLO_OK).
        """
        # Abre conexão TCP com o endereço IP (host) e porta especificados
        self.reader, self.writer = await asyncio.open_connection(
            host,
            port
        )

        print("TCP connection established")

        # Cria a mensagem de handshake inicial HELLO
        hello_msg = {
            "type": "HELLO",
            "peer_id": self.peer_id,
            "version": "1.0",
            "features": [],
            "ttl": 1
        }

        # Cria uma mensagem PING imediata (teste de keep-alive)
        ping_msg = {
            "type": "PING",
            "peer_id": self.peer_id,
            "version": "1.0",
            "features": [],
            "ttl": 1
        }
    
        # Envia a mensagem HELLO
        await self.send(hello_msg)

        # Envia a mensagem PING logo em seguida
        await self.send(ping_msg)


        #await self.send("batata, teste string pura")

        # Garante que as mensagens foram transmitidas
        await self.writer.drain()

        print("HELLO sent")
    

        # Aguarda a resposta do servidor/peer remoto
        data = await self.reader.readline()
        
        # Decodifica a mensagem de resposta
        msg = decode_message(data)
        
        print(msg)
    

        # Verifica se o peer remoto aceitou a conexão com um HELLO_OK
        if msg["type"] != "HELLO_OK":
            raise Exception(
                f"Expected HELLO_OK, got {msg['type']}"
            )
        
        # Salva o identificador do peer remoto
        self.remote_peer_id = msg["peer_id"]
    
        print(f"Connected to {self.remote_peer_id}")