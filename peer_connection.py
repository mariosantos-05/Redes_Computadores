import asyncio
from typing import Optional
from message_router import decode_message
from message_router import encode_message

class PeerConnection:
    """
    Gerencia uma conexão de rede TCP ativa de saída do seu cliente para um peer remoto.
    
    O que esta classe faz?
    Quando o seu programa descobre outro peer ativo (ex: Bob) e você deseja falar com ele, você instancia
    um 'PeerConnection'. Ele é responsável por:
    1. Abrir a conexão TCP física (`asyncio.open_connection`).
    2. Realizar o handshake do protocolo enviando a mensagem 'HELLO' e esperando 'HELLO_OK'.
    3. Manter a escuta em segundo plano para capturar as mensagens de chat que aquele peer te enviar.
    """

    def __init__(self, peer_id):
        # O identificador único do peer local (ex: 'caldo@CIC')
        self.peer_id = peer_id
        
        # Fluxo de entrada assíncrono para ler dados do socket do peer remoto
        self.reader: Optional[asyncio.StreamReader] = None
        
        # Fluxo de saída assíncrono para escrever dados no socket do peer remoto
        self.writer: Optional[asyncio.StreamWriter] = None
        
        # O ID do peer remoto ao qual nos conectamos (ex: 'bob@CIC')
        # É preenchido de forma dinâmica durante o handshake após receber o HELLO_OK
        self.remote_peer_id = None

    async def send(self, msg: dict):
        """
        Codifica e envia uma mensagem JSON estruturada pelo canal de saída do socket.
        """
        # Escreve os bytes serializados da mensagem (terminada em \n) no buffer interno
        self.writer.write(
            encode_message(msg)
        )
        # O drain() é uma corrotina assíncrona que esvazia o buffer de rede.
        # Ele suspende esta função temporariamente até que todos os bytes tenham sido gravados
        # fisicamente na placa de rede, garantindo que a mensagem foi de fato transmitida.
        await self.writer.drain()

    async def listen(self):
        """
        Loop contínuo de escuta executado em background para receber dados enviados por este peer.
        
        Como funciona:
        Uma vez estabelecida a conexão, esta função roda infinitamente em uma tarefa em segundo plano.
        Ela lê cada linha terminada em '\n' enviada pelo peer remoto, decodifica e imprime o conteúdo.
        Se a conexão cair, o loop é interrompido e a limpeza de recursos deve ser feita.
        """
        while True:
            # Lê bytes da rede até encontrar o terminador '\n'
            data = await self.reader.readline()
    
            # Se 'data' for vazio, o peer remoto fechou a conexão de forma limpa.
            # Encerramos o loop imediatamente.
            if not data:
                break
            
            # Desserializa os bytes binários para um dicionário JSON legível do Python
            msg = decode_message(data)
    
            # Exibe no terminal a mensagem estruturada recebida do parceiro de chat
            print("Received:", msg)


    async def connect(self, host: str, port: int):
        """
        Conecta-se ativamente ao IP e porta informados, executando o handshake de apresentação P2P.
        
        O fluxo de conexão e handshake funciona da seguinte forma:
        1. Cria um socket TCP de cliente e estabelece a conexão física.
        2. Envia um HELLO para se apresentar (informando seu peer_id local).
        3. Envia um PING inicial (para testes e medições de conexão).
        4. Aguarda a primeira resposta do outro lado. De acordo com a especificação, esta resposta
           DEVE ser do tipo 'HELLO_OK' para autorizar a continuidade do chat.
        5. Salva a identidade confirmada do peer remoto.
        """
        # Abre a conexão TCP de forma assíncrona. Retorna o leitor e o escritor do socket.
        self.reader, self.writer = await asyncio.open_connection(
            host,
            port
        )

        print("TCP connection established")

        # Prepara a mensagem de identificação 'HELLO' conforme os padrões do protocolo
        hello_msg = {
            "type": "HELLO",
            "peer_id": self.peer_id,
            "version": "1.0",
            "features": [],
            "ttl": 1
        }

        # Prepara uma mensagem inicial de PING de keep-alive
        ping_msg = {
            "type": "PING",
            "peer_id": self.peer_id,
            "version": "1.0",
            "features": [],
            "ttl": 1
        }
    
        # Envia a apresentação (HELLO)
        await self.send(hello_msg)

        # Envia o teste de atividade (PING)
        await self.send(ping_msg)

        # Aguarda a transmissão física de ambos os pacotes
        await self.writer.drain()

        print("HELLO sent")
    
        # O programa fica pausado de forma assíncrona aguardando a resposta do peer remoto
        data = await self.reader.readline()
        
        # Converte a resposta em dicionário Python
        msg = decode_message(data)
        
        print(msg)
    
        # Validação do Handshake:
        # Se o peer remoto responder com qualquer coisa diferente de HELLO_OK,
        # a conexão é considerada inválida pela regra do protocolo e lançamos um erro.
        if msg["type"] != "HELLO_OK":
            raise Exception(
                f"Expected HELLO_OK, got {msg['type']}"
            )
        
        # Salva o identificador (ex: 'bob@CIC') que o peer remoto informou no seu HELLO_OK
        self.remote_peer_id = msg["peer_id"]
    
        print(f"Connected to {self.remote_peer_id}")