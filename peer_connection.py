import asyncio
import time
from typing import Optional, Dict
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

    def __init__(self, peer_id, peer_table=None):
        # O identificador único do peer local (ex: 'Grupo2@CIC')
        self.peer_id = peer_id
        
        # Tabela de peers compartilhada
        self.peer_table = peer_table
        
        # Fluxo de entrada assíncrono para ler dados do socket do peer remoto
        self.reader: Optional[asyncio.StreamReader] = None
        
        # Fluxo de saída assíncrono para escrever dados no socket do peer remoto
        self.writer: Optional[asyncio.StreamWriter] = None
        
        # O ID do peer remoto ao qual nos conectamos (ex: 'bob@CIC')
        # É preenchido de forma dinâmica durante o handshake após receber o HELLO_OK
        self.remote_peer_id = None

        # Controle de Keep-Alive e RTT
        self.keepalive_task: Optional[asyncio.Task] = None
        self.last_ping_time: Optional[float] = None
        self.keepalive_interval = 30

        # Mapa de ACKs pendentes: msg_id -> asyncio.Event
        # Quando enviamos uma mensagem com require_ack=True, registramos um Event aqui.
        # O loop de escuta sinaliza o Event quando o ACK correspondente chegar.
        self._pending_acks: Dict[str, asyncio.Event] = {}

        # Timeout (em segundos) para aguardar um ACK antes de emitir aviso nos logs
        self.ack_timeout = 5.0

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
        Se a conexão cair ou houver timeout, o loop é interrompido e a limpeza de recursos é feita.
        """
        try:
            while True:
                # Lê bytes da rede com timeout
                try:
                    data = await asyncio.wait_for(
                        self.reader.readline(),
                        timeout=self.keepalive_interval * 2
                    )
                except asyncio.TimeoutError:
                    print(f"Connection timeout with {self.remote_peer_id}")
                    break
        
                # Se 'data' for vazio, o peer remoto fechou a conexão de forma limpa.
                # Encerramos o loop imediatamente.
                if not data:
                    print(f"Connection closed by remote peer: {self.remote_peer_id}")
                    break
                
                # Desserializa os bytes binários para um dicionário JSON legível do Python
                msg = decode_message(data)
        
                # Processamento de Keep-Alive
                if msg.get("type") == "PONG":
                    if self.last_ping_time is not None:
                        rtt_ms = (time.time() - self.last_ping_time) * 1000
                        print(f"[RTT] Measured RTT to {self.remote_peer_id}: {rtt_ms:.2f} ms")
                        if self.peer_table and self.remote_peer_id:
                            peer_entry = self.peer_table.get_peer(self.remote_peer_id)
                            if peer_entry:
                                peer_entry.add_rtt(rtt_ms)

                # Processamento de ACK: sinaliza o Event pendente para liberar o send_message
                elif msg.get("type") == "ACK":
                    msg_id = msg.get("msg_id")
                    if msg_id and msg_id in self._pending_acks:
                        self._pending_acks[msg_id].set()
                        print(f"[ACK] Received ACK for msg_id={msg_id} from {self.remote_peer_id}")
                    else:
                        print(f"[ACK] Received unexpected ACK (msg_id={msg_id}) from {self.remote_peer_id}")

                else:
                    # Exibe no terminal qualquer outra mensagem estruturada recebida
                    print("Received:", msg)
        except Exception as e:
            print(f"Connection error with {self.remote_peer_id}: {e}")
        finally:
            await self.close()
            if self.peer_table and self.remote_peer_id:
                # Atualiza estado para DISCONNECTED
                peer_entry = self.peer_table.get_peer(self.remote_peer_id)
                if peer_entry and peer_entry.status == "CONNECTED":
                    self.peer_table.mark_disconnected(self.remote_peer_id)


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
        self.last_ping_time = time.time()
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

        # Se a tabela de peers existe, marca como conectado
        if self.peer_table:
            self.peer_table.mark_connected(self.remote_peer_id)

        # Agenda a execução recorrente de PINGs em background
        self.keepalive_task = asyncio.create_task(self.keepalive_loop())

    async def send_message(self, content: str, msg_id: str, require_ack: bool = False):
        """
        Envia uma mensagem unicast do tipo SEND ao peer remoto.

        Parâmetros:
            content     : Texto da mensagem a ser enviada.
            msg_id      : Identificador único da mensagem (ex: UUID).
            require_ack : Se True, aguarda um ACK do destinatário por até 'ack_timeout' segundos.
                         Caso o ACK não chegue dentro do prazo, emite um aviso nos logs.

        Fluxo com require_ack=True:
        1. Um asyncio.Event é registrado em '_pending_acks' antes do envio.
        2. A mensagem SEND é transmitida com o campo 'require_ack': True.
        3. Aguardamos o Event ser sinalizado pelo loop de escuta quando o ACK chegar.
        4. Se expirar o timeout, registramos o aviso e limpamos o Event pendente.
        """
        msg = {
            "type": "SEND",
            "msg_id": msg_id,
            "src": self.peer_id,
            "dst": self.remote_peer_id,
            "payload": content,
            "require_ack": require_ack,
            "ttl": 1
        }

        if require_ack:
            # Registra o evento pendente ANTES de enviar para evitar race condition
            ack_event = asyncio.Event()
            self._pending_acks[msg_id] = ack_event

        await self.send(msg)
        print(f"[SEND] Message sent to {self.remote_peer_id} (msg_id={msg_id}, require_ack={require_ack})")

        if require_ack:
            try:
                await asyncio.wait_for(ack_event.wait(), timeout=self.ack_timeout)
            except asyncio.TimeoutError:
                print(
                    f"[ACK] WARNING: No ACK received from {self.remote_peer_id} "
                    f"for msg_id={msg_id} within {self.ack_timeout}s"
                )
            finally:
                # Garante limpeza do mapa independente do resultado
                self._pending_acks.pop(msg_id, None)

    async def publish(self, content: str, msg_id: str, scope: str):
        """
        Envia uma mensagem de difusão (broadcast) do tipo PUB a este peer remoto.

        O campo 'dst' indica o escopo da difusão:
            - '#namespace' : difusão para todos os peers do namespace atual.
            - '*'          : difusão global para todos os peers conectados.

        Parâmetros:
            content : Texto da mensagem a ser difundida.
            msg_id  : Identificador único da mensagem (ex: UUID).
            scope   : '#namespace' ou '*', conforme especificação do protocolo.

        Nota: Este método transmite a mensagem a UM peer por vez. O remetente é
        responsável por chamar 'publish()' em cada PeerConnection ativa para realizar
        a difusão completa (ver lógica de fan-out no nível da aplicação).
        """
        msg = {
            "type": "PUB",
            "msg_id": msg_id,
            "src": self.peer_id,
            "dst": scope,
            "payload": content,
            "require_ack": False,
            "ttl": 1
        }
        await self.send(msg)
        print(f"[PUB] Broadcast sent to {self.remote_peer_id} (scope={scope}, msg_id={msg_id})")

    async def keepalive_loop(self):
        """
        Loop periódico em background para enviar PINGs ao peer remoto.
        """
        try:
            while True:
                await asyncio.sleep(self.keepalive_interval)
                ping_msg = {
                    "type": "PING",
                    "peer_id": self.peer_id,
                    "version": "1.0",
                    "features": [],
                    "ttl": 1
                }
                self.last_ping_time = time.time()
                await self.send(ping_msg)
                print(f"[KeepAlive] Sent PING to {self.remote_peer_id}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[KeepAlive] Error sending PING to {self.remote_peer_id}: {e}")

    async def close(self):
        """
        Fecha a conexão TCP e cancela tarefas de background.
        """
        if self.keepalive_task:
            self.keepalive_task.cancel()
            self.keepalive_task = None
        
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
            self.writer = None
        self.reader = None