import asyncio
from typing import Optional
from peer_table import PeerTable
# Nota Importante: O import 'writer' do módulo csv não é utilizado neste arquivo. Ele provavelmente 
# foi importado por engano no lugar do StreamWriter do asyncio. Mantemos aqui por questões de compatibilidade
# com o rascunho inicial do usuário, mas adicionamos esse alerta.
from csv import writer
from message_router import decode_message
from message_router import encode_message

class PeerServer:
    """
    Representa o servidor TCP local deste nó (Peer).
    
    O que este servidor faz?
    Ele abre uma porta TCP local em seu computador e fica escutando em segundo plano. Quando outros peers
    na rede (ex: Alice ou Bob) descobrem você através do Rendezvous, eles usam o 'PeerConnection' deles 
    para se conectar à porta do seu 'PeerServer'.
    Esta classe gerencia a recepção e resposta de handshakes (HELLO) e mensagens de controle (PING/PONG).
    """

    def __init__(self, peer_id, port, peer_table: Optional[PeerTable] = None):
        # Identificador do peer local (ex: 'alice@CIC'). Usado para se apresentar a outros peers no handshake.
        self.peer_id = peer_id
        # Porta de rede local onde o servidor irá escutar conexões de entrada (ex: 5000)
        self.port = port
        # Tabela de peers compartilhada para gerenciar os estados das conexões
        self.peer_table = peer_table
        # Conexões ativas de clientes (inbound)
        self.active_connections = set()
        # Tarefas ativas de clientes (inbound)
        self.active_tasks = set()
        # Mensagens recebidas (histórico de SEND/PUB) para observabilidade e testes
        self.received_messages = []

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Método de callback assíncrono que o asyncio executa automaticamente toda vez que um novo peer se conecta.
        
        Como funciona a concorrência assíncrona aqui?
        Para cada cliente que se conecta, o asyncio cria uma nova tarefa e executa este método em segundo plano.
        Isso permite que múltiplos peers conversem com você simultaneamente sem que um bloqueie o outro.
        
        Parâmetros:
            reader: Fluxo de leitura assíncrono (StreamReader) para receber dados enviados por este peer.
            writer: Fluxo de escrita assíncrono (StreamWriter) para enviar dados de volta a este peer.
        """
        remote_peer = None
        self.active_connections.add(writer)
        current_task = asyncio.current_task()
        if current_task:
            self.active_tasks.add(current_task)
        try:
            while True:
                # 1. Leitura com enquadramento (Framing):
                # O 'readline()' lê os bytes recebidos da rede até encontrar o delimitador de quebra de linha ('\n').
                # Isso é fundamental porque, como o TCP é um fluxo contínuo, ler uma linha garante que pegamos 
                # exatamente uma única mensagem JSON completa por vez.
                data = await reader.readline()

                # 2. Detecção de Desconexão:
                # Se 'data' vir vazio (bytes vazios), significa que o peer remoto fechou a conexão de forma
                # limpa na outra ponta (EOF - End Of File). Encerramos o loop para liberar os recursos do socket.
                if not data:
                    break 

                # 3. Desserialização:
                # Traduz os bytes recebidos para um dicionário Python contendo os campos da especificação do protocolo.
                msg = decode_message(data)
                

                # 4. Trata o handshake inicial (HELLO):
                # De acordo com a especificação, antes de trocar mensagens normais de chat, as duas pontas precisam
                # se identificar. O peer que iniciou a chamada envia um "HELLO".
                if msg.get("type") == "HELLO":
                    remote_peer = msg.get("peer_id")
                    print(f"Received HELLO from {remote_peer}")

                    # Criamos a mensagem de sucesso (HELLO_OK) contendo nossa própria identidade
                    response = {
                        "type": "HELLO_OK",
                        "peer_id": self.peer_id
                    }

                    # Escreve a resposta codificada em JSON + bytes no buffer de saída do socket
                    writer.write(
                        encode_message(response)
                    )
                    # O 'writer.write' apenas coloca os dados no buffer local do Python.
                    # Precisamos usar 'await writer.drain()' para forçar o envio físico dos bytes pela rede,
                    # pausando esta tarefa de forma assíncrona até que o envio termine.
                    await writer.drain()

                    # Atualiza a tabela de peers para marcar como conectado
                    if self.peer_table and remote_peer:
                        self.peer_table.mark_connected(remote_peer)

                
                # 5. Trata mensagens de Keep-Alive (PING):
                # Periodicamente (a cada 30 segundos), o cliente de outro peer vai te enviar um "PING" para ver se 
                # seu servidor ainda está vivo e medir o tempo de ida e volta (RTT).
                elif msg.get("type") == "PING":
                    print(f"PING RECEIVED from {remote_peer or 'unknown'}")

                    # Prepara a mensagem de resposta correspondente
                    pong = {
                        "type": "PONG",
                    }

                    # Escreve o PONG no buffer do socket
                    writer.write(
                        encode_message(pong)
                    )
                    # Força a transmissão física para garantir medição de RTT precisa no cliente
                    await writer.drain()
                    print(f"PONG SENT to {remote_peer or 'unknown'}")

                # 6. Trata mensagens unicast (SEND):
                # Outro peer enviou uma mensagem direta para este nó. Imprimimos o conteúdo e,
                # se o campo 'require_ack' estiver ativo, respondemos com um ACK para confirmar
                # o recebimento ao remetente.
                elif msg.get("type") == "SEND":
                    sender  = msg.get("src", remote_peer or "unknown")
                    content = msg.get("payload", "")
                    msg_id  = msg.get("msg_id")
                    print(f"[SEND] {sender}: {content}")
                    self.received_messages.append(msg)

                    if msg.get("require_ack") and msg_id:
                        import datetime
                        timestamp = datetime.datetime.utcnow().isoformat() + "Z"
                        ack = {
                            "type": "ACK",
                            "msg_id": msg_id,
                            "timestamp": timestamp,
                            "ttl": 1
                        }
                        writer.write(encode_message(ack))
                        await writer.drain()
                        print(f"[ACK] Sent ACK for msg_id={msg_id} to {sender}")

                # 7. Trata mensagens de difusão (PUB):
                # Outro peer enviou uma mensagem de broadcast. O campo 'dst' indica o escopo:
                # '#namespace' para difusão no namespace atual ou '*' para difusão global.
                elif msg.get("type") == "PUB":
                    sender  = msg.get("src", remote_peer or "unknown")
                    content = msg.get("payload", "")
                    scope   = msg.get("dst", "*")
                    print(f"[PUB] [{scope}] {sender}: {content}")
                    self.received_messages.append(msg)

        except Exception as e:
            print(f"Server error handling client {remote_peer or 'unknown'}: {e}")
        finally:
            self.active_connections.discard(writer)
            current_task = asyncio.current_task()
            if current_task:
                self.active_tasks.discard(current_task)
            # Garante que o socket seja fechado de forma limpa
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            print(f"Connection with client {remote_peer or 'unknown'} closed")

            # Atualiza o status do peer na tabela se ele estava conectado
            if self.peer_table and remote_peer:
                peer_entry = self.peer_table.get_peer(remote_peer)
                if peer_entry and peer_entry.status == "CONNECTED":
                    self.peer_table.mark_disconnected(remote_peer)

    async def start(self):
        """
        Inicializa e inicia o servidor TCP local, colocando-o em estado de escuta (listening).
        
        Como funciona:
        1. 'asyncio.start_server' cria o socket TCP do sistema operacional e o vincula (bind) à porta especificada.
        2. O endereço '0.0.0.0' significa que o servidor aceitará conexões vindas de qualquer placa de rede
           disponível no seu computador (tanto localhost/127.0.0.1 quanto conexões de rede externa).
        3. Registramos o 'self.handle_client' como a corrotina que cuidará de cada conexão.
        """
        server = await asyncio.start_server(
            self.handle_client,
            "0.0.0.0",
            self.port
        )

        print(f"listening on port {self.port}")
    
        try:
            # Mantém o loop de eventos focado em servir o socket deste servidor para sempre,
            # impedindo que o programa termine e pare de aceitar conexões.
            await asyncio.Event().wait()
        finally:
            server.close()
            print(f"[PeerServer] Stopping server on port {self.port}, cancelling {len(self.active_tasks)} active client tasks...")
            for task in list(self.active_tasks):
                task.cancel()
            for w in list(self.active_connections):
                try:
                    w.close()
                except Exception:
                    pass
            await server.wait_closed()
