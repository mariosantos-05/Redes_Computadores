# ================================================================
# PROJETO: Chat P2P (Versão Revisada)
# DISCIPLINA: Redes de Computadores
# INSTITUIÇÃO: Universidade de Brasília (UnB) - CIC
# GRUPO: 2
#
# MEMBROS DA EQUIPE:
# - Gabriel Gonçalves Caldo (Matrícula: 231034627)
# - Daniel Rodrigues de Abreu (Matrícula: 241038540)
# - Mario Augusto Vieira dos Santos (Matrícula: 231035778)
#
# ARQUIVO: peer_server.py
# ================================================================

import asyncio
import logging
from typing import Optional
from peer_table import PeerTable
# Nota Importante: O import 'writer' do módulo csv não é utilizado neste arquivo. Ele provavelmente 
# foi importado por engano no lugar do StreamWriter do asyncio. Mantemos aqui por questões de compatibilidade
# com o rascunho inicial do usuário, mas adicionamos esse alerta.
from message_router import decode_message, encode_message, process_common_messages
from config import Config
import uuid
import datetime
import sys
import readline

class PeerServer:
    """
    Representa o servidor TCP local deste nó (Peer).
    
    O que este servidor faz?
    Ele abre uma porta TCP local em seu computador e fica escutando em segundo plano. Quando outros peers
    na rede (ex: Alice ou Bob) descobrem você através do Rendezvous, eles usam o 'PeerConnection' deles 
    para se conectar à porta do seu 'PeerServer'.
    Esta classe gerencia a recepção e resposta de handshakes (HELLO) e mensagens de controle (PING/PONG).
    """

    def __init__(self, peer_id, port, peer_table: Optional[PeerTable] = None, shared_connections: Optional[dict] = None):
        # Identificador do peer local (ex: 'alice@CIC'). Usado para se apresentar a outros peers no handshake.
        self.peer_id = peer_id
        # Porta de rede local onde o servidor irá escutar conexões de entrada (ex: 5000)
        self.port = port
        # Tabela de peers compartilhada para gerenciar os estados das conexões
        self.peer_table = peer_table
        # Dicionário de conexões ativas compartilhado com o cliente/CLI
        self.shared_connections = shared_connections
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
            # 1. O primeiro pacote deve ser obrigatoriamente um HELLO de handshake
            try:
                data = await asyncio.wait_for(reader.readline(), timeout=5.0)
            except asyncio.TimeoutError:
                logging.getLogger(__name__).warning("[PeerServer] Timeout de 5s aguardando HELLO. Fechando conexão.")
                return
            if not data:
                return

            msg = decode_message(data)

            if msg.get("type") != "HELLO":
                logging.getLogger(__name__).warning("[PeerServer] Conexão rejeitada: primeira mensagem não é HELLO")
                return

            remote_peer = msg.get("peer_id", "desconhecido")
            remote_addr = writer.get_extra_info('peername')
            logging.getLogger(__name__).info(f"[PeerServer] Conexão de entrada de {remote_peer} em {remote_addr}")

            # --- EVITAR CONEXÕES DUPLICADAS ---
            # if self.shared_connections is not None and remote_peer in self.shared_connections:
            #     existing_conn = self.shared_connections[remote_peer]
            #     if existing_conn != "connecting":
            #         logging.getLogger(__name__).warning(f"[PeerServer] Conexão duplicada detectada para {remote_peer}. Rejeitando nova conexão.")
            #         return

            # Registra o peer na tabela
            if self.peer_table:
                self.peer_table.update_peer(remote_peer, remote_addr[0], remote_addr[1], msg.get("ttl", 3600))

            # Envia HELLO_OK
            hello_ok = {
                "type": "HELLO_OK",
                "peer_id": self.peer_id,
                "status": "OK",
                "features": Config().features,
                "ttl": Config().fixed_msg_ttl
            }
            writer.write(encode_message(hello_ok))
            await writer.drain()

            # Delega o gerenciamento e a escuta da conexão para a classe PeerConnection
            if self.shared_connections is not None:
                from peer_connection import PeerConnection
                inbound_conn = PeerConnection(self.peer_id, self.peer_table)
                inbound_conn.reader = reader
                inbound_conn.writer = writer
                inbound_conn.remote_peer_id = remote_peer

                # Agenda envio periódico de keep-alive (PING) para o peer
                inbound_conn.keepalive_task = asyncio.create_task(inbound_conn.keepalive_loop())
                self.shared_connections[remote_peer] = inbound_conn

                try:
                    await inbound_conn.listen()
                finally:
                    self.shared_connections.pop(remote_peer, None)

        except Exception as e:
            logging.getLogger(__name__).error(f"Erro no servidor ao lidar com cliente {remote_peer or 'desconhecido'}: {e}")
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
            logging.getLogger(__name__).info(f"Conexão com cliente {remote_peer or 'desconhecido'} fechada")

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

        logging.getLogger(__name__).info(f"Escutando na porta {self.port}")
    
        try:
            # Mantém o loop de eventos focado em servir o socket deste servidor para sempre,
            # impedindo que o programa termine e pare de aceitar conexões.
            await asyncio.Event().wait()
        finally:
            server.close()
            logging.getLogger(__name__).info(f"[PeerServer] Parando servidor na porta {self.port}, cancelando {len(self.active_tasks)} tarefas de clientes ativos...")
            for task in list(self.active_tasks):
                task.cancel()
            for w in list(self.active_connections):
                try:
                    w.close()
                except Exception:
                    pass
            await server.wait_closed()
