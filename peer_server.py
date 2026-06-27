# ==============================================================================
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
# ==============================================================================

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
                
                # Trata mensagens comuns (PING, SEND, PUB, BYE) usando o router
                async def send_wrapper(data):
                    writer.write(encode_message(data))
                    await writer.drain()

                action = await process_common_messages(msg, self.peer_id, remote_peer, send_wrapper)
                
                if action == "BREAK":
                    break
                elif action == "HANDLED":
                    # Opcional: manter log de mensagens recebidas no servidor
                    if msg.get("type") in ["SEND", "PUB"]:
                        self.received_messages.append(msg)
                    continue

                # Processamento específico do Servidor (HELLO)
                if msg.get("type") == "HELLO":
                    remote_peer = msg.get("peer_id", "unknown")
                    remote_addr = writer.get_extra_info('peername')
                    logging.getLogger(__name__).info(f"[PeerServer] Inbound connected: {remote_peer} from {remote_addr}")

                    # Se estiver usando uma tabela de peers, registra o peer entrante
                    if self.peer_table:
                        self.peer_table.update_peer(remote_peer, remote_addr[0], remote_addr[1], msg.get("ttl", 3600))
                    
                    hello_ok = {
                        "type": "HELLO_OK",
                        "peer_id": self.peer_id,
                        "status": "OK",
                        "features": Config().features,
                        "ttl": Config().fixed_msg_ttl
                    }
                    writer.write(encode_message(hello_ok))
                    await writer.drain()

                else:
                    logging.getLogger(__name__).info(f"Received unknown structured msg from {remote_peer or 'unknown'}: {msg}")

        except Exception as e:
            logging.getLogger(__name__).error(f"Server error handling client {remote_peer or 'unknown'}: {e}")
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
            logging.getLogger(__name__).info(f"Connection with client {remote_peer or 'unknown'} closed")

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

        logging.getLogger(__name__).info(f"listening on port {self.port}")
    
        try:
            # Mantém o loop de eventos focado em servir o socket deste servidor para sempre,
            # impedindo que o programa termine e pare de aceitar conexões.
            await asyncio.Event().wait()
        finally:
            server.close()
            logging.getLogger(__name__).info(f"[PeerServer] Stopping server on port {self.port}, cancelling {len(self.active_tasks)} active client tasks...")
            for task in list(self.active_tasks):
                task.cancel()
            for w in list(self.active_connections):
                try:
                    w.close()
                except Exception:
                    pass
            await server.wait_closed()
