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
# ARQUIVO: cli.py
# ==============================================================================

import asyncio
import sys
import logging
import uuid
import readline # Habilita teclas de seta e histórico para input()
from typing import Dict
from peer_connection import PeerConnection
from peer_table import PeerTable

async def async_input(prompt: str) -> str:
    """
    Lê a entrada do usuário de forma assíncrona usando run_in_executor e a função input().
    Como o módulo readline já foi importado, ele automaticamente gerencia o histórico de
    comandos e navegação por setas.
    """
    loop = asyncio.get_event_loop()
    # Executa a função síncrona input() em uma thread separada para não bloquear o event loop
    return await loop.run_in_executor(None, input, prompt)

from logging_config import ReadlineConsoleHandler

async def cli_loop(
    peer_id: str,
    peer_table: PeerTable,
    outbound_connections: Dict[str, PeerConnection],
    inbound_connections: Dict[str, PeerConnection],
    shutdown_event: asyncio.Event
):
    """
    Loop interativo da Interface de Linha de Comando (CLI).
    Permite que o usuário insira comandos em tempo real sem interromper as rotinas
    de rede que executam em plano de fundo.
    """
    logger = logging.getLogger(__name__)
    await asyncio.sleep(1.0) # Aguarda 1 segundo para garantir que os logs iniciais (ex: registro) passem

    ReadlineConsoleHandler.cli_active = False

    while not shutdown_event.is_set():
        try:
            ReadlineConsoleHandler.cli_active = True
            line = await async_input("p2p> ")
            ReadlineConsoleHandler.cli_active = False
            if shutdown_event.is_set():
                break
            
            line = line.strip()
            if not line:
                continue

            parts = line.split(" ", 2)
            cmd = parts[0].lower()

            if cmd == "/peers":
                filter_arg = parts[1] if len(parts) > 1 else None
                peers = peer_table.get_all_peers()
                print("--- Peers Conhecidos ---")
                for p in peers:
                    if filter_arg == "*" or filter_arg is None:
                        pass
                    elif filter_arg.startswith("#"):
                        if p.namespace != filter_arg[1:]:
                            continue
                    print(f"[{p.status}] {p.peer_id} em {p.ip}:{p.port} (TTL: {p.ttl})")
                print("-------------------")

            elif cmd == "/msg":
                if len(parts) < 3:
                    print("Uso: /msg <peer_id> <mensagem>")
                    continue
                target_id = parts[1]
                msg_content = parts[2]
                
                # Verifica se já possuímos uma conexão TCP ativa com este peer
                conn = outbound_connections.get(target_id)
                
                # Se não possuir, tenta descobrir o peer e estabelecer a conexão
                if conn is None:
                    peer_entry = peer_table.get_peer(target_id)
                    if not peer_entry:
                        print(f"Peer desconhecido: {target_id}. Tente usar /peers para descobrir novos peers.")
                        continue
                    
                    if peer_entry.status == "CONNECTED" or target_id in outbound_connections:
                        print(f"Você já possui uma conexão ativa ou em andamento com o peer: {target_id}")
                        continue
                    
                    print(f"Conectando a {target_id} em {peer_entry.ip}:{peer_entry.port}...")
                    conn = PeerConnection(
                        peer_id,
                        peer_table,
                        on_close=lambda c: outbound_connections.pop(c.remote_peer_id, None) if c.remote_peer_id else None
                    )
                    try:
                        await conn.connect(peer_entry.ip, peer_entry.port)
                        # Inicia a tarefa de escuta contínua para essa nova conexão
                        asyncio.create_task(conn.listen())
                        outbound_connections[target_id] = conn
                    except Exception as e:
                        print(f"Falha ao conectar: {e}")
                        continue
                
                # Envia a mensagem com um UUID único
                msg_id = str(uuid.uuid4())
                try:
                    await conn.send_message(msg_content, msg_id, require_ack=True)
                except Exception as e:
                    print(f"Falha ao enviar mensagem: {e}")
                    if target_id in outbound_connections:
                        del outbound_connections[target_id]

            elif cmd == "/pub":
                if len(parts) < 3:
                    print("Uso: /pub <* | #namespace> <mensagem>")
                    continue
                scope = parts[1]
                msg_content = parts[2]
                msg_id = str(uuid.uuid4())
                
                # Identifica todos os peers que correspondem ao escopo selecionado (global ou namespace)
                target_peers = []
                for p in peer_table.get_all_peers():
                    if scope == "*":
                        target_peers.append(p)
                    elif scope.startswith("#") and p.namespace == scope[1:]:
                        target_peers.append(p)

                count = 0
                for p in target_peers:
                    # Reaproveita a conexão ativa, se já existir
                    conn = outbound_connections.get(p.peer_id)
                    if conn is None:
                        if p.status == "CONNECTED" or p.peer_id in outbound_connections:
                            continue
                        # Se não existir, tenta abrir a conexão em background para o envio
                        conn = PeerConnection(
                            peer_id,
                            peer_table,
                            on_close=lambda c: outbound_connections.pop(c.remote_peer_id, None) if c.remote_peer_id else None
                        )
                        try:
                            await conn.connect(p.ip, p.port)
                            asyncio.create_task(conn.listen())
                            outbound_connections[p.peer_id] = conn
                        except Exception:
                            continue
                    
                    # Dispara a mensagem PUB sobre a conexão, propagando-a no escopo definido
                    try:
                        await conn.publish(msg_content, msg_id, scope)
                        count += 1
                    except Exception:
                        pass
                print(f"Broadcast enviado para {count} peer(s).")

            elif cmd == "/conn":
                print("--- Conexões Ativas de Entrada (Inbound) ---")
                for pid, conn in inbound_connections.items():
                    print(f"{pid} -> Connected")
                print("--- Conexões Ativas de Saída (Outbound) ---")
                # Exibe uma lista das conexões TCP que foram iniciadas por este nó
                for pid, conn in outbound_connections.items():
                    print(f"{pid} -> Connected")
                print("-----------------------------------------")
                
            elif cmd == "/rtt":
                print("--- RTT Médio por Peer ---")
                for p in peer_table.get_all_peers():
                    if p.average_rtt is not None:
                        print(f"{p.peer_id}: {p.average_rtt:.2f} ms")
                    else:
                        print(f"{p.peer_id}: N/D")
                print("--------------------------")

            elif cmd == "/reconnect":
                print("Iniciando reconexão manual para todos os peers desconectados/obsoletos...")
                for p in peer_table.get_all_peers():
                    if p.status in ["DISCONNECTED", "STALE", "RECONNECTING"]:
                        p.status = "RECONNECTING"
                        p.next_attempt_allowed_at = 0.0 # Força tentativa imediata
                        p.reconnect_attempts = 0

            elif cmd == "/log":
                if len(parts) < 2:
                    print("Uso: /log <DEBUG|INFO|WARNING|ERROR>")
                    continue
                level_str = parts[1].upper()
                num_level = getattr(logging, level_str, None)
                if num_level is not None:
                    logging.getLogger().setLevel(num_level)
                    for handler in logging.getLogger().handlers:
                        handler.setLevel(num_level)
                    print(f"Nível de log alterado para {level_str}")
                else:
                    print(f"Nível de log inválido: {level_str}")

            elif cmd == "/help":
                print("--- Comandos Disponíveis ---")
                print("  /help                                    : Exibe esta mensagem de ajuda")
                print("  /peers [* | #namespace]                  : Lista peers conhecidos")
                print("  /msg <peer_id> <mensagem>                 : Envia mensagem direta (unicast)")
                print("  /pub <* | #namespace> <mensagem>          : Envia mensagem global (broadcast)")
                print("  /conn                                    : Exibe conexões ativas (saída)")
                print("  /rtt                                     : Exibe o RTT médio por peer")
                print("  /reconnect                               : Força tentativa imediata de reconexão")
                print("  /log <DEBUG|INFO|WARNING|ERROR>          : Altera nível do log do sistema")
                print("  /quit                                    : Sai e finaliza a aplicação")
                print("----------------------------")

            elif cmd == "/quit":
                print("Iniciando encerramento limpo...")
                shutdown_event.set()
                break

            else:
                print(f"Comando desconhecido: {cmd}. Digite /help para listar comandos.")

        except Exception as e:
            logger.error(f"Erro no CLI: {e}")
            break
            
    # Sinaliza que o CLI foi desativado para restaurar o comportamento normal de logs
    ReadlineConsoleHandler.cli_active = False
