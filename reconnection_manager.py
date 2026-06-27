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
# ARQUIVO: reconnection_manager.py
# ==============================================================================

import asyncio
import logging
from peer_connection import PeerConnection
from peer_table import PeerTable
from typing import Dict

async def reconnection_loop(
    peer_id: str,
    peer_table: PeerTable,
    outbound_connections: Dict[str, PeerConnection],
    interval: int = 5
):
    """
    Loop em segundo plano que verifica periodicamente se existem peers no estado
    'RECONNECTING' cujo tempo de penalidade (backoff) já expirou, e então tenta
    estabelecer uma nova conexão TCP de forma autônoma.
    """
    logger = logging.getLogger(__name__)
    try:
        while True:
            await asyncio.sleep(interval)
            candidates = peer_table.get_peers_to_reconnect()
            
            for peer in candidates:
                logger.info(f"[Reconnect] Tentando reconectar automaticamente a {peer.peer_id} em {peer.ip}:{peer.port}")
                
                # Cria uma nova instância de conexão
                conn = PeerConnection(peer_id, peer_table)
                try:
                    await conn.connect(peer.ip, peer.port)
                    # Inicia a escuta contínua de mensagens
                    listen_task = asyncio.create_task(conn.listen())
                    
                    # Armazena a nova conexão ativa
                    outbound_connections[peer.peer_id] = conn
                    logger.info(f"[Reconnect] Reconectado com sucesso a {peer.peer_id}")
                except Exception as e:
                    logger.warning(f"[Reconnect] Falha ao reconectar a {peer.peer_id}: {e}")
                    # Registra a falha, o que incrementará o contador e aumentará o tempo do próximo backoff exponencial
                    peer_table.mark_failed_attempt(peer.peer_id)
                    
    except asyncio.CancelledError:
        pass
