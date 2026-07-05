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
# ARQUIVO: reconnection_manager.py
# ================================================================

import asyncio
import logging
import time
from peer_connection import PeerConnection
from peer_table import PeerTable
from typing import Dict

async def _attempt_reconnect(peer_id, peer, peer_table, outbound_connections, logger):
    if peer.status == "CONNECTED" or peer.peer_id in outbound_connections:
        logger.debug(f"[Reconnect] Conexão ativa ou em andamento já existe para {peer.peer_id}. Cancelando reconexão.")
        return
    logger.info(f"[Reconnect] Tentando reconectar automaticamente a {peer.peer_id} em {peer.ip}:{peer.port}")
    conn = PeerConnection(
        peer_id,
        peer_table,
        on_close=lambda c: outbound_connections.pop(c.remote_peer_id, None) if c.remote_peer_id else None
    )
    try:
        await conn.connect(peer.ip, peer.port)
        listen_task = asyncio.create_task(conn.listen())
        
        # Se a identidade do peer mudou (ex: reaproveitamento de IP)
        if conn.remote_peer_id and conn.remote_peer_id != peer.peer_id:
            peer_table.mark_stale(peer.peer_id)
            outbound_connections[conn.remote_peer_id] = conn
            logger.info(f"[Reconnect] Peer mudou de identidade. Era {peer.peer_id}, agora é {conn.remote_peer_id}")
        else:
            outbound_connections[peer.peer_id] = conn
            logger.info(f"[Reconnect] Reconectado com sucesso a {peer.peer_id}")
            
    except Exception as e:
        err_msg = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
        logger.warning(f"[Reconnect] Falha ao reconectar a {peer.peer_id}: {err_msg}")
        peer_table.mark_failed_attempt(peer.peer_id)

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
            
            # Lança tarefas concorrentes para não bloquear o loop caso uma das conexões sofra timeout
            for peer in candidates:
                # Previne múltiplas tentativas simultâneas para o mesmo peer jogando o timer para o futuro
                peer.next_attempt_allowed_at = time.time() + 60.0
                asyncio.create_task(_attempt_reconnect(peer_id, peer, peer_table, outbound_connections, logger))
                    
    except asyncio.CancelledError:
        pass
