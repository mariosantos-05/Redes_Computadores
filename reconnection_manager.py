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
    Periodically checks for peers in RECONNECTING state that have passed their
    backoff time, and attempts to reconnect.
    """
    logger = logging.getLogger(__name__)
    try:
        while True:
            await asyncio.sleep(interval)
            candidates = peer_table.get_peers_to_reconnect()
            
            for peer in candidates:
                logger.info(f"[Reconnect] Attempting to reconnect to {peer.peer_id} at {peer.ip}:{peer.port}")
                
                # Create a new connection
                conn = PeerConnection(peer_id, peer_table)
                try:
                    await conn.connect(peer.ip, peer.port)
                    # Start listening
                    listen_task = asyncio.create_task(conn.listen())
                    
                    # Store the connection
                    outbound_connections[peer.peer_id] = conn
                    logger.info(f"[Reconnect] Successfully reconnected to {peer.peer_id}")
                except Exception as e:
                    logger.warning(f"[Reconnect] Failed to reconnect to {peer.peer_id}: {e}")
                    peer_table.mark_failed_attempt(peer.peer_id)
                    
    except asyncio.CancelledError:
        pass
