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
# ARQUIVO: message_router.py
# ==============================================================================

import json
import uuid
import datetime
import logging
from config import Config

def encode_message(data : dict) -> bytes:
    """
    Transforma um dicionário Python em uma string JSON codificada em bytes,
    adicionando uma quebra de linha ('\n') no final para servir de delimitador.
    """
    return (json.dumps(data) + "\n").encode('utf-8')

def decode_message(raw : bytes) -> dict:
    """
    Decodifica uma sequência de bytes recebida da rede (que contém uma string JSON)
    e reconstrói o dicionário Python original.
    """
    return json.loads(raw.decode('utf-8'))

async def process_common_messages(msg: dict, local_peer_id: str, remote_peer_id: str, send_func) -> str:
    """
    Processa mensagens comuns do protocolo (PING, SEND, PUB, BYE) que podem chegar
    tanto pelo canal de servidor (inbound) quanto pelo canal de cliente (outbound).
    
    :param msg: Dicionário decodificado da mensagem.
    :param local_peer_id: Identificador local (ex: alice@CIC).
    :param remote_peer_id: Identificador remoto.
    :param send_func: Função/Corrotina async que recebe um dicionário para enviá-lo de volta.
    :return: "BREAK" se a conexão deve ser encerrada, "CONTINUE" se deve ser repassada, "HANDLED" se tratada.
    """
    msg_type = msg.get("type")
    
    if msg_type == "PING":
        logging.getLogger(__name__).debug(f"PING RECEBIDO de {remote_peer_id or 'desconhecido'}")
        pong = {
            "type": "PONG",
            "msg_id": msg.get("msg_id", str(uuid.uuid4())),
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "ttl": Config().fixed_msg_ttl
        }
        await send_func(pong)
        logging.getLogger(__name__).debug(f"PONG ENVIADO para {remote_peer_id or 'desconhecido'}")
        return "HANDLED"

    elif msg_type == "SEND":
        sender  = msg.get("src", remote_peer_id or "desconhecido")
        content = msg.get("payload", "")
        msg_id  = msg.get("msg_id")
        logging.getLogger(__name__).info(f"[SEND] {sender}: {content}")

        if msg.get("require_ack") and msg_id:
            ack = {
                "type": "ACK",
                "msg_id": msg_id,
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "ttl": Config().fixed_msg_ttl
            }
            await send_func(ack)
            logging.getLogger(__name__).debug(f"[ACK] Recibo ACK enviado para msg_id={msg_id} (destino: {sender})")
        return "HANDLED"

    elif msg_type == "PUB":
        sender  = msg.get("src", remote_peer_id or "desconhecido")
        content = msg.get("payload", "")
        scope   = msg.get("dst", "*")
        logging.getLogger(__name__).info(f"[PUB] [{scope}] {sender}: {content}")
        return "HANDLED"

    elif msg_type == "BYE":
        reason = msg.get("reason", "Sem motivo especificado")
        logging.getLogger(__name__).info(f"Recebido comando BYE de {remote_peer_id or 'desconhecido'}. Motivo: {reason}")
        
        bye_ok = {
            "type": "BYE_OK",
            "msg_id": msg.get("msg_id", str(uuid.uuid4())),
            "src": local_peer_id,
            "dst": msg.get("src", remote_peer_id or "desconhecido"),
            "ttl": Config().fixed_msg_ttl
        }
        await send_func(bye_ok)
        return "BREAK"
        
    return "CONTINUE"
