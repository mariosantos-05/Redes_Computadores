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

def validate_message_fields(msg: dict):
    """
    Valida os campos de uma mensagem de acordo com as especificações do protocolo:
    - namespace: string (até 64 caracteres)
    - name: string (até 64 caracteres)
    - port: inteiro (1-65535)
    - ttl: inteiro em segundos (1-86400)
    
    Lança ValueError caso alguma validação falhe.
    """
    # Validação de namespace
    if "namespace" in msg:
        ns = msg["namespace"]
        if not isinstance(ns, str):
            raise ValueError("O campo 'namespace' deve ser uma string.")
        if len(ns) > 64:
            raise ValueError("O campo 'namespace' não deve exceder 64 caracteres.")
            
    # Validação de name
    if "name" in msg:
        name = msg["name"]
        if not isinstance(name, str):
            raise ValueError("O campo 'name' deve ser uma string.")
        if len(name) > 64:
            raise ValueError("O campo 'name' não deve exceder 64 caracteres.")
            
    # Validação de peer_id (se contiver name@namespace)
    if "peer_id" in msg:
        pid = msg["peer_id"]
        if not isinstance(pid, str):
            raise ValueError("O campo 'peer_id' deve ser uma string.")
        if "@" in pid:
            parts = pid.split("@", 1)
            name_part = parts[0]
            ns_part = parts[1]
            if len(name_part) > 64:
                raise ValueError("O nome extraído de 'peer_id' não deve exceder 64 caracteres.")
            if len(ns_part) > 64:
                raise ValueError("O namespace extraído de 'peer_id' não deve exceder 64 caracteres.")
        else:
            if len(pid) > 64:
                raise ValueError("O campo 'peer_id' não deve exceder 64 caracteres.")
                
    # Validação de port
    if "port" in msg:
        port = msg["port"]
        try:
            port_val = int(port)
        except (ValueError, TypeError):
            raise ValueError("O campo 'port' deve ser um inteiro válido.")
        if not (1 <= port_val <= 65535):
            raise ValueError(f"O campo 'port' deve estar entre 1 e 65535. Valor recebido: {port_val}")
            
    # Validação de ttl
    if "ttl" in msg:
        ttl = msg["ttl"]
        try:
            ttl_val = int(ttl)
        except (ValueError, TypeError):
            raise ValueError("O campo 'ttl' deve ser um inteiro válido.")
        if not (1 <= ttl_val <= 86400):
            raise ValueError(f"O campo 'ttl' deve estar entre 1 e 86400. Valor recebido: {ttl_val}")

def encode_message(data : dict) -> bytes:
    """
    Transforma um dicionário Python em uma string JSON codificada em bytes,
    adicionando uma quebra de linha ('\n') no final para servir de delimitador.
    Também valida os formatos dos campos da mensagem antes de enviar.
    """
    validate_message_fields(data)
    return (json.dumps(data) + "\n").encode('utf-8')

def decode_message(raw : bytes) -> dict:
    """
    Decodifica uma sequência de bytes recebida da rede (que contém uma string JSON)
    e reconstrói o dicionário Python original, validando seus campos.
    """
    msg = json.loads(raw.decode('utf-8'))
    validate_message_fields(msg)
    return msg

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
