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


