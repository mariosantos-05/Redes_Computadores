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
# ARQUIVO: config.py
# ==============================================================================

import json
import sys
import os
import uuid
from pathlib import Path

class Config:
    """
    Classe Singleton para carregar e fornecer configurações do arquivo config.json.
    Garante que os arquivos de metadados do projeto sejam lidos uma única vez
    na inicialização e compartilhados entre todos os módulos.
    """
    _instance = None

    def __new__(cls):
        # Implementação clássica de um Singleton: se a instância não existir, cria-a
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        """Lê o arquivo config.json e extrai as variáveis de ambiente."""
        config_path = Path(__file__).parent / 'config.json'
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Identificação do nó
            self.namespace = data.get("namespace", "Default")
            if not isinstance(self.namespace, str) or len(self.namespace) > 64:
                raise ValueError("Configuração inválida: 'namespace' deve ser string de até 64 caracteres.")
                
            self.name = data.get("name", f"peer_{uuid.uuid4().hex[:4]}")
            if not isinstance(self.name, str) or len(self.name) > 64:
                raise ValueError("Configuração inválida: 'name' deve ser string de até 64 caracteres.")
                
            self.peer_id = f"{self.name}@{self.namespace}"
            
            # Endereço e Porta do Rendezvous (Servidor Central de Descoberta)
            self.rdv_host = data.get("rdv_host", "45.171.101.167")
            self.rdv_port = int(data.get("rdv_port", 8080))
            if not (1 <= self.rdv_port <= 65535):
                raise ValueError("Configuração inválida: 'rdv_port' deve estar na faixa de 1 a 65535.")
            
            # Configurações do servidor local (porta em que este nó vai escutar conexões)
            self.tcp_port = int(data.get("tcp_port", 0))
            self.listen_port = self.tcp_port # Usando tcp_port como porta de escuta principal
            if not (0 <= self.listen_port <= 65535): # 0 é aceito para alocação dinâmica do sistema operacional nos testes
                raise ValueError("Configuração inválida: 'tcp_port' deve estar na faixa de 0 a 65535.")
            
            # Protocolo (Metadados do Handshake)
            self.version = data.get("version", "1.0")
            self.features = data.get("features", ["ack"])
            self.fixed_msg_ttl = data.get("fixed_msg_ttl", 1)
            self.type = data.get("type", ["REGISTER", "DISCOVER"])
            
            # Tempos de reconexão e TTL do Servidor
            self.max_reconnect_attempts = int(data.get("max_reconnect_attempts", 5))
            self.rdv_ttl = int(data.get("rdv_ttl", 3600))
            if not (1 <= self.rdv_ttl <= 86400):
                raise ValueError("Configuração inválida: 'rdv_ttl' deve estar na faixa de 1 a 86400 segundos.")
            
            # Parâmetros adicionais
            self.log_level = data.get("log_level", "INFO")
            self.autonomous_mode = data.get("autonomous_mode", False)
            self.discover_interval = data.get("discover_interval", 20)
            self.keepalive_interval = data.get("keepalive_interval", 30)
            
        except Exception as e:
            # Em caso de falha severa na leitura do JSON, avisa e encerra o sistema
            print(f"Erro fatal ao carregar config.json: {e}")
            sys.exit(1)