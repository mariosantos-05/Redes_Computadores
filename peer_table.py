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
# ARQUIVO: peer_table.py
# ==============================================================================

import time
from typing import Dict, List, Optional
from config import Config

class PeerEntry:
    """
    Representa o estado e todas as informações de rede e conexão de um peer remoto específico.
    Cada peer que você descobre ou se conecta possui uma instância única desta classe na sua tabela.
    """
    def __init__(self, name: str, namespace: str, ip: str, port: int, ttl: int):
        # Nome identificador do usuário (ex: 'alice', 'bob')
        self.name = name
        
        # Compartimento lógico onde o usuário se encontra (ex: 'CIC', 'UnB')
        self.namespace = namespace
        
        # Identificador composto único para este peer na rede (ex: 'alice@CIC')
        # Este ID é usado como a chave primária para buscar contatos na tabela
        self.peer_id = f"{name}@{namespace}"
        
        # Endereço IP físico do computador remoto (ex: '192.168.1.15' ou '45.171.101.167')
        self.ip = ip
        
        # Porta de rede TCP na qual o servidor deste peer está escutando conexões
        self.port = port
        
        # Time-to-live: Tempo em segundos que este cadastro no servidor Rendezvous permanece válido
        self.ttl = ttl
        
        # Instante de tempo do sistema (timestamp Unix) em que este cadastro expira e
        # deve ser considerado inválido ou removido.
        self.expires_at = time.time() + ttl
        
        # Estado atual da sua conexão TCP com este peer. Pode assumir um de quatro valores:
        # - "DISCONNECTED": Estado inicial, você conhece o peer mas não há conexão TCP ativa com ele.
        # - "CONNECTED": Existe um socket TCP aberto, handshake (HELLO) feito com sucesso e keep-alive ativo.
        # - "RECONNECTING": Uma conexão anterior caiu e o peer está aguardando o tempo de backoff para tentar novamente.
        # - "STALE": O peer falhou em responder todas as tentativas de conexão e foi marcado como inativo/offline.
        self.status = "DISCONNECTED"
        
        # Lista contendo os últimos valores medidos de RTT (tempo de ida e volta) em milissegundos.
        # Funciona como uma fila circular que retém apenas as medições mais recentes.
        self.rtt_history: List[float] = []
        
        # Média dos RTTs medidos (None se nenhuma medição foi feita ainda)
        self.average_rtt: Optional[float] = None
        
        # Contador de quantas tentativas de reconexão TCP seguidas falharam para este peer.
        # É zerado assim que a conexão é estabelecida com sucesso.
        self.reconnect_attempts = 0
        
        # Timestamp Unix que guarda o momento exato em que a última tentativa de conexão falhou.
        self.last_attempt_time = 0.0
        
        # Timestamp Unix que diz a partir de qual momento o seu programa está autorizado
        # a tentar se conectar a este peer novamente (usado para respeitar o tempo de backoff).
        self.next_attempt_allowed_at = 0.0

    def update_info(self, ip: str, port: int, ttl: int):
        """
        Atualiza as informações de rede do peer quando ele é retornado em um novo DISCOVER do Rendezvous.
        
        Por que é necessário?
        Os peers podem mudar de IP dinamicamente (ex: mudar de rede Wi-Fi) ou reiniciar seu contador
        de expiração (TTL). Esse método garante que seu programa use os dados atualizados.
        """
        self.ip = ip
        self.port = port
        self.ttl = ttl
        # Recalcula a hora de expiração com base no novo TTL recebido
        self.expires_at = time.time() + ttl

    def add_rtt(self, rtt_ms: float):
        """
        Registra uma nova medição de tempo de resposta (RTT) obtida por uma troca de PING/PONG.
        Para evitar consumo excessivo de memória, mantemos apenas as últimas 10 medições na lista.
        """
        self.rtt_history.append(rtt_ms)
        # Se ultrapassar 10 medições, removemos a mais antiga (primeiro elemento da lista)
        if len(self.rtt_history) > 10:
            self.rtt_history.pop(0)
        
        # Calcula e atualiza a média do RTT diretamente na variável
        self.average_rtt = sum(self.rtt_history) / len(self.rtt_history)


class PeerTable:
    """
    Gerencia o banco de dados local na memória contendo a lista de todos os peers conhecidos
    e centraliza a lógica de controle de estados, contagem de reconexões e backoff exponencial.
    """
    def __init__(self, max_reconnect_attempts: Optional[int] = None, initial_backoff_sec: Optional[float] = None):
        # Dicionário que mapeia o 'peer_id' (string) para o objeto completo 'PeerEntry'.
        # Permite buscas super rápidas de complexidade O(1) pelo identificador do peer.
        self.peers: Dict[str, PeerEntry] = {}
        
        # Limite máximo de tentativas seguidas de conexão que faremos antes de desistir e marcar como STALE
        self.max_reconnect_attempts = (
            max_reconnect_attempts 
            if max_reconnect_attempts is not None 
            else Config().max_reconnect_attempts
        )
        
        # Tempo base (em segundos) que servirá de ponto de partida para o atraso de reconexão exponencial
        # Como o novo sistema de configuração no main não possui initial_backoff_sec, usamos 2.0 como padrão.
        self.initial_backoff_sec = (
            initial_backoff_sec 
            if initial_backoff_sec is not None 
            else 2.0
        )

    def update_from_discovery(self, discovery_peers: List[Dict]) -> List[PeerEntry]:
        """
        Sincroniza a tabela local de peers com a lista de peers retornada pelo comando DISCOVER do Rendezvous.
        
        Como funciona:
        1. Varre a lista de contatos retornada pelo servidor Rendezvous.
        2. Se o peer já existir no seu dicionário local, suas informações de IP/porta/TTL são atualizadas.
        3. Se for um peer novo, uma nova instância de PeerEntry é criada, adicionada na tabela e
           colocada em uma lista de retorno (útil para que o cliente saiba quem acabou de entrar na rede).
           
        Retorna:
            - Uma lista contendo as instâncias de novos PeerEntry adicionados nessa chamada de sincronização.
        """
        new_peers = []
        for p in discovery_peers:
            name = p["name"]
            namespace = p["namespace"]
            peer_id = f"{name}@{namespace}"
            ip = p["ip"]
            port = p["port"]
            ttl = p.get("ttl", 3600)

            if peer_id in self.peers:
                # O peer já era conhecido, apenas atualizamos seus dados de endereço e expiração
                self.peers[peer_id].update_info(ip, port, ttl)
            else:
                # É um peer novo na rede! Criamos seu registro inicial na tabela local
                entry = PeerEntry(name, namespace, ip, port, ttl)
                self.peers[peer_id] = entry
                new_peers.append(entry)
                
        return new_peers

    def update_peer(self, peer_id: str, ip: str, port: int, ttl: int):
        """
        Atualiza ou insere um peer na tabela a partir de uma conexão de entrada (inbound)
        e marca o seu status como "CONNECTED".
        """
        if "@" in peer_id:
            name, namespace = peer_id.split("@", 1)
        else:
            name = peer_id
            namespace = "Default"
            
        if peer_id in self.peers:
            self.peers[peer_id].update_info(ip, port, ttl)
        else:
            self.peers[peer_id] = PeerEntry(name, namespace, ip, port, ttl)
            
        self.mark_connected(peer_id)

    def get_peer(self, peer_id: str) -> Optional[PeerEntry]:
        """
        Busca e retorna o objeto completo PeerEntry associado ao peer_id informado.
        
        Exemplo:
            peer = peer_table.get_peer("alice@CIC")
            
        Retorna:
            - O objeto PeerEntry contendo todo o estado do peer.
            - None se o peer_id não estiver cadastrado na tabela.
        """
        return self.peers.get(peer_id)

    def mark_connected(self, peer_id: str):
        """
        Marca o peer como conectado com sucesso ("CONNECTED").
        
        Além de atualizar o status, este método reseta o contador de tentativas de reconexão
        e o temporizador de backoff, pois o canal de comunicação agora está saudável.
        """
        peer = self.get_peer(peer_id)
        if peer:
            peer.status = "CONNECTED"
            peer.reconnect_attempts = 0
            peer.next_attempt_allowed_at = 0.0

    def mark_failed_attempt(self, peer_id: str):
        """
        Registra que uma tentativa de conexão TCP com este peer remoto falhou.
        
        Este método aplica a política de backoff exponencial:
        1. Incrementa o número de tentativas seguidas de falha.
        2. Se o número de falhas atingir ou passar de 'max_reconnect_attempts', assume que o peer
           está offline definitivamente e muda o status para "STALE" (reseta o backoff).
        3. Caso contrário, define o status para "RECONNECTING" e calcula a penalidade de tempo:
           atraso = initial_backoff * (2 ^ (tentativas - 1))
           O peer só poderá ser reconectado após esse período de atraso ter passado.
        """
        peer = self.get_peer(peer_id)
        if not peer:
            return

        peer.reconnect_attempts += 1
        now = time.time()
        peer.last_attempt_time = now

        if peer.reconnect_attempts >= self.max_reconnect_attempts:
            # Excedeu o número limite de tentativas permitidas. O peer é marcado como offline.
            peer.status = "STALE"
            peer.next_attempt_allowed_at = 0.0
        else:
            # Aplica o backoff exponencial para evitar sobrecarregar a rede com tentativas seguidas de conexão
            # Ex: Tentativa 1 -> 2s, Tentativa 2 -> 4s, Tentativa 3 -> 8s, Tentativa 4 -> 16s...
            backoff_delay = self.initial_backoff_sec * (2 ** (peer.reconnect_attempts - 1))
            peer.status = "RECONNECTING"
            peer.next_attempt_allowed_at = now + backoff_delay

    def mark_disconnected(self, peer_id: str):
        """
        Muda o status do peer manualmente para "DISCONNECTED".
        
        Isso acontece quando o encerramento da conexão ocorre de forma controlada e amigável
        (por exemplo, quando recebemos ou enviamos uma mensagem 'BYE' informando encerramento normal).
        Como não foi uma queda acidental, os contadores de erro e backoff são zerados.
        """
        peer = self.get_peer(peer_id)
        if peer:
            peer.status = "DISCONNECTED"
            peer.reconnect_attempts = 0
            peer.next_attempt_allowed_at = 0.0

    def mark_stale(self, peer_id: str):
        """
        Força a marcação de um peer como inativo/offline ("STALE") diretamente.
        
        Útil se o seu programa detectar que o peer foi desregistrado do servidor Rendezvous
        ou se você deseja interromper tentativas de reconexão imediatamente por algum motivo externo.
        """
        peer = self.get_peer(peer_id)
        if peer:
            peer.status = "STALE"

    def remove_peer(self, peer_id: str):
        """
        Remove completamente o registro de um peer da tabela local.
        """
        if peer_id in self.peers:
            del self.peers[peer_id]

    def get_all_peers(self) -> List[PeerEntry]:
        """
        Retorna uma lista simples contendo todos os objetos PeerEntry da tabela.
        Útil para listar usuários ativos na tela do usuário.
        """
        return list(self.peers.values())

    def get_connected_peers(self) -> List[PeerEntry]:
        """
        Retorna uma lista contendo apenas os objetos PeerEntry de peers que
        estão atualmente com status "CONNECTED" (conexão TCP ativa).
        """
        return [p for p in self.peers.values() if p.status == "CONNECTED"]

    def get_peers_to_reconnect(self) -> List[PeerEntry]:
        """
        Filtra e retorna todos os peers que estão com falha temporária ("RECONNECTING")
        e cuja penalidade de tempo de backoff já terminou (ou seja, o tempo atual é igual ou maior
        que 'next_attempt_allowed_at').
        
        Esse método deve ser chamado periodicamente pela thread/tarefa de background do cliente
        para saber para quais peers ele deve tentar iniciar um novo processo de reconexão.
        """
        now = time.time()
        candidates = []
        for p in self.peers.values():
            if p.status == "RECONNECTING" and now >= p.next_attempt_allowed_at:
                candidates.append(p)
        return candidates
