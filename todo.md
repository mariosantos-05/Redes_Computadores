# 📝 Lista de Tarefas (To-Do List) — Chat P2P

Este documento rastreia o progresso do desenvolvimento do Chat P2P conforme os critérios da especificação do projeto.

---

## 1. 🏗️ Arquitetura de Módulos e Estrutura Geral
* [x] Criar estrutura básica de mensagens (JSON + `\n` bytes) — [messages.py]
* [ ] Criar estrutura de dados de peers (`PeerTable` e `PeerEntry`)
* [x] Corrigir erro de digitação do módulo do Rendezvous (`randezvour_connection.py` ➔ `rendezvous_connection.py`)
* [x] Estruturar o projeto nos módulos recomendados pela especificação:
  - `main.py`
  - `p2p_client.py`
  - `rendezvous_connection.py`
  - `peer_connection.py`
  - `message_router.py`
  - `keep_alive.py`
  - `peer_table.py`
  - `state.py`
  - `cli.py`

---

## 2. 🛜 Integração com o Servidor Rendezvous
* [x] Classe de conexão curta de comando único TCP para o Rendezvous — [rendezvous_connection.py]
* [x] Envio de `REGISTER` de teste
* [x] Envio de `DISCOVER` de teste
* [ ] Implementar loop de descoberta recorrente e automática (`DISCOVER`) em background
* [ ] Implementar atualização periódica do registro no servidor Rendezvous (re-registro antes da expiração do TTL)
* [ ] Envio de `UNREGISTER` limpo ao encerrar a aplicação

---

## 3. 🤝 Conexão TCP entre Peers & Keep-Alive
* [x] Servidor local TCP (`PeerServer`) escutando conexões entrantes
* [x] Cliente TCP (`PeerConnection`) abrindo conexões de saída
* [x] Handshake básico estabelecendo conexão (`HELLO` / `HELLO_OK`)
* [ ] Enviar de forma recorrente mensagens de `PING` a cada 30 segundos (intervalo configurável)
* [ ] Tratar recebimento de `PING` e responder com `PONG`
* [ ] Calcular RTT (tempo de resposta) de cada `PING`/`PONG` e salvar média no `PeerEntry`
* [ ] Fechar a conexão de forma limpa em caso de erros de leitura/escrita (timeouts)

---

## 4. ✉️ Fluxo de Mensageria (Chat)
* [ ] Implementar comando `SEND` para mensagens unicast diretas
* [ ] Tratar mensagens com confirmação (`require_ack`):
  - Retornar mensagem do tipo `ACK`
  - Lançar aviso de timeout nos logs caso o `ACK` não chegue em até 5 segundos
* [ ] Implementar comando `PUB` para mensagens de difusão:
  - Difusão para o Namespace atual (`#namespace`)
  - Difusão global (`*`) para todos os peers conectados

---

## 5. 🚪 Encerramento Controlado (Sessão)
* [ ] Enviar comando `BYE` especificando o motivo ao fechar conexões
* [ ] Responder com `BYE_OK` ao receber um comando `BYE` e fechar o socket de forma ordenada

---

## 6. 🔄 Resiliência e Reconexões
* [ ] Lógica para varrer a `PeerTable` e identificar peers elegíveis para reconexão
* [ ] Implementar tentativas automáticas de reconexão seguindo a política de **backoff exponencial**
* [ ] Limitar tentativas a `max_reconnect_attempts` (lido do `config.json` ou padrão) e marcar peers inacessíveis como `STALE`

---

## 7. 💻 Interface de Usuário (CLI) & Observabilidade
* [ ] Construir a CLI interativa (`cli.py`) suportando os seguintes comandos:
  - `/peers [* | #namespace]` (listar/descobrir peers)
  - `/msg <peer_id> <mensagem>` (mensagem direta)
  - `/pub * <mensagem>` (broadcast global)
  - `/pub #<namespace> <mensagem>` (broadcast no namespace)
  - `/conn` (mostrar conexões ativas inbound/outbound)
  - `/rtt` (mostrar RTT médio por peer)
  - `/reconnect` (forçar reconciliação de peers imediatamente)
  - `/log <Nível>` (ajustar dinamicamente nível de log)
  - `/quit` (sair e fechar tudo limpo)
* [ ] Configurar sistema de logging com formatação profissional, enviando registros para a tela e para um arquivo de logs (`p2p.log`).
