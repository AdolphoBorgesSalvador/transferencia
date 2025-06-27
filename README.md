# Análise de Transferências - PostgreSQL

Este projeto realiza a análise de movimentações, estoques e previsões de entrada de materiais específicos utilizando dados armazenados em um banco de dados PostgreSQL. O objetivo é gerar relatórios consolidados para apoiar decisões logísticas e de estoque.

## Funcionalidades
- Conexão automática ao banco de dados PostgreSQL (Docker ou local)
- Extração de dados das tabelas `zmb51`, `zstok` e `fup`
- Processamento e consolidação dos dados por centro e material
- Geração de tabelas pivô com médias móveis
- Exportação dos resultados em arquivos JSON

## Estrutura dos Dados
- **zmb51**: Movimentações dos últimos 12 meses
- **zstok**: Estoque atual por material e centro
- **fup**: Previsões de entrada de materiais

## Como executar
1. **Pré-requisitos:**
   - Python 3.8+
   - Docker (opcional, para PostgreSQL)
   - Variáveis de ambiente configuradas (`.env`)

2. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure o arquivo `.env`** com as credenciais do banco de dados:
   ```env
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=postgres
   DB_USER=postgres
   DB_PASSWORD=sua_senha
   ```

4. **Execute o script principal:**
   ```bash
   python Main.py
   ```

5. **Saída:**
   - Os arquivos JSON serão gerados na pasta `output/`.

## Estrutura do Projeto
- `Main.py`: Script principal de extração, processamento e exportação dos dados
- `requirements.txt`: Dependências do projeto
- `output/`: Pasta onde os resultados são salvos

## Observações
- Certifique-se de que as tabelas estejam populadas no schema `power_bi` do PostgreSQL.
- O script pode ser adaptado para outros materiais ou centros alterando as queries em `get_queries()`.

---

**Autor:**
Adolpho Salvador

**Data:** Junho/2025
