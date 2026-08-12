# coletorOpenAlex

Aplicação desktop em Python para coletar metadados de **Works da OpenAlex** a partir de listas de **DOIs** ou **IDs OpenAlex (W...)**.

O programa foi pensado para coletas pequenas ou de grande escala, com **cache local**, **retomada automática**, controle de requisições e exportação em CSV com **uma publicação por linha**.

## O que o programa faz

- Lê arquivos CSV, TSV ou TXT com DOI ou Work ID.
- Detecta automaticamente a provável coluna de identificadores.
- Normaliza DOI e IDs OpenAlex.
- Remove identificadores duplicados.
- Consulta a API da OpenAlex.
- Armazena os resultados em cache SQLite.
- Retoma coletas interrompidas sem recomeçar do zero.
- Reaproveita Works já coletados em execuções futuras.
- Controla multithreading, limite de requisições e retries.
- Permite consultas singleton ou batch.
- Exporta os metadados em CSV.
- Mantém **1 Work = 1 linha**.
- Consolida autores e instituições em JSON.
- Remove quebras de linha internas que poderiam prejudicar o CSV.

## Requisitos

- Python 3.10+
- `requests`

```bash
pip install requests
python coletorOpenAlex.py
```

A aplicação utiliza Tkinter para a interface gráfica.

## Entrada

O arquivo deve possuir uma coluna com DOI ou ID OpenAlex de Work.

Exemplos de DOI aceitos:

```text
10.1007/abc
doi:10.1007/abc
https://doi.org/10.1007/abc
```

Exemplos de OpenAlex ID aceitos:

```text
2975492377
W2975492377
https://openalex.org/W2975492377
```

IDs numéricos recebem automaticamente o prefixo `W`.

Ao selecionar o arquivo, o programa analisa o cabeçalho e uma amostra dos valores para sugerir a coluna e o tipo de identificador mais provável. A escolha pode ser alterada manualmente.

## API key

É necessária uma **API key da OpenAlex**.

A interface possui um botão para testar a chave e consultar:

- orçamento diário;
- saldo diário restante;
- saldo pré-pago;
- horário de reset.

A chave não é salva em arquivo de configuração pelo programa.

## Estratégias de consulta

**Automático**  
Usa batch enquanto houver orçamento diário disponível e depois passa para singleton.

**Econômico**  
Usa somente consultas singleton.

**Rápido**  
Prioriza batch e pode usar saldo pré-pago quando autorizado pelo usuário.

Os batches possuem até **100 identificadores por requisição**.

## Cache e retomada

O cache é armazenado em:

```text
data/coletorOpenAlex_cache.sqlite3
```

Os Works recuperados são comprimidos e armazenados localmente. O cache também registra a associação entre DOI e Work ID.

Isso permite, por exemplo, que um trabalho coletado inicialmente por DOI seja reconhecido posteriormente por seu `W...`, evitando uma nova consulta.

Quando o programa é executado novamente, apenas identificadores ainda não resolvidos são enviados à OpenAlex.

O mesmo cache é reutilizado entre diferentes coletas.

## Tratamento de falhas

O coletor realiza retries automáticos para falhas temporárias, incluindo:

- erros de conexão;
- timeout;
- HTTP 429;
- HTTP 500, 502, 503 e 504.

Um `404` é registrado como `not_found`.

Erros persistentes são registrados como `error` e podem ser tentados novamente em outra execução.

Falhas de autenticação (`401` ou `403`) interrompem a coleta.

## Saída

A saída é um CSV em UTF-8.

A regra principal é:

> **1 Work = 1 linha**

Autores e instituições não geram linhas adicionais. A estrutura completa de autoria é armazenada na coluna `autores` em JSON.

O programa também sanitiza títulos, afiliações e demais textos para remover quebras internas de linha.

Entre os metadados exportados estão:

- OpenAlex Work ID e DOI;
- título e resumo;
- ano e data de publicação;
- tipo e idioma;
- quantidade de citações;
- domínio, campo, subcampo e tópicos;
- fonte de publicação, ISSN e ISSN-L;
- informações de acesso aberto;
- quantidade de autores, países e instituições;
- FWCI e percentil normalizado de citações;
- autores, ORCID, países, instituições e afiliações;
- keywords, funders e awards;
- datas de criação e atualização do registro OpenAlex.

### Autoria

A coluna `autores` mantém a relação entre cada autor e suas instituições.

Exemplo simplificado:

```json
[
  {
    "id_autor": "A123456789",
    "nome": "Autor Exemplo",
    "paises": ["UY"],
    "instituicoes": [
      {
        "id": "I123456789",
        "nome": "Universidad Ejemplo",
        "pais": "UY"
      }
    ],
    "orcid": "https://orcid.org/0000-0000-0000-0000"
  }
]
```

## Pausar e interromper

A coleta pode ser pausada ou interrompida pela interface.

Ao interromper:

- o que já foi coletado permanece no SQLite;
- um CSV parcial é gerado com os resultados disponíveis;
- a execução seguinte reaproveita o cache.

## Arquivos criados

```text
coletorOpenAlex.py
data/
├── coletorOpenAlex_cache.sqlite3
└── logs/
    └── coletorOpenAlex.log
```

O log registra informações sobre progresso, cache, requisições e erros.

## Gerar executável no Windows

```bash
pip install pyinstaller requests
```

```bat
pyinstaller --clean --noconfirm --onefile --windowed ^
    --name coletorOpenAlex ^
    coletorOpenAlex.py
```

## Fluxo geral

```text
Arquivo com DOI ou Work ID
        ↓
Detecção e normalização
        ↓
Remoção de duplicatas
        ↓
Consulta ao cache
        ↓
OpenAlex API
        ↓
SQLite
        ↓
CSV
```

O `coletorOpenAlex` busca tornar a coleta de metadados da OpenAlex simples, reutilizável e robusta, especialmente quando o volume de identificadores torna importante evitar consultas repetidas e permitir a retomada do processamento.
