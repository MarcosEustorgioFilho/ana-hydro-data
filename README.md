# ana-hydro-data

[![License: GPL v3](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](LICENSE)
[![Language](https://img.shields.io/badge/language-R-276DC3.svg)](https://www.r-project.org/)
[![DOI](https://zenodo.org/badge/1359622771.svg)](https://doi.org/10.5281/zenodo.22586520)

## O que este algoritmo faz

Pipeline desenvolvido para a obtenção automatizada de séries históricas
hidrometeorológicas — chuva, nível de rio (cota) e vazão — diretamente do
WebService público da Agência Nacional de Águas e Saneamento Básico (ANA).
Ele cuida, de forma automática, dos problemas mais comuns de integração com
APIs governamentais legadas: instabilidade de rede (retries com backoff
exponencial), respostas em XML inconsistentes ou redundantes (uma mesma
estação pode ter múltiplas séries sobrepostas para o mesmo mês) e ausência de
padronização temporal (o resultado é sempre um calendário diário completo,
com `NA` explícito nos dias sem registro — nunca um "buraco" silencioso na
série).

## Qual problema real ele resolve

Equipes de dados em seguradoras (risco climático/agrícola), consultorias
ambientais, órgãos públicos de gestão hídrica, grupos de pesquisa em saúde
ambiental/epidemiologia climática e modelagem de risco de desastres naturais
gastam um tempo desproporcional coletando e curando manualmente dados
heterogêneos de fontes institucionais brasileiras. Este pipeline resolve
especificamente a etapa de **aquisição e padronização**: em vez de XML bruto
sujeito a erro humano, entrega dados tabulares, tipados e com controle básico
de qualidade (classificação explícita entre "sem dados no período" e "falha
técnica de coleta"), prontos para a etapa de análise.

## Como utilizar

Fluxo em 2 passos, na ordem exata implementada neste repositório: primeiro
gera-se o inventário de estações; depois esse arquivo é usado como entrada
obrigatória do script de download das séries diárias.

```r
# Passo 1 — gerar o inventario de estacoes (cadastro), salvo em um
# diretorio local escolhido por voce (nunca em um caminho fixo):
source("R/WebService/01_get_stations_inventory.R")

res_inventario <- get_stations_inventory(
  tipos = c("1", "2"),           # "1" = fluviometrica (nivel/vazao) | "2" = pluviometrica (chuva)
  uf = NULL,                     # NULL = consulta nacional em uma unica chamada por tipo
  dir_saida = "dados/estacoes",
  formato_saida = "csv"
)
```

```r
# Passo 2 — usar o arquivo gerado no Passo 1 como entrada obrigatoria e
# baixar as series diarias para o periodo desejado:
source("R/WebService/02_get_daily_series.R")

estacoes_chuva <- carregar_listagem_estacoes("dados/estacoes/HidroInventario_precipitation_stations_ANA.csv")

resultado <- processar_lote_estacoes(
  estacoes   = estacoes_chuva,
  start_date = "2015-01-01",
  end_date   = "2020-12-31",
  typedata   = "2"               # "1" = cota | "2" = chuva | "3" = vazao
)
```

```r
# Passo 3 (opcional) — inspecionar/gravar o resultado classificado em 3
# categorias (o pipeline nunca escreve em disco automaticamente):
nrow(resultado$validos)    # series com dado valido
nrow(resultado$sem_dados)  # estacoes sem dado no periodo
nrow(resultado$falhas)     # estacoes com falha tecnica apos retries

arrow::write_parquet(resultado$validos, "dados/series/chuva_validos.parquet")
```

> Quer baixar dados particionados por normal climatológica e por UF (com
> resumo consolidado e uma estrutura de diretórios por categoria)? Veja a
> seção **"Download por normal climatológica e UF"** logo abaixo — ela usa
> um terceiro script, `R/WebService/03_download_normal_uf.R`.

## Download por normal climatológica e UF

Além do fluxo geral acima (para qualquer período, qualquer lista de
estações), este repositório inclui **`R/WebService/03_download_normal_uf.R`**,
desenvolvido no âmbito do TCC citado em
[Relação com o TCC de origem](#relação-com-o-tcc-de-origem). Ele baixa dados
particionando o trabalho por **normal climatológica** (1961–1990 / 1991–2020
/ 2021–2024) e por **Unidade Federativa**, gravando uma estrutura de
diretórios por categoria (`long_data` / `nodata_gauges` / `problem_gauges`)
e um resumo consolidado de execução (`resumo_download`, em CSV + Parquet) —
com todos os caminhos como parâmetro, nunca fixos.

Esse script **não duplica** a lógica de download por estação: ele importa
automaticamente (via `source()`, ao ser carregado) e reaproveita
`get_ana_station_series()` e `processar_lote_estacoes()` de
`R/WebService/02_get_daily_series.R`, adicionando apenas a camada de
orquestração por UF/normal. Por isso ele usa um **critério de elegibilidade
mais estrito** que o script 02 (exige ~1 ano de operação antes do fim da
normal, e não apenas `DataInicioOperacao <= end_date`) — o cabeçalho do
próprio arquivo detalha o motivo.

```r
# 0) Basta carregar o script 03 — as duas funcoes de download por estacao sao
#    importadas automaticamente de R/WebService/02_get_daily_series.R.
#    Sourcear o script 02 manualmente antes e opcional (util se voce tambem
#    quiser usar carregar_listagem_estacoes() diretamente, como no passo 1
#    abaixo):
source("R/WebService/02_get_daily_series.R")
source("R/WebService/03_download_normal_uf.R")

# 1) Carregar a listagem de estacoes (saida do script 01):
estacoes_chuva <- carregar_listagem_estacoes("dados/estacoes/HidroInventario_precipitation_stations_ANA.csv")

# 2) Baixar os dados de chuva da normal climatologica 2 (1991-2020), por UF,
#    com 12 workers fixos (padrao):
resumo <- baixar_por_normal_uf(
  estacoes       = estacoes_chuva,
  climate_normal = 2,
  typedata       = "2",
  dir_saida      = "dados/normais_climatologicas"
)
```

Use `workers = "auto"` para usar `núcleos_disponíveis - 2` em vez dos 12
fixos (deixando ao menos 2 núcleos livres para o sistema operacional) — útil
em máquinas menores ou compartilhadas.

## Tecnologias utilizadas

Pacotes R efetivamente usados no código:

- [`httr2`](https://httr2.r-lib.org/) — requisições HTTP com retry/backoff exponencial e timeout
- [`xml2`](https://xml2.r-lib.org/) — parsing do XML retornado pelo WebService da ANA
- [`dplyr`](https://dplyr.tidyverse.org/) / [`tidyr`](https://tidyr.tidyverse.org/) — transformação e limpeza tabular
- [`purrr`](https://purrr.tidyverse.org/) — iteração funcional sobre tabelas/séries
- [`lubridate`](https://lubridate.tidyverse.org/) — datas, calendários mensais e períodos
- [`readr`](https://readr.tidyverse.org/) / [`stringr`](https://stringr.tidyverse.org/) — parsing numérico/texto, leitura e escrita de CSV
- [`arrow`](https://arrow.apache.org/docs/r/) — leitura/escrita de Parquet (usado por `02_get_daily_series.R`; opcional em `01_get_stations_inventory.R`)
- `tibble` — estruturas de dados tabulares

Opcionais em `01_get_stations_inventory.R`/`02_get_daily_series.R` (apenas se
`paralelizar_meses = TRUE`), porém **usados por padrão** em
`03_download_normal_uf.R` (que define `paralelizar_meses = TRUE`):
[`future`](https://future.futureverse.org/),
[`doFuture`](https://dofuture.futureverse.org/), [`foreach`](https://cran.r-project.org/package=foreach).

## Estrutura do repositório

```
ana-hydro-data/
├── R/
│   └── WebService/                   Scripts de extracao via WebService legado da ANA (ver "Como utilizar" acima).
│       ├── 01_get_stations_inventory.R   Baixa e padroniza o inventario cadastral de estacoes (tipo 1 e 2).
│       ├── 02_get_daily_series.R         Baixa as series diarias, a partir do inventario gerado acima.
│       └── 03_download_normal_uf.R       Baixa series diarias particionando por normal climatologica e UF
│                                          (resumo consolidado + estrutura de diretorios), reaproveitando
│                                          as funcoes de 02_get_daily_series.R. Ver secao sobre o TCC abaixo.
├── LICENSE                           Texto integral da GNU GPL v3.0 (ou posterior).
├── CITATION.cff                      Metadados de citacao, no formato Citation File Format v1.2.0.
├── README.md                         Este arquivo.
└── .gitignore                        Regras de exclusao para projetos R.
```

> **Nota sobre `R/WebService/`:** os scripts atuais ficam agrupados nesta
> subpasta porque consomem o *WebService* legado da ANA (baseado em
> XML/SOAP).

## Documentação completa dos dados

Este repositório contém a **lógica de aquisição** dos dados. A documentação
técnica completa sobre os dados em si — dicionário de variáveis, metodologia
de georreferenciamento, limitações conhecidas da base da ANA e controle de
qualidade — foi compilada separadamente pelo CIDACS/Fiocruz e está disponível
em:
[plataforma-clima-ambiente-readthedocs.readthedocs.io](https://plataforma-clima-ambiente-readthedocs.readthedocs.io/en/latest/index.html)

## Relação com o TCC de origem

`R/WebService/03_download_normal_uf.R` foi desenvolvido no âmbito do
Trabalho de Conclusão de Curso **"Automação e Curadoria de Séries Históricas
Hidrometeorológicas da ANA via Pipeline Computacional em Linguagem R"**
(Eustorgio Filho, M.A.; De Paula, D.A., 2026), apresentado ao MBA em Data Science e
Analytics da USP/Esalq, implementando a metodologia de download por normal
climatológica e por Unidade Federativa ali descrita.

Os demais scripts deste repositório (`01_get_stations_inventory.R` e
`02_get_daily_series.R`) são de uso geral — qualquer período, qualquer lista
de estações — e não estão vinculados a esse trabalho específico.

Como `03_download_normal_uf.R` reaproveita as funções de download por
estação do script 02, recomenda-se citar uma *release*/tag específica deste
repositório (associada a um DOI do Zenodo — ver badge no topo deste README)
ao referenciá-lo a partir de um trabalho acadêmico, em vez do branch
principal, garantindo que a citação permaneça estável mesmo que o
repositório evolua no futuro.

## Como Citar / Citation

**ABNT:**

```
EUSTORGIO FILHO, Marcos Aurélio. ana-hydro-data: pipeline
reprodutível para aquisição de séries históricas hidrometeorológicas da
ANA. 2026. Disponível em: <https://github.com/MarcosEustorgioFilho/ana-hydro-data>.
Acesso em: [DATA DE ACESSO].
```

**IEEE/ACM:**

```
M. A. Eustorgio Filho, "ana-hydro-data: reproducible pipeline
for acquisition of Brazilian hydrometeorological historical series (ANA),"
2026. [Online]. Available: https://github.com/MarcosEustorgioFilho/ana-hydro-data
```

Uma citação estruturada também está disponível no arquivo
[`CITATION.cff`](CITATION.cff) deste repositório, reconhecido automaticamente
pelo GitHub (botão "Cite this repository").

## Licença

Este projeto está licenciado sob a GNU GPL v3.0 (ou posterior) — ver arquivo
[`LICENSE`](LICENSE).

## Autor / Contato

**Marcos Aurélio Eustorgio Filho**

- LinkedIn: [marcos-eustorgio](https://www.linkedin.com/in/marcos-eustorgio)
- GitHub: [MarcosEustorgioFilho](https://github.com/MarcosEustorgioFilho)
- ORCID: [0000-0003-4596-5896](https://orcid.org/0000-0003-4596-5896)
- Lattes: [lattes.cnpq.br/0006073382117233](http://lattes.cnpq.br/0006073382117233)
