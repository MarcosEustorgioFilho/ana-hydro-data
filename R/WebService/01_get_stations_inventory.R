# ana-hydro-data: pipeline reprodutível para aquisição de séries históricas hidrometeorológicas da ANA
# Copyright (C) 2026 Marcos Aurélio Eustorgio Filho
# Licensed under the GNU GPLv3
# =============================================================================
# ana-hydro-data — 01_get_stations_inventory.R
# -----------------------------------------------------------------------------
# Objetivo:
#   Baixar e padronizar o inventario cadastral de estacoes hidrometeorologicas
#   da ANA (Agencia Nacional de Aguas e Saneamento Basico), para os dois tipos
#   de estacao disponiveis no WebService HidroInventario:
#     * '1' = Fluviometrica (nivel/cota e descarga/vazao — mesmo cadastro)
#     * '2' = Pluviometrica (chuva)
#   O resultado e um arquivo de listagem de estacoes (uma linha por estacao),
#   que serve como entrada obrigatoria do script 02_get_daily_series.R.
#
# Autor:   Marcos Aurelio Eustorgio Filho
# Criado:  04/09/2026
#
# Codificacao: este arquivo e todos os dados que ele gera usam UTF-8. Se o seu
#   editor exibir caracteres estranhos ao abrir este script, configure-o para
#   ler arquivos como UTF-8 (no RStudio isso ja e o padrao). O CSV exportado
#   inclui um BOM (Byte Order Mark) para garantir que aplicativos como o
#   Microsoft Excel abram corretamente nomes com acentuacao (ver Bloco 9).
#
# Observacoes gerais: nenhum caminho de arquivo e fixo no codigo — o
#   diretorio de saida (`dir_saida`) e sempre um parametro obrigatorio. O
#   formato padrao de exportacao e CSV (via 'readr'), por nao depender do
#   pacote 'arrow'; quem preferir Parquet usa `formato_saida = "parquet"`. O
#   filtro por data de cadastro (`data_limite_inicio`) e opcional — a
#   elegibilidade real por periodo de download fica a cargo do script
#   02_get_daily_series.R.
#
# Pacotes utilizados: httr2, xml2, dplyr, purrr, lubridate, readr, stringr,
#                      tidyr (+ arrow, apenas se formato_saida = "parquet")
# =============================================================================

# ==== Bloco 0 — Pacotes ------------------------------------------------------
library(httr2)
library(xml2)
library(dplyr)
library(purrr)
library(lubridate)
library(readr)
library(stringr)
library(tidyr)

# ==== Bloco 1 — Configuracoes e helpers gerais -------------------------------

ANA_INVENT_URL <- "https://telemetriaws1.ana.gov.br/ServiceANA.asmx/HidroInventario"

# Extrai texto de uma tag com XPath simples (apos xml_ns_strip)
xtext <- function(no, tag) {
  out <- xml2::xml_find_first(no, paste0(".//", tag))
  if (inherits(out, "xml_missing")) return(NA_character_)
  val <- xml2::xml_text(out)
  if (!nzchar(val)) NA_character_ else val
}

# Conversores basicos
to_num  <- function(x) readr::parse_number(x, locale = locale(decimal_mark = ".", grouping_mark = ","))
to_date <- function(x) suppressWarnings(lubridate::as_date(x))
to_dt   <- function(x) suppressWarnings(lubridate::ymd_hms(x, quiet = TRUE))

# Se latitude/longitude vierem sem ponto decimal, insere ponto 5 casas do fim.
# Em levantamentos anteriores foram detectadas coordenadas com erro de
# registro (localizacoes geograficas invalidas) quando o ponto decimal vem
# ausente na resposta da ANA.
ajustar_coord_se_preciso <- function(x_chr) {
  ifelse(
    is.na(x_chr),
    NA_character_,
    ifelse(!stringr::str_detect(x_chr, "\\.") & nchar(x_chr) > 5,
           paste0(stringr::str_sub(x_chr, end = -6), ".", stringr::str_sub(x_chr, start = -5)),
           x_chr)
  )
}

# Mapeia tipo -> "apelido" (slug) para usar no nome dos arquivos exportados.
# Implementado com ifelse() vetorizado em base R (em vez de dplyr::case_match)
# para evitar depender de uma funcao especifica de versao do dplyr.
slug_do_tipo <- function(tipo) {
  tipo_chr <- as.character(tipo)
  ifelse(
    tipo_chr == "1", "level",
    ifelse(tipo_chr == "2", "precipitation", paste0("type_", tipo_chr))
  )
}

# ==== Bloco 2 — Campos cadastrais do inventario (unicos para tipo 1 e 2) -----
# Observacao: os cadastros fluviometricos e pluviometricos compartilham o mesmo schema.
# Observacao: nomes de campos retirados do XML da ANA
CAMPOS_INVENTARIO <- c(
  "BaciaCodigo","SubBaciaCodigo","RioCodigo","RioNome",
  "EstadoCodigo","nmEstado","MunicipioCodigo","nmMunicipio",
  "ResponsavelCodigo","ResponsavelSigla","ResponsavelUnidade","ResponsavelJurisdicao",
  "OperadoraCodigo","OperadoraSigla","OperadoraUnidade","OperadoraSubUnidade",
  "TipoEstacao","Codigo","Nome","CodigoAdicional",
  "Latitude","Longitude","Altitude","AreaDrenagem",
  "TipoEstacaoEscala","TipoEstacaoRegistradorNivel","TipoEstacaoDescLiquida",
  "TipoEstacaoSedimentos","TipoEstacaoQualAgua","TipoEstacaoPluviometro","TipoEstacaoRegistradorChuva",
  "TipoEstacaoTanqueEvapo","TipoEstacaoClimatologica","TipoEstacaoPiezometria","TipoEstacaoTelemetrica",
  "PeriodoEscalaInicio","PeriodoEscalaFim","PeriodoRegistradorNivelInicio","PeriodoRegistradorNivelFim",
  "PeriodoDescLiquidaInicio","PeriodoDescLiquidaFim","PeriodoSedimentosInicio","PeriodoSedimentosFim",
  "PeriodoQualAguaInicio","PeriodoQualAguaFim","PeriodoPluviometroInicio","PeriodoPluviometroFim",
  "PeriodoRegistradorChuvaInicio","PeriodoRegistradorChuvaFim","PeriodoTanqueEvapoInicio","PeriodoTanqueEvapoFim",
  "PeriodoClimatologicaInicio","PeriodoClimatologicaFim","PeriodoPiezometriaInicio","PeriodoPiezometriaFim",
  "PeriodoTelemetricaInicio","PeriodoTelemetricaFim",
  "TipoRedeBasica","TipoRedeEnergetica","TipoRedeNavegacao","TipoRedeCursoDagua",
  "TipoRedeEstrategica","TipoRedeCaptacao","TipoRedeSedimentos","TipoRedeQualAgua","TipoRedeClasseVazao",
  "UltimaAtualizacao","Operando","Descricao","NumImagens","DataIns","DataAlt"
)

# ==== Bloco 3 — Colunas candidatas a Data de Inicio e Fim de operacao, por tipo ----
# Por que estas colunas?
# - Tipo 1 (fluviometrica): inicio/fim de operacao pode estar associado a escala
#   (nivel), registrador de nivel, descarga liquida e, em alguns casos, qualidade
#   d'agua/telemetria.
# - Tipo 2 (pluviometrica): inicio/fim tipicamente vem de pluviometro e/ou
#   registrador de chuva, alem da telemetria e da rede climatologica.
datas_inicio_por_tipo <- list(
  `1` = c("PeriodoEscalaInicio","PeriodoRegistradorNivelInicio","PeriodoDescLiquidaInicio",
          "PeriodoQualAguaInicio","PeriodoTelemetricaInicio"),
  `2` = c("PeriodoPluviometroInicio","PeriodoRegistradorChuvaInicio",
          "PeriodoTelemetricaInicio","PeriodoClimatologicaInicio")
)

datas_fim_por_tipo <- list(
  `1` = c("PeriodoEscalaFim","PeriodoRegistradorNivelFim","PeriodoDescLiquidaFim",
          "PeriodoQualAguaFim","PeriodoTelemetricaFim"),
  `2` = c("PeriodoPluviometroFim","PeriodoRegistradorChuvaFim",
          "PeriodoTelemetricaFim","PeriodoClimatologicaFim")
)

# ==== Bloco 4 — Requisicao e parsing do inventario ANA (funcao principal de download) ----
# Parametros:
# - tipo: "1" (fluviometrica) ou "2" (pluviometrica)
# - uf: nome por extenso, em caixa alta e com acentuacao correta (ex.: "BAHIA").
#   Se NULL, consulta TODAS as UFs em uma unica chamada (nmEstado = "").
# - pausa_seg: pausa opcional apos a chamada (util quando chamada em sequencia
#   dentro de um loop por UF).
#
# Parametros de retry/backoff/timeout:
#   - timeout: 180s
#   - max_tries: 6
#   - backoff: min(90, 2^(tentativa - 1))  -> 1,2,4,8,16,32... limitado a 90s
#   - condicao de retry (is_transient): erro de conexao, HTTP >= 500 ou HTTP 429
baixar_hidroinventario <- function(tipo, uf = NULL, pausa_seg = 0) {
  if (missing(tipo) || !(tipo %in% c("1", "2"))) {
    stop("Tipo de estacao invalido. Use '1' para fluviometricas ou '2' para pluviometricas.", call. = FALSE)
  }

  req <- httr2::request(ANA_INVENT_URL) |>
    httr2::req_user_agent("ana-hydro-data/1.0 (minimal reproducible pipeline)") |>
    httr2::req_url_query(
      codEstDE = "", codEstATE = "", tpEst = tipo, nmEst = "", nmRio = "",
      codSubBacia = "", codBacia = "", nmMunicipio = "",
      nmEstado = ifelse(is.null(uf), "", uf),
      sgResp = "", sgOper = "", telemetrica = ""
    ) |>
    httr2::req_timeout(180) |>
    httr2::req_retry(
      max_tries = 6,
      backoff = function(attempt) min(90, 2^(attempt - 1)),
      is_transient = function(x) {
        if (inherits(x, "error")) return(TRUE)
        st <- tryCatch(httr2::resp_status(x), error = function(e) NA_integer_)
        isTRUE(st >= 500L) || isTRUE(st == 429L)
      }
    )

  resp <- httr2::req_perform(req)
  if (httr2::resp_status(resp) != 200) {
    cat(sprintf("Falha HTTP %s (UF=%s, tipo=%s)\n", httr2::resp_status(resp),
                ifelse(is.null(uf), "TODAS", uf), tipo))
    if (pausa_seg > 0) Sys.sleep(pausa_seg)
    return(tibble::tibble())
  }

  doc <- xml2::read_xml(httr2::resp_body_string(resp))
  xml2::xml_ns_strip(doc)

  err_node <- xml2::xml_find_first(doc, ".//Error")
  if (!inherits(err_node, "xml_missing")) {
    msg <- xml2::xml_text(err_node)
    if (nzchar(msg)) {
      cat(sprintf("UF=%s, tipo=%s: %s\n", ifelse(is.null(uf), "TODAS", uf), tipo, msg))
      if (pausa_seg > 0) Sys.sleep(pausa_seg)
      return(tibble::tibble())
    }
  }

  tables <- xml2::xml_find_all(doc, ".//Table")
  if (length(tables) == 0) {
    cat(sprintf("UF=%s, tipo=%s sem nos <Table>.\n", ifelse(is.null(uf), "TODAS", uf), tipo))
    if (pausa_seg > 0) Sys.sleep(pausa_seg)
    return(tibble::tibble())
  }

  out <- purrr::map_dfr(tables, function(tb) {
    vals <- setNames(lapply(CAMPOS_INVENTARIO, function(cmp) xtext(tb, cmp)), CAMPOS_INVENTARIO)
    tibble::as_tibble(vals)
  })

  out$tipo_estacao <- as.character(tipo)
  if (pausa_seg > 0) Sys.sleep(pausa_seg)

  out
}

# ==== Bloco 5 — Limpeza e tipagem apos concatenar ----------------------------
# Ordem de aplicacao: coordenadas -> numericos; inteiros; datas
# (Periodo...Inicio/Fim); datetimes administrativos; textos-chave por ultimo.
limpar_tipos_pos_concat <- function(df) {
  if (nrow(df) == 0) return(df)

  num_candidatas <- c("Latitude","Longitude","Altitude")
  for (coordenadas in intersect(num_candidatas, names(df))) {
    if (coordenadas %in% c("Latitude","Longitude")) {
      df[[coordenadas]] <- ajustar_coord_se_preciso(as.character(df[[coordenadas]]))
    }
    df[[coordenadas]] <- to_num(df[[coordenadas]])
  }

  int_candidatas <- c(
    "BaciaCodigo","SubBaciaCodigo","RioCodigo",
    "EstadoCodigo","MunicipioCodigo","ResponsavelCodigo","OperadoraCodigo",
    "TipoEstacao","TipoEstacaoEscala","TipoEstacaoRegistradorNivel","TipoEstacaoDescLiquida",
    "TipoEstacaoSedimentos","TipoEstacaoQualAgua","TipoEstacaoPluviometro","TipoEstacaoRegistradorChuva",
    "TipoEstacaoTanqueEvapo","TipoEstacaoClimatologica","TipoEstacaoPiezometria","TipoEstacaoTelemetrica",
    "TipoRedeBasica","TipoRedeEnergetica","TipoRedeNavegacao","TipoRedeCursoDagua","TipoRedeEstrategica",
    "TipoRedeCaptacao","TipoRedeSedimentos","TipoRedeQualAgua","TipoRedeClasseVazao","Operando"
  )
  for (var_int in intersect(int_candidatas, names(df))) {
    df[[var_int]] <- suppressWarnings(as.integer(df[[var_int]]))
  }

  cols_data <- grep("^Periodo.*(Inicio|Fim)$", names(df), value = TRUE)
  for (var_datas in cols_data) df[[var_datas]] <- to_date(df[[var_datas]])

  cols_dt <- intersect(c("UltimaAtualizacao","DataIns","DataAlt"), names(df))
  for (var_datas in cols_dt) df[[var_datas]] <- to_dt(df[[var_datas]])

  cols_txt <- intersect(c("nmEstado","nmMunicipio","ResponsavelSigla","OperadoraSigla",
                           "Nome","Codigo","CodigoAdicional","RioNome","Descricao"), names(df))
  for (var_texto in cols_txt) df[[var_texto]] <- dplyr::na_if(stringr::str_squish(df[[var_texto]]), "")

  df
}

# ==== Bloco 6 — Data de Inicio/Fim de Operacao por tipo ----------------------
# DataInicioOperacao / DataFimOperacao NAO sao campos brutos da ANA: sao
# derivados como o menor / maior valor valido entre as colunas candidatas
# (Bloco 3), por tipo de estacao.
derivar_inicio_operacao <- function(df, mapa_datas = datas_inicio_por_tipo) {
  if (nrow(df) == 0) return(df)
  df$DataInicioOperacao <- as.Date(NA)
  for (t in unique(df$tipo_estacao)) {
    cols <- mapa_datas[[as.character(t)]]
    if (is.null(cols)) next
    cols_pres <- intersect(cols, names(df))
    if (length(cols_pres) == 0) next
    idx <- df$tipo_estacao == t
    df[idx, "DataInicioOperacao"] <- do.call(pmin, c(df[idx, cols_pres], list(na.rm = TRUE)))
  }
  df
}

derivar_fim_operacao <- function(df, mapa_datas = datas_fim_por_tipo) {
  if (nrow(df) == 0) return(df)
  df$DataFimOperacao <- as.Date(NA)
  for (t in unique(df$tipo_estacao)) {
    cols <- mapa_datas[[as.character(t)]]
    if (is.null(cols)) next
    cols_pres <- intersect(cols, names(df))
    if (length(cols_pres) == 0) next
    idx <- df$tipo_estacao == t
    df[idx, "DataFimOperacao"] <- do.call(pmax, c(df[idx, cols_pres], list(na.rm = TRUE)))
  }
  df
}

# ==== Bloco 7 — Deduplicacao por Codigo (mantem cadastro mais recente) -------
# ANA pode conter mais de um registro para o mesmo codigo de estacao (revisoes
# administrativas). Mantem 1 linha por Codigo, priorizando DataAlt > DataIns >
# UltimaAtualizacao (mais recente primeiro).
dedup_por_codigo_recente <- function(df) {
  if (nrow(df) == 0) return(df)
  df |>
    dplyr::arrange(
      Codigo,
      dplyr::desc(!is.na(DataAlt)),           dplyr::desc(DataAlt),
      dplyr::desc(!is.na(DataIns)),           dplyr::desc(DataIns),
      dplyr::desc(!is.na(UltimaAtualizacao)), dplyr::desc(UltimaAtualizacao)
    ) |>
    dplyr::distinct(Codigo, .keep_all = TRUE)
}

# ==== Bloco 8 — Validacao geografica e ajuste de altitude --------------------
# Faixas geograficas do territorio brasileiro usadas para validacao:
# lat/lon: Arroio Chui (RS) / Monte Caburai (RR) / Serra do Divisor (AC) /
# Ilha do Sul (ES); altitude maxima: Pico da Neblina (AM).
# Estacoes fora da faixa sao apenas SINALIZADAS (lat_fora/lon_fora = 1), nao removidas.
validar_ajustar_geo <- function(df) {
  if (nrow(df) == 0) return(df)

  lat_min <- -33.75; lat_max <-  5.27
  lon_min <- -74.00; lon_max <- -28.83
  alt_max <- 3000

  df <- df |>
    dplyr::mutate(
      Altitude = dplyr::if_else(!is.na(Altitude) & Altitude < 0 & Altitude > -10, 0, Altitude),
      Altitude = dplyr::if_else(!is.na(Altitude) & (Altitude > alt_max | Altitude < -10), NA_real_, Altitude),
      lat_fora = dplyr::if_else(!is.na(Latitude)  & (Latitude  < lat_min | Latitude  > lat_max), 1L, 0L),
      lon_fora = dplyr::if_else(!is.na(Longitude) & (Longitude < lon_min | Longitude > lon_max), 1L, 0L)
    )

  cat(sprintf("%d latitudes e %d longitudes fora dos limites do territorio brasileiro.\n",
              sum(df$lat_fora, na.rm = TRUE), sum(df$lon_fora, na.rm = TRUE)))

  df
}

# ==== Bloco 9 — Pipeline principal (funcao de entrada do script) ------------
# get_stations_inventory() coordena a coleta (nacional ou por UF), o
# pos-processamento e a exportacao do inventario, por tipo de estacao.
# Parametros principais: `tipos` (vetor com "1" e/ou "2"); `uf` (NULL para
# consulta nacional, ou vetor de nomes de UF por extenso/caixa alta); `dir_saida`
# (obrigatorio); `formato_saida` ("csv" ou "parquet"); `nome_arquivo` (opcional,
# default gera "HidroInventario_<slug>_stations_ANA"). Retorna uma lista
# nomeada por tipo, com `$dados` (tibble tratado) e `$resumo_uf` (contagem de
# estacoes por UF).
get_stations_inventory <- function(
    tipos = c("1", "2"),
    uf = NULL,
    pausa_seg = 5,
    data_limite_inicio = NULL,
    dir_saida,
    formato_saida = c("csv", "parquet"),
    nome_arquivo = NULL
) {
  formato_saida <- match.arg(formato_saida)

  if (missing(dir_saida) || is.null(dir_saida) || !nzchar(dir_saida)) {
    stop("Parametro 'dir_saida' e obrigatorio: informe o diretorio onde os arquivos de estacoes serao gravados.", call. = FALSE)
  }
  if (formato_saida == "parquet" && !requireNamespace("arrow", quietly = TRUE)) {
    stop("formato_saida = 'parquet' requer o pacote 'arrow' instalado. Use formato_saida = 'csv' ou instale 'arrow'.", call. = FALSE)
  }

  if (!dir.exists(dir_saida)) {
    dir.create(dir_saida, recursive = TRUE)
    cat(sprintf("Diretorio de saida criado: %s\n", dir_saida))
  }

  resultados <- list()

  for (tp in tipos) {
    cat("\n==============================================================================\n")
    cat(sprintf(">>> Baixando inventario ANA - Tipo %s (%s)\n",
                tp, ifelse(tp == "1", "fluviometrica", "pluviometrica")))
    cat("==============================================================================\n")

    # 9.1 — Coleta: uma chamada nacional (uf = NULL) ou loop pelas UFs informadas
    if (is.null(uf)) {
      inv_bruto <- baixar_hidroinventario(tipo = tp, uf = NULL, pausa_seg = 0)
    } else {
      listas <- vector("list", length(uf))
      for (i in seq_along(uf)) {
        uf_nome <- uf[i]
        cat(sprintf("UF: %s ... ", uf_nome))
        tbi <- baixar_hidroinventario(tipo = tp, uf = uf_nome, pausa_seg = pausa_seg)
        cat(sprintf("%d linhas\n", nrow(tbi)))
        listas[[i]] <- tbi
      }
      inv_bruto <- dplyr::bind_rows(listas)
    }

    # 9.2 — Pos-processamento (tipagem/geo, datas de inicio/fim, dedup)
    inv_tratado <- inv_bruto |>
      limpar_tipos_pos_concat() |>
      derivar_inicio_operacao(datas_inicio_por_tipo) |>
      derivar_fim_operacao(datas_fim_por_tipo) |>
      dedup_por_codigo_recente() |>
      validar_ajustar_geo() |>
      dplyr::relocate(
        tipo_estacao, Codigo, Nome, nmEstado, EstadoCodigo, nmMunicipio, MunicipioCodigo,
        Latitude, Longitude, Altitude, DataInicioOperacao, DataFimOperacao,
        .before = dplyr::everything()
      )

    # 9.3 — Filtro opcional por data de cadastro (data_limite_inicio)
    inv_final <- inv_tratado
    if (!is.null(data_limite_inicio)) {
      inv_final <- inv_tratado |>
        dplyr::filter(!is.na(DataInicioOperacao) & DataInicioOperacao <= data_limite_inicio)
    }

    resumo_uf <- inv_final |>
      dplyr::count(nmEstado, name = "n_estacoes") |>
      dplyr::arrange(dplyr::desc(n_estacoes))

    # 9.4 — Nome do arquivo de saida (padrao ou customizado via nome_arquivo)
    slug <- slug_do_tipo(tp)
    nome_base <- if (is.null(nome_arquivo)) {
      sprintf("HidroInventario_%s_stations_ANA", slug)
    } else if (length(tipos) > 1) {
      sprintf("%s_%s", nome_arquivo, slug)
    } else {
      nome_arquivo
    }

    # 9.5 — Exportacao (CSV por padrao; Parquet se solicitado)
    # CSV e escrito com readr::write_excel_csv(), que inclui um BOM UTF-8 no
    # inicio do arquivo. Isso e necessario porque, sem o BOM, aplicativos como
    # o Microsoft Excel tendem a assumir a codificacao ANSI/Latin-1 padrao do
    # Windows ao abrir um .csv, exibindo nomes com acentuacao de forma
    # incorreta mesmo que o arquivo esteja, de fato, em UTF-8 valido.
    if (formato_saida == "csv") {
      caminho_saida <- file.path(dir_saida, paste0(nome_base, ".csv"))
      readr::write_excel_csv(inv_final, caminho_saida)
    } else {
      caminho_saida <- file.path(dir_saida, paste0(nome_base, ".parquet"))
      arrow::write_parquet(inv_final, caminho_saida, compression = "gzip")
    }
    cat(sprintf("Arquivo de estacoes exportado (tipo %s, %d linhas):\n- %s\n", tp, nrow(inv_final), caminho_saida))

    resultados[[as.character(tp)]] <- list(dados = inv_final, resumo_uf = resumo_uf)
  }

  resultados
}

# =============================================================================
# Exemplo de uso:
# =============================================================================
#
# # 1) Baixar inventario nacional (todas as UFs em uma unica chamada por tipo)
# #    para os dois tipos de estacao, sem filtro de data de cadastro, salvando
# #    em um diretorio local de saida (qualquer caminho, escolhido pelo usuario),
# #    com o nome de arquivo padrao (nome_arquivo = NULL):
#
# res_inventario <- get_stations_inventory(
#   tipos = c("1", "2"),
#   uf = NULL,
#   dir_saida = "dados/estacoes",
#   formato_saida = "csv"
# )
#
# # Inspecionando o resultado:
# nrow(res_inventario$`1`$dados)   # estacoes fluviometricas
# nrow(res_inventario$`2`$dados)   # estacoes pluviometricas
#
# # 2) Baixar por UF (uma requisicao por estado — mais lento, porem mais
# #    estavel para consultas nacionais completas), com filtro de cadastro e
# #    nome de arquivo customizado:
#
# estados_exemplo <- c("BAHIA", "ALAGOAS", "SERGIPE")
# res_uf <- get_stations_inventory(
#   tipos = "2",
#   uf = estados_exemplo,
#   pausa_seg = 3,
#   data_limite_inicio = as.Date("2023-12-31"),
#   dir_saida = "dados/estacoes",
#   formato_saida = "csv",
#   nome_arquivo = "estacoes_chuva_ba_al_se"
# )
#
# Observacao: caso utilize este metodo para baixar inventario de todas as UF's,
# deve ser fornecido vetor contendo todas os estados, com escrita em caixa alta
# (padrao ANA).
#
# # O arquivo gerado em `dir_saida` (ex.: "dados/estacoes/HidroInventario_precipitation_stations_ANA.csv")
# # e o arquivo que deve ser passado como entrada obrigatoria para
# # 02_get_daily_series.R (parametro `arquivo_estacoes`).
