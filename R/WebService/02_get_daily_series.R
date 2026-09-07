# ana-hydro-data: pipeline reprodutível para aquisição de séries históricas hidrometeorológicas da ANA
# Copyright (C) 2026 Marcos Aurélio Eustorgio Filho
# Licensed under the GNU GPLv3
# =============================================================================
# ana-hydro-data — 02_get_daily_series.R
# -----------------------------------------------------------------------------
# Objetivo:
#   Baixar as series historicas diarias (cota, chuva ou vazao) de um conjunto
#   arbitrario de estacoes hidrometeorologicas da ANA, a partir do arquivo de
#   listagem de estacoes gerado por 01_get_stations_inventory.R, e classificar
#   o resultado de cada estacao em uma de 3 categorias: dado valido, sem dados
#   no periodo, ou falha tecnica.
#
# Autor:   Marcos Aurelio Eustorgio Filho
# Criado:  04/09/2026
#
# Codificacao: este arquivo e todos os dados que ele gera usam UTF-8. Se o seu
#   editor exibir caracteres estranhos ao abrir este script, configure-o para
#   ler arquivos como UTF-8 (no RStudio isso ja e o padrao). Nos exemplos de
#   uso ao final deste arquivo, CSVs sao escritos com
#   readr::write_excel_csv() (inclui BOM UTF-8), para que aplicativos como o
#   Microsoft Excel exibam corretamente nomes com acentuacao.
#
# Entrada obrigatoria:
#   Um arquivo de listagem de estacoes (.csv ou .parquet) gerado por
#   01_get_stations_inventory.R, contendo no minimo as colunas:
#     - Codigo             (character) codigo da estacao na ANA
#     - DataInicioOperacao (Date)      data de inicio de operacao derivada
#   Nenhuma outra coluna do inventario e exigida por este script: a listagem
#   pode conter estacoes de multiplas UFs misturadas, em qualquer ordem.
#
# Observacoes gerais:
#   Nenhum caminho de arquivo e fixo no codigo. O processamento aceita uma
#   listagem arbitraria de estacoes (varias UFs misturadas) e roda em um
#   unico loop por estacao, sem escrita de arquivo por estado — uma estacao e
#   elegivel para o periodo [start_date, end_date] se
#   `DataInicioOperacao <= end_date` (regra permissiva, pensada para aceitar
#   qualquer periodo arbitrario; se o seu caso de uso exige um minimo de
#   tempo previo de operacao, filtre a listagem antes de chamar
#   processar_lote_estacoes()). A funcao de lote RETORNA (nao grava em disco)
#   os resultados classificados em 3 categorias (valido / sem_dados / falha)
#   mais um log de rastreabilidade por estacao — gravar em disco fica a
#   cargo do usuario (ver exemplo de uso ao final). Retry/backoff/timeout sao
#   usados sem simplificacao (timeout 180s; max_tries = 6; backoff
#   min(90, 2^(tentativa-1)); pausa de 6s entre estacoes; pausa de 60s a cada
#   30 estacoes), pois sao a principal defesa contra a instabilidade
#   conhecida do WebService legado da ANA. A paralelizacao entre os meses de
#   uma mesma estacao e opcional (`paralelizar_meses`, default FALSE) e nunca
#   ocorre entre estacoes distintas. Cada estacao roda dentro de tryCatch(),
#   entao uma falha isolada nunca interrompe o lote. Quando ha mais de uma
#   serie candidata para o mesmo mes, a selecao e deterministica (ver Bloco
#   2.7); a data de insercao (`DataIns`) da serie escolhida tambem e
#   armazenada na saida, com o mesmo tratamento dado a `Consistence`.
#
# 02_get_daily_series.R cuida apenas do nivel de ESTACAO (loop, tryCatch,
# classificacao, pausas por estacao) — nao ha orquestracao por UF nem por
# normal climatologica aqui; essa camada, quando necessaria, vive em
# 03_download_normal_uf.R, que reaproveita as funcoes deste arquivo.
#
# Pacotes utilizados (obrigatorios): dplyr, readr, arrow, stringr, purrr,
#   lubridate, httr2, xml2, tidyr, tibble
# Pacotes opcionais (apenas se paralelizar_meses = TRUE): future, doFuture, foreach
# =============================================================================

# ==== Bloco 0 — Pacotes -------------------------------------------------------
suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(arrow)
  library(stringr)
  library(purrr)
  library(lubridate)
  library(httr2)
  library(xml2)
  library(tidyr)
})

# ==== Bloco 1 — Leitura da listagem de estacoes (entrada do script 01) -------

# Carrega a listagem de estacoes gerada por 01_get_stations_inventory.R.
# Aceita .csv (via readr) ou .parquet (via arrow), detectado pela extensao,
# e valida a presenca minima das colunas exigidas pelas demais funcoes.
carregar_listagem_estacoes <- function(arquivo_estacoes) {
  if (missing(arquivo_estacoes) || !nzchar(arquivo_estacoes)) {
    stop("Parametro 'arquivo_estacoes' e obrigatorio: informe o caminho do arquivo gerado por 01_get_stations_inventory.R.", call. = FALSE)
  }
  if (!file.exists(arquivo_estacoes)) {
    stop("Arquivo de estacoes nao encontrado: ", arquivo_estacoes, call. = FALSE)
  }

  ext <- tolower(tools::file_ext(arquivo_estacoes))
  estacoes <- switch(
    ext,
    "csv"     = readr::read_csv(arquivo_estacoes, show_col_types = FALSE, locale = readr::locale(encoding = "UTF-8")),
    "parquet" = arrow::read_parquet(arquivo_estacoes),
    stop("Extensao de arquivo nao suportada: '", ext, "'. Use .csv ou .parquet.", call. = FALSE)
  )

  colunas_exigidas <- c("Codigo", "DataInicioOperacao")
  faltantes <- setdiff(colunas_exigidas, names(estacoes))
  if (length(faltantes) > 0) {
    stop(sprintf(
      "O arquivo de estacoes nao contem a(s) coluna(s) exigida(s): %s. Este arquivo deve ser a saida de 01_get_stations_inventory.R.",
      paste(faltantes, collapse = ", ")
    ), call. = FALSE)
  }

  estacoes$DataInicioOperacao <- suppressWarnings(lubridate::as_date(estacoes$DataInicioOperacao))
  estacoes
}

# ==== Bloco 2 — Download e consolidacao de UMA estacao -----------------------

# Baixa e consolida a serie diaria de UMA estacao da ANA: requisicao unica
# por estacao, com retry/backoff/timeout, selecao mensal deterministica e
# parsing diario (ver Bloco 2.7 para os criterios de selecao/desempate).
# Parametros: station (codigo), start_date/end_date (periodo arbitrario),
# typedata ("1" cota | "2" chuva | "3" vazao), paralelizar_meses (default
# FALSE) e workers (numero de processos paralelos, so usado se
# paralelizar_meses = TRUE; default NULL = nucleos_disponiveis - 1).
# Retorna um tibble longo com as colunas Date, Value, Consistence, Status,
# Code, DataIns.
get_ana_station_series <- function(station, start_date, end_date, typedata, paralelizar_meses = FALSE, workers = NULL) {

  # BLOCO 2.0 — Endpoint -------------------------------------------------
  url <- "https://telemetriaws1.ana.gov.br/ServiceANA.asmx/HidroSerieHistorica"

  # BLOCO 2.1 — Prefixo XML por tipo de dado ------------------------------
  tipo_xml <- switch(
    typedata,
    "1" = "Cota", "2" = "Chuva", "3" = "Vazao",
    stop("typedata invalido: use '1' (cota), '2' (chuva) ou '3' (vazao)", call. = FALSE)
  )

  # BLOCO 2.2 — Helpers XML -------------------------------------------------
  xtext <- function(node, xpath) {
    out <- xml2::xml_find_first(node, xpath)
    if (length(out) == 0) return(NA_character_)
    val <- trimws(xml2::xml_text(out))
    if (!nzchar(val)) NA_character_ else val
  }
  xname <- function(node, xpath) {
    out <- xml2::xml_find_first(node, xpath)
    if (length(out) == 0) return(NA_character_)
    nm <- xml2::xml_name(out)
    if (length(nm) == 0) NA_character_ else nm
  }

  # BLOCO 2.3 — Datas e grade mensal ----------------------------------------
  start_date <- lubridate::ymd(start_date)
  end_date   <- lubridate::ymd(end_date)

  combinacoes <- expand.grid(
    year  = lubridate::year(start_date):lubridate::year(end_date),
    month = 1:12
  ) |>
    dplyr::mutate(data_mes = lubridate::ymd(paste(year, month, "01"))) |>
    dplyr::filter(
      data_mes >= lubridate::floor_date(start_date, "month"),
      data_mes <= lubridate::floor_date(end_date, "month")
    ) |>
    dplyr::arrange(year, month)

  # BLOCO 2.4 — Requisicao HTTP robusta (retry/backoff/timeout) --------------
  req <- httr2::request(url) |>
    httr2::req_user_agent("ana-hydro-data/1.0 (minimal reproducible pipeline)") |>
    httr2::req_url_query(
      codEstacao        = station,
      dataInicio        = format(start_date, "%Y-%m-%d"),
      dataFim           = format(end_date, "%Y-%m-%d"),
      tipoDados         = typedata,
      nivelConsistencia = ""
    ) |>
    httr2::req_timeout(seconds = 180) |>
    httr2::req_retry(
      max_tries = 6,
      backoff = function(attempt) min(90, 2^(attempt - 1)),
      is_transient = function(x) {
        if (inherits(x, "error")) return(TRUE)
        st <- tryCatch(httr2::resp_status(x), error = function(e) NA_integer_)
        isTRUE(st >= 500L) || isTRUE(st == 429L)
      }
    )

  # BLOCO 2.5 — Download, strip de namespaces, deteccao de erro --------------
  resp <- httr2::req_perform(req)
  if (httr2::resp_status(resp) != 200) {
    stop(sprintf("Falha HTTP %s ao baixar estacao %s", httr2::resp_status(resp), station))
  }

  xml_data <- xml2::read_xml(httr2::resp_body_string(resp))
  xml2::xml_ns_strip(xml_data)

  err_node <- xml2::xml_find_first(xml_data, ".//Error")
  if (!inherits(err_node, "xml_missing")) {
    err_msg <- trimws(xml2::xml_text(err_node))
    if (nzchar(err_msg)) {
      cat(sprintf("\nErro - Estacao %s: %s\n\n", station, err_msg))

      start_month <- lubridate::floor_date(start_date, "month")
      end_month   <- lubridate::floor_date(end_date,   "month")
      last_day_end_month <- (end_month %m+% lubridate::period(months = 1)) - lubridate::days(1)
      seq_full <- seq(start_month, last_day_end_month, by = "day")

      return(dplyr::tibble(
        Date = seq_full, Value = NA_real_, Consistence = NA_integer_,
        Status = NA_integer_, Code = station, DataIns = as.POSIXct(NA)
      ))
    }
  }

  # BLOCO 2.6 — Todas as series retornadas -----------------------------------
  series_all <- xml2::xml_find_all(xml_data, ".//SerieHistorica")
  if (length(series_all) == 0) {
    cat(sprintf("\nEstacao %s sem nos SerieHistorica.\n", station))
  }

  series_all_chr <- if (length(series_all) > 0) {
    vapply(series_all, as.character, FUN.VALUE = character(1))
  } else {
    character(0)
  }

  metadados_todas_series <- if (length(series_all) > 0) {
    purrr::map_dfr(seq_along(series_all), function(j) {
      sj <- series_all[[j]]
      tibble::tibble(
        id_serie      = j,
        cons          = suppressWarnings(as.integer(xtext(sj, ".//NivelConsistencia"))),
        data_insercao = suppressWarnings(lubridate::ymd_hms(xtext(sj, ".//DataIns"), quiet = TRUE)),
        data_ref      = suppressWarnings(lubridate::as_date(substr(xtext(sj, ".//DataHora"), 1, 10)))
      )
    })
  } else {
    tibble::tibble(id_serie = integer(0), cons = integer(0),
                    data_insercao = as.POSIXct(character(0)), data_ref = as.Date(character(0)))
  }

  # BLOCO 2.7 — Processamento de UM mes (usado sequencial ou em paralelo) -----
  processar_mes <- function(i) {
    if (nrow(metadados_todas_series) == 0) {
      data_inicio_mes <- combinacoes$data_mes[i]
      data_fim_mes    <- (data_inicio_mes %m+% lubridate::period(months = 1)) - lubridate::days(1)
      sequencia_datas <- seq(data_inicio_mes, data_fim_mes, by = "day")
      values <- dplyr::tibble(Date = sequencia_datas, Value = NA_real_,
                               Consistence = NA_integer_, Status = NA_integer_,
                               DataIns = as.POSIXct(NA))
      values$Code <- station
      return(list(dados = values, ano = lubridate::year(data_inicio_mes),
                  mes = lubridate::month(data_inicio_mes), tem_dados = FALSE))
    }

    data_inicio_mes <- combinacoes$data_mes[i]
    data_fim_mes    <- (data_inicio_mes %m+% lubridate::period(months = 1)) - lubridate::days(1)
    dias_mes        <- lubridate::days_in_month(data_inicio_mes)
    sequencia_datas <- seq(data_inicio_mes, data_fim_mes, by = "day")

    # Selecao de series candidatas por data_ref, em ordem de preferencia:
    # (1) 1o dia do mes -> (2) ultimo dia do mes -> (3) qualquer dia no mes
    info_cand <- metadados_todas_series |> dplyr::filter(!is.na(data_ref))

    series_mes <- info_cand |> dplyr::filter(data_ref == data_inicio_mes)
    if (nrow(series_mes) == 0L) {
      series_mes <- info_cand |> dplyr::filter(data_ref == data_fim_mes)
    }
    if (nrow(series_mes) == 0L) {
      series_mes <- info_cand |> dplyr::filter(data_ref >= data_inicio_mes, data_ref <= data_fim_mes)
    }

    if (nrow(series_mes) > 1L) {
      # Desempate, nesta ordem:
      # 1) maior consistencia -> 2) mais perto do dia 1 -> 3) tem DataIns -> 4) DataIns mais recente
      series_mes <- series_mes |>
        dplyr::mutate(distancia_d1 = abs(as.numeric(data_ref - data_inicio_mes))) |>
        dplyr::arrange(
          dplyr::desc(cons), distancia_d1,
          dplyr::desc(!is.na(data_insercao)), dplyr::desc(data_insercao)
        )
      id_escolhido         <- series_mes$id_serie[1]
      cons_serie_escolhida <- series_mes$cons[1]
      data_insercao_serie_escolhida <- series_mes$data_insercao[1]
    } else if (nrow(series_mes) == 1L) {
      id_escolhido         <- series_mes$id_serie[1]
      cons_serie_escolhida <- series_mes$cons[1]
      data_insercao_serie_escolhida <- series_mes$data_insercao[1]
    } else {
      values <- dplyr::tibble(Date = sequencia_datas, Value = NA_real_,
                               Consistence = NA_integer_, Status = NA_integer_,
                               DataIns = as.POSIXct(NA))
      values$Code <- station
      return(list(dados = values, ano = lubridate::year(data_inicio_mes),
                  mes = lubridate::month(data_inicio_mes), tem_dados = FALSE))
    }

    # Reconstroi o no XML da serie escolhida a partir da string serializada
    serie_xml <- series_all_chr[id_escolhido]
    wrapped <- paste0(
      '<root xmlns:diffgr="urn:schemas-microsoft-com:xml-diffgram-v1" ',
      '      xmlns:msdata="urn:schemas-microsoft-com:xml-msdata">',
      serie_xml, '</root>'
    )
    doc <- xml2::read_xml(wrapped)
    xml2::xml_ns_strip(doc)
    serie_escolhida <- xml2::xml_find_first(doc, ".//SerieHistorica")

    Names <- vector("list", dias_mes); Value <- vector("list", dias_mes); Status <- vector("list", dias_mes)
    for (k in seq_len(dias_mes)) {
      tag_val <- sprintf(".//%s%02d", tipo_xml, k)
      tag_sts <- sprintf(".//%s%02dStatus", tipo_xml, k)
      nm <- xname(serie_escolhida, tag_val)
      vl <- suppressWarnings(as.numeric(xtext(serie_escolhida, tag_val)))
      st <- suppressWarnings(as.integer(xtext(serie_escolhida, tag_sts)))
      Names[[k]]  <- if (is.na(nm)) NA_character_ else nm
      Value[[k]]  <- if (is.na(vl)) NA_real_     else vl
      Status[[k]] <- if (is.na(st)) NA_integer_  else st
    }

    values <- dplyr::tibble(
      Date = sequencia_datas, Names = unlist(Names), Value = as.double(unlist(Value)),
      Consistence = as.integer(cons_serie_escolhida), Status = as.integer(unlist(Status)),
      DataIns = data_insercao_serie_escolhida
    )

    # Desempate defensivo se houver mais linhas que dias no mes: menor Status > 0 vence
    if (nrow(values) > dias_mes) {
      values <- values |>
        dplyr::mutate(Status = as.integer(Status),
                       Prioridade = dplyr::if_else(!is.na(Status) & Status > 0, Status, NA_integer_)) |>
        dplyr::group_by(Names) |>
        dplyr::filter(if (all(is.na(Prioridade))) dplyr::row_number() == 1L else Prioridade == min(Prioridade, na.rm = TRUE)) |>
        dplyr::ungroup() |>
        dplyr::select(-Prioridade)
    }

    values <- values |> dplyr::arrange(Names) |> dplyr::select(-Names)
    values$Code <- station

    list(dados = values, ano = lubridate::year(data_inicio_mes),
         mes = lubridate::month(data_inicio_mes), tem_dados = any(!is.na(values$Value)))
  }

  # BLOCO 2.8 — Execucao do loop mensal: sequencial (default) ou paralelo -----
  if (isTRUE(paralelizar_meses)) {
    if (!requireNamespace("future", quietly = TRUE) ||
        !requireNamespace("doFuture", quietly = TRUE) ||
        !requireNamespace("foreach", quietly = TRUE)) {
      stop("paralelizar_meses = TRUE requer os pacotes 'future', 'doFuture' e 'foreach' instalados.", call. = FALSE)
    }
    workers_efetivos <- if (is.null(workers)) max(1, future::availableCores() - 1) else workers
    future::plan(future::multisession, workers = workers_efetivos)
    doFuture::registerDoFuture()
    on.exit(future::plan(future::sequential), add = TRUE)

    i <- NULL # evita nota do R CMD check sobre variavel de loop do foreach
    resultados_mes <- foreach::foreach(
      i = seq_len(nrow(combinacoes)),
      .options.future = list(packages = c("dplyr", "lubridate", "xml2", "purrr", "tibble"))
    ) %dofuture% { processar_mes(i) }
  } else {
    resultados_mes <- lapply(seq_len(nrow(combinacoes)), processar_mes)
  }

  # BLOCO 2.9 — Consolidacao e log analitico por mes/ano -----------------------
  dados_estacao <- purrr::map(resultados_mes, "dados") |> dplyr::bind_rows() |> dplyr::arrange(Code, Date)

  info_mes <- purrr::map_dfr(resultados_mes, ~tibble::tibble(ano = .x$ano, mes = .x$mes, tem_dados = .x$tem_dados)) |>
    dplyr::arrange(ano, mes)

  # Log "inteligente": agrupa anos inteiros e blocos de meses consecutivos sem
  # dados, inserindo linha em branco apenas nas transicoes relevantes (nunca
  # duas linhas em branco seguidas).
  cat("\n")
  log_lines <- character(0)
  last_print_type   <- NA_character_
  last_printed_year <- NA_integer_
  anos <- unique(info_mes$ano)

  for (a in anos) {
    info_ano <- dplyr::filter(info_mes, ano == a)

    if (all(!info_ano$tem_dados)) {
      if (!is.na(last_print_type) && last_print_type != "year" &&
          length(log_lines) > 0 && utils::tail(log_lines, 1) != "") {
        log_lines <- c(log_lines, "")
      }
      log_lines <- c(log_lines, sprintf("Estacao %s sem dados no ANO %d.", station, a))
      last_print_type  <- "year"
      last_printed_year <- a
    } else {
      info_sem <- dplyr::filter(info_ano, !tem_dados) |> dplyr::arrange(mes)
      if (nrow(info_sem) > 0) {
        if (!is.na(last_printed_year) && last_printed_year != a &&
            length(log_lines) > 0 && utils::tail(log_lines, 1) != "") {
          log_lines <- c(log_lines, "")
        }
        grp <- cumsum(c(TRUE, diff(info_sem$mes) != 1L))
        info_sem$grp <- grp
        blocks <- split(info_sem, info_sem$grp)
        for (b in seq_along(blocks)) {
          blk <- blocks[[b]]
          if (b > 1 && length(log_lines) > 0 && utils::tail(log_lines, 1) != "") {
            log_lines <- c(log_lines, "")
          }
          log_lines <- c(log_lines, sprintf("Estacao %s sem dados no MES %02d/%d.", station, as.integer(blk$mes), as.integer(blk$ano)))
        }
        last_print_type   <- "month"
        last_printed_year <- a
      }
    }
  }
  if (length(log_lines) > 0) cat(paste(log_lines, collapse = "\n"), "\n", sep = "")

  dados_estacao
}

# ==== Bloco 3 — Funcao de lote: baixa e classifica um conjunto de estacoes ---
# processar_lote_estacoes() e a funcao de entrada deste script: coordena o
# loop por estacao (chamando get_ana_station_series() para cada uma), o
# tryCatch, a classificacao em 3 categorias e as pausas de controle de
# carga. Nao ha orquestracao por UF ou por normal climatologica aqui — isso
# fica a cargo de 03_download_normal_uf.R.
#
# Aceita uma listagem de estacoes arbitraria (pode conter varias UFs
# misturadas) — nao ha agrupamento nem loop por estado. Parametros
# principais: estacoes (data frame com Codigo e DataInicioOperacao),
# start_date/end_date, typedata, paralelizar_meses/workers (repassados a
# get_ana_station_series()), e as pausas de controle de carga
# (pause_between_requests, max_requests, long_pause_seconds). Retorna uma
# lista com 4 data frames: validos (serie diaria completa), sem_dados
# (Codigo + timestamp), falhas (Codigo + timestamp + mensagem_erro) e
# log_processamento (1 linha por estacao processada, para rastreabilidade).
processar_lote_estacoes <- function(
    estacoes, start_date, end_date, typedata,
    paralelizar_meses = FALSE,
    workers = NULL,
    pause_between_requests = 6,
    max_requests = 30,
    long_pause_seconds = 60
) {
  colunas_exigidas <- c("Codigo", "DataInicioOperacao")
  faltantes <- setdiff(colunas_exigidas, names(estacoes))
  if (length(faltantes) > 0) {
    stop("O data.frame 'estacoes' nao contem a(s) coluna(s): ", paste(faltantes, collapse = ", "), call. = FALSE)
  }

  start_date <- lubridate::ymd(start_date)
  end_date   <- lubridate::ymd(end_date)

  # Criterio de elegibilidade: DataInicioOperacao <= end_date (inelegivel
  # apenas se a estacao comecou a operar depois do fim do periodo solicitado).
  elegiveis <- estacoes |>
    dplyr::filter(!is.na(DataInicioOperacao), as.Date(DataInicioOperacao) <= end_date) |>
    dplyr::distinct(Codigo, .keep_all = TRUE)

  n_elegiveis <- nrow(elegiveis)

  cat(sprintf(
    "\nBAIXANDO DADOS (tipo %s) PARA %d ESTACAO(OES) ELEGIVEIS - PERIODO: %s ATE %s\n\n",
    typedata, n_elegiveis, format(start_date), format(end_date)
  ))

  vazio_validos           <- tibble::tibble(Date = as.Date(character()), Value = double(),
                                             Consistence = integer(), Status = integer(), Code = character(),
                                             DataIns = as.POSIXct(character()))
  vazio_sem_dados         <- tibble::tibble(Codigo = character(), timestamp = character())
  vazio_falhas            <- tibble::tibble(Codigo = character(), timestamp = character(), mensagem_erro = character())
  vazio_log_processamento <- tibble::tibble(Codigo = character(), categoria = character(),
                                             timestamp = character(), mensagem_erro = character())

  if (n_elegiveis == 0) {
    cat("Nenhuma estacao elegivel para o periodo solicitado.\n")
    return(list(validos = vazio_validos, sem_dados = vazio_sem_dados,
                falhas = vazio_falhas, log_processamento = vazio_log_processamento))
  }

  validos_list <- list(); nodata_list <- list(); falha_list <- list(); log_list <- list()
  num_requests <- 0L

  # Loop UNICO por estacao (sem agrupamento por UF) — cada estacao roda dentro
  # de tryCatch(), garantindo que uma falha isolada nao interrompe o lote.
  for (i in seq_len(n_elegiveis)) {
    station_id <- elegiveis$Codigo[i]
    cat(sprintf("\n[%d/%d] Baixando dados da estacao %s\n", i, n_elegiveis, station_id))

    start_station <- Sys.time()
    resultado <- tryCatch(
      list(ok = TRUE, valor = get_ana_station_series(station_id, start_date, end_date, typedata, paralelizar_meses, workers)),
      error = function(e) list(ok = FALSE, valor = NULL, mensagem_erro = conditionMessage(e))
    )
    tempo_station <- as.numeric(difftime(Sys.time(), start_station, units = "secs"))
    cat(sprintf("\n Processamento de dados da estacao %s: %.2f segundos\n", station_id, tempo_station))

    timestamp_proc <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    mensagem_erro  <- NA_character_

    required_out_cols <- c("Date", "Value", "Consistence", "Status", "Code", "DataIns")

    if (isTRUE(resultado$ok) && !is.null(resultado$valor) && all(required_out_cols %in% names(resultado$valor))) {
      valores <- resultado$valor
      if (nrow(valores) == 0 || all(is.na(valores$Value))) {
        categoria <- "sem_dados"
        nodata_list[[length(nodata_list) + 1L]] <- tibble::tibble(Codigo = station_id, timestamp = timestamp_proc)
      } else {
        categoria <- "valido"
        validos_list[[length(validos_list) + 1L]] <- valores
      }
    } else {
      categoria <- "falha"
      mensagem_erro <- if (!is.null(resultado$mensagem_erro)) resultado$mensagem_erro else "Retorno invalido/incompleto da funcao de download."
      cat(sprintf("Erro ao processar estacao %s: %s\n", station_id, mensagem_erro))
      falha_list[[length(falha_list) + 1L]] <- tibble::tibble(Codigo = station_id, timestamp = timestamp_proc, mensagem_erro = mensagem_erro)
    }

    log_list[[length(log_list) + 1L]] <- tibble::tibble(
      Codigo = station_id, categoria = categoria, timestamp = timestamp_proc, mensagem_erro = mensagem_erro
    )

    num_requests <- num_requests + 1L
    if (num_requests %% max_requests == 0L) {
      cat("\n || Pausa automatica para evitar sobrecarga no servidor...\n")
      Sys.sleep(long_pause_seconds)
    }

    Sys.sleep(pause_between_requests)
  }

  validos           <- if (length(validos_list) > 0) dplyr::bind_rows(validos_list) else vazio_validos
  sem_dados         <- if (length(nodata_list) > 0) dplyr::bind_rows(nodata_list) else vazio_sem_dados
  falhas            <- if (length(falha_list) > 0) dplyr::bind_rows(falha_list) else vazio_falhas
  log_processamento <- dplyr::bind_rows(log_list)

  # Resumo de execucao (mensagem de console)
  cat("\n==============================================================\n")
  cat("Download finalizado. Resumo da execucao:\n")
  cat(sprintf("- Estacoes elegiveis no periodo:  %d\n", n_elegiveis))
  cat(sprintf("- Processadas com sucesso:        %d\n", sum(log_processamento$categoria == "valido")))
  cat(sprintf("- Sem dados no periodo:           %d\n", sum(log_processamento$categoria == "sem_dados")))
  cat(sprintf("- Falha tecnica:                  %d\n", sum(log_processamento$categoria == "falha")))
  cat("==============================================================\n")

  list(validos = validos, sem_dados = sem_dados, falhas = falhas, log_processamento = log_processamento)
}

# =============================================================================
# Exemplo de uso:
# =============================================================================
#
# # 1) Carregar a listagem de estacoes gerada por 01_get_stations_inventory.R
# #    (entrada obrigatoria deste script):
#
# estacoes_chuva <- carregar_listagem_estacoes("dados/estacoes/HidroInventario_precipitation_stations_ANA.csv")
#
# # 2) Baixar as series diarias de chuva para um periodo arbitrario, para TODAS
# #    as estacoes elegiveis da listagem (podem ser de qualquer UF, misturadas):
#
# resultado <- processar_lote_estacoes(
#   estacoes  = estacoes_chuva,
#   start_date = "2015-01-01",
#   end_date   = "2020-12-31",
#   typedata   = "2"   # 1 = cota | 2 = chuva | 3 = vazao
# )
#
# # 3) Inspecionar e gravar os resultados (a cargo do usuario — nenhum arquivo
# #    e escrito automaticamente pela funcao de lote). CSVs sao escritos com
# #    write_excel_csv() (BOM UTF-8) para exibicao correta de acentos no Excel:
#
# nrow(resultado$validos)
# nrow(resultado$sem_dados)
# nrow(resultado$falhas)
# resultado$log_processamento
#
# dir.create("dados/series", recursive = TRUE, showWarnings = FALSE)
# arrow::write_parquet(resultado$validos, "dados/series/chuva_validos.parquet", compression = "gzip")
# readr::write_excel_csv(resultado$sem_dados, "dados/series/chuva_sem_dados.csv")
# readr::write_excel_csv(resultado$falhas, "dados/series/chuva_falhas.csv")
#
# # 4) Exemplo baixando series de UMA unica estacao diretamente (sem passar
# #    pela funcao de lote), util para testes/depuracao:
#
# serie_uma_estacao <- get_ana_station_series(
#   station = estacoes_chuva$Codigo[1],
#   start_date = "2015-01-01",
#   end_date   = "2020-12-31",
#   typedata   = "2"
# )
