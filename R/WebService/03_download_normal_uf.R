# ana-hydro-data: pipeline reprodutível para aquisição de séries históricas hidrometeorológicas da ANA
# Copyright (C) 2026 Marcos Aurélio Eustorgio Filho
# Licensed under the GNU GPLv3
# =============================================================================
# ana-hydro-data — 03_download_normal_uf.R
# -----------------------------------------------------------------------------
# Objetivo:
#   Baixar series historicas diarias (cota, chuva ou vazao) particionando o
#   trabalho por NORMAL CLIMATOLOGICA (1961-1990 / 1991-2020 / 2021-2024) e
#   por UNIDADE FEDERATIVA (UF), organizando a saida em uma estrutura de
#   diretorios por categoria (long_data / nodata_gauges / problem_gauges) e
#   produzindo um resumo consolidado de execucao (resumo_download, em CSV +
#   Parquet).
#
#   Este script nao reimplementa a logica de download por estacao: ele
#   IMPORTA, via source(), apenas as funcoes `get_ana_station_series()` e
#   `processar_lote_estacoes()` do arquivo "02_get_daily_series.R" (ver
#   Bloco 0). A importacao acontece automaticamente ao carregar este script
#   — nao e necessario nenhum passo manual antes.
#
# Autor:   Marcos Aurelio Eustorgio Filho
# Contexto: script desenvolvido no ambito do Trabalho de Conclusao de Curso
#           "Automacao e Curadoria de Series Historicas Hidrometeorologicas
#           da ANA via Pipeline Computacional em Linguagem R", MBA em Data
#           Science e Analytics, USP/Esalq.
# Criado:  04/09/2026
#
# Codificacao: este arquivo e todos os dados que ele gera usam UTF-8. CSVs
#   sao escritos com readr::write_excel_csv() (inclui BOM UTF-8), para exibir
#   corretamente nomes com acentuacao em aplicativos como o Microsoft Excel.
#
# Observacoes gerais:
#   O criterio de elegibilidade aqui e mais estrito que o de
#   processar_lote_estacoes(): exige que a estacao ja estivesse operando ha
#   pelo menos ~1 ano antes do fim da normal climatologica (`corte_final <-
#   end_date %m-% years(1) + days(1)`), pois a normal e uma janela fixa de 30
#   anos (ou 4, para a normal 3) — os dois criterios coexistem porque
#   atendem a objetivos diferentes. O loop por UF cruza a listagem de
#   estacoes com uma pequena tabela de referencia UF <-> sigla embutida no
#   script (Bloco 1, dado publico); as UFs processadas sao derivadas dos
#   proprios dados, nao de uma lista fixa. A saida segue
#   `<dir_saida>/<subpasta_por_tipo>/periodo_<N>_<ano1>_<ano2>/` com
#   subpastas `long_data`, `problem_gauges` e `nodata_gauges`, mais o resumo
#   consolidado `resumo_download_<tipo>_per_<N>_<data>.parquet`/`.csv` —
#   `dir_saida` e sempre um parametro obrigatorio. Uma UF sem estacao
#   elegivel e pulada (sem arquivo, sem linha no resumo); uma UF com
#   estacoes elegiveis mas nenhum dado valido nao grava `long_data` e pula a
#   pausa entre UFs. O numero de workers e fixo em 12 por padrao, mas aceita
#   `workers = "auto"` (`nucleos_disponiveis - 2`) ou qualquer inteiro >= 1.
#   `get_ana_station_series()` e `processar_lote_estacoes()` sao chamadas
#   deste script uma vez por UF, nunca reescritas aqui.
#
# Pacotes utilizados (obrigatorios): dplyr, tibble, lubridate, arrow, readr,
#   parallel (base R) — mais os pacotes exigidos por 02_get_daily_series.R
#   (importado automaticamente, ver Bloco 0).
# Pacotes exigidos por padrao (paralelizar_meses = TRUE neste script):
#   future, doFuture, foreach.
# =============================================================================

# ==== Bloco 0 — Pacotes e importacao das funcoes de download por estacao ----
suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(lubridate)
  library(arrow)
  library(readr)
})

# Importa, via source(), APENAS as duas funcoes de que este script precisa
# (get_ana_station_series() e processar_lote_estacoes()) a partir do arquivo
# vizinho "02_get_daily_series.R" — nenhuma copia do codigo dessas funcoes
# existe aqui. A importacao roda dentro de um ambiente proprio, descartado
# em seguida, para nao trazer nenhum outro objeto daquele arquivo para o
# ambiente global. So e executada se as funcoes ainda nao estiverem
# disponiveis (ex.: o usuario ja fez `source("02_get_daily_series.R")`
# manualmente nao ha novo trabalho a fazer).
if (!exists("processar_lote_estacoes", mode = "function") ||
    !exists("get_ana_station_series", mode = "function")) {
  local({
    caminho <- Sys.getenv("ANA_HYDRO_DATA_SCRIPT_DOWNLOAD", unset = "")
    if (!nzchar(caminho)) {
      diretorio <- tryCatch(dirname(sys.frame(1)$ofile), error = function(e) NA_character_)
      if (is.na(diretorio) || !nzchar(diretorio)) diretorio <- "R/WebService"
      caminho <- file.path(diretorio, "02_get_daily_series.R")
    }
    if (!file.exists(caminho)) {
      stop(
        "Nao foi possivel localizar '02_get_daily_series.R' em '", caminho, "'. ",
        "Se a estrutura de diretorios do seu projeto for diferente, informe o ",
        "caminho manualmente antes de carregar este script, por exemplo:\n",
        "  Sys.setenv(ANA_HYDRO_DATA_SCRIPT_DOWNLOAD = \"caminho/para/02_get_daily_series.R\")",
        call. = FALSE
      )
    }

    ambiente_temporario <- new.env()
    source(caminho, local = ambiente_temporario)

    funcoes_necessarias <- c("get_ana_station_series", "processar_lote_estacoes")
    faltantes <- setdiff(funcoes_necessarias, ls(ambiente_temporario))
    if (length(faltantes) > 0) {
      stop("O arquivo '", caminho, "' nao define a(s) funcao(oes) esperada(s): ",
           paste(faltantes, collapse = ", "), call. = FALSE)
    }

    for (nome_funcao in funcoes_necessarias) {
      assign(nome_funcao, get(nome_funcao, envir = ambiente_temporario), envir = globalenv())
    }
  })
}

# ==== Bloco 1 — Tabela de referencia UF <-> sigla (dado publico, embutido) ---
# `nome_estado` esta em CAIXA ALTA com acentuacao, no mesmo formato em que a
# ANA devolve o campo `nmEstado` no cadastro de estacoes (ex.: "AMAPA" e
# devolvido como "AMAPÁ", "SAO PAULO" como "SÃO PAULO", etc.) — preservar a
# acentuacao aqui e o que garante o cruzamento correto com `nmEstado`.
UF_SIGLAS <- tibble::tribble(
  ~nome_estado,           ~sigla, ~regiao,
  "ACRE",                 "AC",   "Regiao Norte",
  "ALAGOAS",              "AL",   "Regiao Nordeste",
  "AMAPÁ",                "AP",   "Regiao Norte",
  "AMAZONAS",             "AM",   "Regiao Norte",
  "BAHIA",                "BA",   "Regiao Nordeste",
  "CEARÁ",                "CE",   "Regiao Nordeste",
  "DISTRITO FEDERAL",     "DF",   "Regiao Centro-Oeste",
  "ESPÍRITO SANTO",       "ES",   "Regiao Sudeste",
  "GOIÁS",                "GO",   "Regiao Centro-Oeste",
  "MARANHÃO",             "MA",   "Regiao Nordeste",
  "MATO GROSSO",          "MT",   "Regiao Centro-Oeste",
  "MATO GROSSO DO SUL",   "MS",   "Regiao Centro-Oeste",
  "MINAS GERAIS",         "MG",   "Regiao Sudeste",
  "PARÁ",                 "PA",   "Regiao Norte",
  "PARAÍBA",              "PB",   "Regiao Nordeste",
  "PARANÁ",               "PR",   "Regiao Sul",
  "PERNAMBUCO",           "PE",   "Regiao Nordeste",
  "PIAUÍ",                "PI",   "Regiao Nordeste",
  "RIO DE JANEIRO",       "RJ",   "Regiao Sudeste",
  "RIO GRANDE DO NORTE",  "RN",   "Regiao Nordeste",
  "RIO GRANDE DO SUL",    "RS",   "Regiao Sul",
  "RONDÔNIA",             "RO",   "Regiao Norte",
  "RORAIMA",              "RR",   "Regiao Norte",
  "SANTA CATARINA",       "SC",   "Regiao Sul",
  "SÃO PAULO",            "SP",   "Regiao Sudeste",
  "SERGIPE",              "SE",   "Regiao Nordeste",
  "TOCANTINS",            "TO",   "Regiao Norte"
)

# Padroniza um nome de estado para cruzamento com UF_SIGLAS. Aplica apenas
# toupper() — o campo nmEstado devolvido pela ANA ja vem em caixa alta com
# acentuacao, mesmo formato de UF_SIGLAS$nome_estado.
padronizar_nome_estado <- function(x) {
  toupper(x)
}

# ==== Bloco 2 — Janelas fixas das normais climatologicas -------------------
# Periodos conforme definicao do INMET:
#   1: 1961-1990 | 2: 1991-2020 (principal) | 3: 2021-2024 (adicional)
# Retorna uma lista com start_date e end_date (objetos Date).
janela_normal_climatologica <- function(climate_normal) {
  if (climate_normal == 1) {
    list(start_date = as.Date("1961-01-01"), end_date = as.Date("1990-12-31"))
  } else if (climate_normal == 2) {
    list(start_date = as.Date("1991-01-01"), end_date = as.Date("2020-12-31"))
  } else if (climate_normal == 3) {
    list(start_date = as.Date("2021-01-01"), end_date = as.Date("2024-12-31"))
  } else {
    stop("Valor invalido para 'climate_normal': use 1 (1961-1990), 2 (1991-2020) ou 3 (2021-2024).", call. = FALSE)
  }
}

# ==== Bloco 3 — Resolucao do numero de workers ------------------------------
# Aceita um inteiro >= 1 fixo, ou a string "auto" (nucleos_disponiveis - 2,
# deixando ao menos 2 nucleos livres para o sistema operacional).
resolver_workers <- function(workers) {
  if (identical(workers, "auto")) {
    return(max(1L, parallel::detectCores() - 2L))
  }
  if (!is.numeric(workers) || length(workers) != 1 || workers < 1) {
    stop("'workers' deve ser um numero inteiro >= 1, ou a string 'auto' (nucleos_disponiveis - 2).", call. = FALSE)
  }
  as.integer(workers)
}

# ==== Bloco 4 — Funcao orquestradora deste script (por UF e por normal) -----
# baixar_por_normal_uf() e a funcao de entrada deste script: coordena o loop
# por UF e a janela fixa da normal climatologica, delegando o trabalho por
# estacao a processar_lote_estacoes() uma vez por UF, e monta/grava a
# estrutura de diretorios e o resumo consolidado. Parametros principais:
# estacoes (data frame com Codigo, DataInicioOperacao e nmEstado),
# climate_normal (1, 2 ou 3), typedata ("1" cota | "2" chuva | "3" vazao —
# "1" e "3" usam o mesmo inventario tipo 1, fluviometrico), dir_saida
# (obrigatorio), workers (default 12, ou "auto"), paralelizar_meses (default
# TRUE) e as pausas de controle de carga. Retorna um tibble consolidado (uma
# linha por UF processada), com as colunas Estado, Total_Elegiveis, Sucesso,
# Sem_Dados, Falha, Tempo_min, Arquivo_Dados, Arquivo_Erros,
# Arquivo_Sem_Dados — o mesmo resultado tambem e gravado em disco (CSV +
# Parquet).
baixar_por_normal_uf <- function(
    estacoes,
    climate_normal,
    typedata,
    dir_saida,
    workers = 12,
    paralelizar_meses = TRUE,
    pause_between_requests = 6,
    max_requests = 30,
    long_pause_seconds = 60,
    pause_between_states = 120
) {
  colunas_exigidas <- c("Codigo", "DataInicioOperacao", "nmEstado")
  faltantes <- setdiff(colunas_exigidas, names(estacoes))
  if (length(faltantes) > 0) {
    stop("O data.frame 'estacoes' nao contem a(s) coluna(s): ", paste(faltantes, collapse = ", "), call. = FALSE)
  }
  if (missing(dir_saida) || is.null(dir_saida) || !nzchar(dir_saida)) {
    stop("Parametro 'dir_saida' e obrigatorio: informe o diretorio raiz onde os resultados serao gravados.", call. = FALSE)
  }

  tipo_download <- switch(typedata, "1" = "cotas", "2" = "chuva", "3" = "vazao",
                           stop("typedata invalido: use '1' (cota), '2' (chuva) ou '3' (vazao)", call. = FALSE))
  tipo_nome     <- switch(typedata, "1" = "level_data", "2" = "prec_data", "3" = "flow_data")
  subpasta_tipo <- switch(typedata, "1" = "river_level_data", "2" = "precipitation_data", "3" = "river_discharge_data")
  out_dir <- file.path(dir_saida, subpasta_tipo)

  janela    <- janela_normal_climatologica(climate_normal)
  str_date  <- janela$start_date
  end_date  <- janela$end_date
  # Criterio de elegibilidade (ver "Observacoes gerais" no cabecalho do arquivo)
  corte_final <- end_date %m-% lubridate::years(1) + lubridate::days(1)

  workers_resolvidos <- resolver_workers(workers)

  cat(sprintf(
    "BAIXANDO DADOS DE %s PARA ESTACOES DA ANA - NORMAL CLIMATOLOGICA: %d - PERIODO: %s ATE %s\n",
    toupper(tipo_download), climate_normal, str_date, end_date
  ))
  cat(sprintf("Workers para paralelizacao entre meses: %d\n", workers_resolvidos))

  # Cruza a listagem com a tabela UF <-> sigla embutida (Bloco 1)
  estacoes_uf <- estacoes |>
    dplyr::mutate(.nmEstado_padr = padronizar_nome_estado(nmEstado)) |>
    dplyr::left_join(UF_SIGLAS, by = c(".nmEstado_padr" = "nome_estado")) |>
    dplyr::rename(UF = sigla) |>
    dplyr::select(-.nmEstado_padr)

  estados <- sort(unique(estacoes_uf$UF[!is.na(estacoes_uf$UF)]))
  if (length(estados) == 0) {
    stop("Nenhuma UF reconhecida na listagem de estacoes (verifique a coluna 'nmEstado').", call. = FALSE)
  }

  resumo_execucao <- list()

  for (estado in estados) {
    start_time_estado <- Sys.time()

    # Numero total de estacoes cadastradas na UF (sem filtro de elegibilidade)
    num_linhas_uf <- estacoes_uf |> dplyr::filter(UF == estado) |> nrow()

    # Filtra estacoes da UF elegiveis para esta normal
    df_uf <- estacoes_uf |>
      dplyr::filter(UF == estado) |>
      dplyr::filter(!is.na(DataInicioOperacao), as.Date(DataInicioOperacao) <= corte_final)

    if (nrow(df_uf) == 0) {
      cat(sprintf(
        "Nenhuma estacao qualificada para o estado %s (corte_final = %s) - pulando.\n",
        estado, format(corte_final)
      ))
      next
    }

    cat("\n================================================================================\n")
    cat(sprintf("ESTADO: %s - TOTAL DE ESTACOES CADASTRADAS: %d\n", estado, num_linhas_uf))
    cat(sprintf("TOTAL DE ESTACOES COM POSSIBILIDADE DE DADOS NO PERIODO: %d\n", nrow(df_uf)))
    cat("================================================================================\n\n")

    # Delega o trabalho por estacao a processar_lote_estacoes()
    resultado_uf <- processar_lote_estacoes(
      estacoes               = df_uf,
      start_date              = str_date,
      end_date                = end_date,
      typedata                = typedata,
      paralelizar_meses       = paralelizar_meses,
      workers                 = workers_resolvidos,
      pause_between_requests  = pause_between_requests,
      max_requests            = max_requests,
      long_pause_seconds      = long_pause_seconds
    )

    total_elegiveis <- nrow(df_uf)
    total_ok       <- sum(resultado_uf$log_processamento$categoria == "valido")
    total_nodata   <- sum(resultado_uf$log_processamento$categoria == "sem_dados")
    total_fail     <- sum(resultado_uf$log_processamento$categoria == "falha")

    nome_pasta <- paste0("periodo_", climate_normal, "_",
                          lubridate::year(str_date), "_", lubridate::year(end_date))
    dir_long   <- file.path(out_dir, nome_pasta, "long_data")
    dir_error  <- file.path(out_dir, nome_pasta, "problem_gauges")
    dir_nodata <- file.path(out_dir, nome_pasta, "nodata_gauges")

    file_long   <- file.path(dir_long,   paste0(tipo_nome, "_long_", estado, "_ANA.parquet"))
    file_error  <- file.path(dir_error,  paste0("error_nondt_", estado, "_ANA.parquet"))
    file_nodata <- file.path(dir_nodata, paste0("nodata_", estado, "_ANA.parquet"))

    dir.create(dir_long,   recursive = TRUE, showWarnings = FALSE)
    dir.create(dir_error,  recursive = TRUE, showWarnings = FALSE)
    dir.create(dir_nodata, recursive = TRUE, showWarnings = FALSE)

    if (total_fail > 0) {
      erros_uf <- resultado_uf$falhas |> dplyr::rename(station = Codigo) |> dplyr::distinct(station)
      arrow::write_parquet(erros_uf, file_error, compression = "gzip")
    } else {
      cat(sprintf("Sem falhas no download de estacoes para estado %s.\n", estado))
    }

    if (total_nodata > 0) {
      nodata_uf <- resultado_uf$sem_dados |> dplyr::rename(station = Codigo) |> dplyr::distinct(station)
      arrow::write_parquet(nodata_uf, file_nodata, compression = "gzip")
    } else {
      cat(sprintf("Todas as estacoes do estado %s possuem dados.\n", estado))
    }

    tempo_total <- as.numeric(difftime(Sys.time(), start_time_estado, units = "mins"))
    resumo_execucao[[length(resumo_execucao) + 1L]] <- tibble::tibble(
      Estado            = estado,
      Total_Elegiveis   = total_elegiveis,
      Sucesso           = total_ok,
      Sem_Dados         = total_nodata,
      Falha             = total_fail,
      Tempo_min         = round(tempo_total, 2),
      Arquivo_Dados     = if (total_ok     > 0) file_long   else NA_character_,
      Arquivo_Erros     = if (total_fail   > 0) file_error  else NA_character_,
      Arquivo_Sem_Dados = if (total_nodata > 0) file_nodata else NA_character_
    )

    # UF com zero sucessos: nao escreve long_data e pula a pausa entre UFs
    if (total_ok == 0) {
      cat(sprintf("Nenhum dado valido obtido para o estado %s - pulando escrita.\n", estado))
      next
    }

    arrow::write_parquet(resultado_uf$validos, file_long, compression = "gzip")

    cat("\n====================================================================================\n")
    cat(toupper(paste("Estado", estado, "concluido. Dados armazenados em:", file_long, "\n")))
    cat(toupper(paste("Preparando proxima execucao em", pause_between_states, "segundos...\n")))
    cat("====================================================================================\n\n")

    Sys.sleep(pause_between_states)
  }

  if (length(resumo_execucao) == 0) {
    cat("\nNenhuma UF produziu resultados (nenhuma estacao elegivel em nenhuma UF).\n")
    return(tibble::tibble())
  }

  resumo_execucao_df <- dplyr::bind_rows(resumo_execucao)

  cat("\n==============================================================\n")
  cat("Download finalizado.\n")
  cat("Resumo da execucao por estado:\n")
  print(resumo_execucao_df)
  cat("==============================================================\n")

  nome_resumo <- function(tipo, normal, ext) {
    paste0("resumo_download_", tipo, "_per_", normal, "_", Sys.Date(), ".", ext)
  }
  file_summary_parquet <- file.path(out_dir, nome_resumo(tipo_download, climate_normal, "parquet"))
  file_summary_csv     <- file.path(out_dir, nome_resumo(tipo_download, climate_normal, "csv"))

  arrow::write_parquet(resumo_execucao_df, file_summary_parquet, compression = "gzip")
  readr::write_excel_csv(resumo_execucao_df, file_summary_csv)

  cat(sprintf("\nResumo salvo em:\n- %s\n- %s\n", file_summary_csv, file_summary_parquet))

  resumo_execucao_df
}

# =============================================================================
# Exemplo de uso:
# =============================================================================
#
# # 0) Basta carregar este script — as funcoes de download por estacao sao
# #    importadas automaticamente do arquivo vizinho "02_get_daily_series.R"
# #    (Bloco 0). Sourcear o script 02 manualmente antes e opcional; faca
# #    isso apenas se tambem quiser usar carregar_listagem_estacoes() para
# #    ler o arquivo de estacoes no passo 1 abaixo:
# source("R/WebService/02_get_daily_series.R")
# source("R/WebService/03_download_normal_uf.R")
#
# # 1) Carregar a listagem de estacoes (saida de 01_get_stations_inventory.R).
# #    Para typedata "1" (cota) ou "3" (vazao), use o inventario tipo 1
# #    (fluviometrica); para typedata "2" (chuva), use o inventario tipo 2:
#
# estacoes_chuva <- carregar_listagem_estacoes("dados/estacoes/HidroInventario_precipitation_stations_ANA.csv")
#
# # 2) Baixar os dados de chuva da normal climatologica 2 (1991-2020), por UF,
# #    com 12 workers fixos (padrao):
#
# resumo <- baixar_por_normal_uf(
#   estacoes       = estacoes_chuva,
#   climate_normal = 2,
#   typedata       = "2",
#   dir_saida      = "dados/normais_climatologicas"
# )
#
# # 3) Alternativa com menos workers (ex.: maquina compartilhada, deixando
# #    2 nucleos livres para o sistema operacional):
#
# resumo <- baixar_por_normal_uf(
#   estacoes       = estacoes_chuva,
#   climate_normal = 2,
#   typedata       = "2",
#   dir_saida      = "dados/normais_climatologicas",
#   workers        = "auto"
# )
#
# # O resultado (arquivos por UF + resumo_download consolidado) e gravado em
# # `dir_saida`, informado pelo usuario — nunca em um caminho fixo.
