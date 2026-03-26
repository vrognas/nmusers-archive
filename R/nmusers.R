# Helper functions for working with the NMusers archive in R
# ---
# Reads the Parquet file produced by the Python parser.
# Provides convenience functions for common queries.
#
# Usage:
#   source("R/nmusers.R")
#   messages <- nmusers_read()
#   technical <- nmusers_technical(messages)

#' Read the NMusers archive from Parquet
#'
#' @param path Character. Path to the Parquet file.
#'   Defaults to the merged dataset; falls back to mail-archive-only.
#' @return A tibble with all parsed messages.
nmusers_read <- function(path = NULL) {
  if (is.null(path)) {
    candidates <- c("data/messages_all.parquet", "data/messages.parquet")
    path <- candidates[file.exists(candidates)][1]
  }

  if (is.na(path) || !file.exists(path)) {
    cli::cli_abort(c(
      "No Parquet file found.",
      "i" = "Run the Python pipeline first:",
      " " = "python python/scrape.py && python python/parse.py",
      " " = "python python/merge.py"
    ))
  }

  arrow::read_parquet(path) |>
    dplyr::as_tibble()
}

#' Filter to technical Q&A messages only
#'
#' Removes job ads, workshop announcements, and other noise.
#' Useful for building a RAG knowledge base.
#'
#' @param messages Tibble. Output of nmusers_read().
#' @return Filtered tibble.
nmusers_technical <- function(messages) {
  messages |>
    dplyr::filter(category == "technical")
}

#' Get all messages in a specific thread
#'
#' @param messages Tibble. Output of nmusers_read().
#' @param thread_root Integer. The message_number of the thread root.
#' @return Tibble of messages in chronological order.
nmusers_thread <- function(messages, thread_root) {
  messages |>
    dplyr::filter(thread_id == thread_root) |>
    dplyr::arrange(date)
}

#' Summarise threads by message count and date range
#'
#' @param messages Tibble. Output of nmusers_read().
#' @return Tibble with one row per thread.
nmusers_thread_summary <- function(messages) {
  messages |>
    dplyr::group_by(thread_id) |>
    dplyr::summarise(
      subject       = dplyr::first(subject),
      category      = dplyr::first(category),
      message_count = dplyr::n(),
      started       = min(date, na.rm = TRUE),
      last_reply    = max(date, na.rm = TRUE),
      contributors  = dplyr::n_distinct(from_name),
      .groups       = "drop"
    ) |>
    dplyr::arrange(dplyr::desc(message_count))
}

#' Top contributors by message count
#'
#' @param messages Tibble. Output of nmusers_read().
#' @param top_n Integer. How many contributors to return.
#' @return Tibble sorted by message count.
nmusers_top_contributors <- function(messages, top_n = 20) {
  messages |>
    dplyr::count(from_name, sort = TRUE) |>
    dplyr::slice_head(n = top_n)
}

#' Monthly message volume over time
#'
#' @param messages Tibble. Output of nmusers_read().
#' @return Tibble with year_month and count columns.
nmusers_monthly_volume <- function(messages) {
  messages |>
    dplyr::filter(!is.na(date)) |>
    dplyr::mutate(year_month = format(date, "%Y-%m")) |>
    dplyr::count(year_month, category) |>
    dplyr::arrange(year_month)
}
