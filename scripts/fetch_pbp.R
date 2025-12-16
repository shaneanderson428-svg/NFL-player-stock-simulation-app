#!/usr/bin/env Rscript
# nolint start
## Fetch play-by-play data using nflfastR and write per-season files to data/pbp/
## Usage:
##   Rscript scripts/fetch_pbp.R 2023 2024
## or (no args -> use current year and previous year):
##   Rscript scripts/fetch_pbp.R

args <- commandArgs(trailingOnly = TRUE)
current_year <- as.integer(format(Sys.Date(), "%Y"))
if (length(args) == 0) {
  years <- (current_year - 1):current_year
} else {
  years <- as.integer(args)
}
if (any(is.na(years))) stop("Years must be integers, e.g. 2023 2024")

message("Will fetch seasons: ", paste(years, collapse = ", "))

if (!requireNamespace("nflfastR", quietly = TRUE)) {
  message(
    "nflfastR not found — installing via remotes::install_github('nflverse/nflfastR')"
  )
  if (!requireNamespace("remotes", quietly = TRUE)) {
    install.packages("remotes", repos = "https://cran.rstudio.com")
  }
  remotes::install_github("nflverse/nflfastR")
}
library(nflfastR)

## load play-by-play for the requested seasons (this may take a while)
pbp <- tryCatch(
  nflfastR::load_pbp(years),
  error = function(e) stop("Failed to load pbp: ", e$message)
)

dir.create("data/pbp", recursive = TRUE, showWarnings = FALSE)

use_arrow <- requireNamespace("arrow", quietly = TRUE)
if (use_arrow) {
  message("arrow available — writing Parquet files per season to data/pbp/")
  for (yr in sort(unique(pbp$season))) {
    subset <- pbp[pbp$season == yr, , drop = FALSE]
    out <- file.path("data/pbp", sprintf("pbp_%d.parquet", yr))
    message("Writing: ", out)
    arrow::write_parquet(subset, out)
  }
} else {
  message("arrow not installed — writing gzipped CSV files per season to data/pbp/")
  for (yr in sort(unique(pbp$season))) {
    subset <- pbp[pbp$season == yr, , drop = FALSE]
    out <- file.path("data/pbp", sprintf("pbp_%d.csv.gz", yr))
    message("Writing: ", out)
    con <- gzfile(out, "w")
    utils::write.csv(subset, con, row.names = FALSE)
    close(con)
  }
}

# nolint end
message("Done. Files written to data/pbp/")
#!/usr/bin/env Rscript
# nolint start
## Fetch play-by-play data using nflfastR and write per-season files to data/pbp/
## Usage:
##   Rscript scripts/fetch_pbp.R 2023 2024
## or (no args -> use current year and previous year):
##   Rscript scripts/fetch_pbp.R

args <- commandArgs(trailingOnly = TRUE)
current_year <- as.integer(format(Sys.Date(), "%Y"))
if (length(args) == 0) {
  years <- (current_year - 1):current_year
} else {
  years <- as.integer(args)
}
if (any(is.na(years))) stop("Years must be integers, e.g. 2023 2024")

message("Will fetch seasons: ", paste(years, collapse = ", "))

if (!requireNamespace("nflfastR", quietly = TRUE)) {
  message(
    "nflfastR not found — installing via remotes::install_github('nflverse/nflfastR')"
  )
  if (!requireNamespace("remotes", quietly = TRUE)) {
    install.packages("remotes", repos = "https://cran.rstudio.com")
  }
  remotes::install_github("nflverse/nflfastR")
}
library(nflfastR)

## load play-by-play for the requested seasons (this may take a while)
pbp <- tryCatch(
  nflfastR::load_pbp(years),
  error = function(e) stop("Failed to load pbp: ", e$message)
)

dir.create("data/pbp", recursive = TRUE, showWarnings = FALSE)

use_arrow <- requireNamespace("arrow", quietly = TRUE)
if (use_arrow) {
  message("arrow available — writing Parquet files per season to data/pbp/")
  for (yr in sort(unique(pbp$season))) {
    subset <- pbp[pbp$season == yr, , drop = FALSE]
    out <- file.path("data/pbp", sprintf("pbp_%d.parquet", yr))
    message("Writing: ", out)
    arrow::write_parquet(subset, out)
  }
} else {
  message("arrow not installed — writing gzipped CSV files per season to data/pbp/")
  for (yr in sort(unique(pbp$season))) {
    subset <- pbp[pbp$season == yr, , drop = FALSE]
    out <- file.path("data/pbp", sprintf("pbp_%d.csv.gz", yr))
    message("Writing: ", out)
    con <- gzfile(out, "w")
    utils::write.csv(subset, con, row.names = FALSE)
    close(con)
  }
}

# nolint end
message("Done. Files written to data/pbp/")
