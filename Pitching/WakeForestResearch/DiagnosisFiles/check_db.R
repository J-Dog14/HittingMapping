# Check database structure
library(DBI)
library(RSQLite)

con <- dbConnect(RSQLite::SQLite(), "hitting_data.db")

cat("=== Database Tables ===\n")
tables <- dbListTables(con)
cat("Tables:", paste(tables, collapse = ", "), "\n\n")

for (tbl in tables) {
  cat("=== Table:", tbl, "===\n")
  
  # Get column names
  cols <- dbGetQuery(con, paste("PRAGMA table_info(", tbl, ")"))
  cat("Columns (", nrow(cols), "):\n", sep = "")
  print(cols$name)
  cat("\n")
  
  # Get row count
  row_count <- dbGetQuery(con, paste("SELECT COUNT(*) as n FROM", tbl))$n
  cat("Row count:", row_count, "\n\n")
  
  # Show first few rows
  if (row_count > 0) {
    cat("First 2 rows:\n")
    sample <- dbGetQuery(con, paste("SELECT * FROM", tbl, "LIMIT 2"))
    print(sample)
    cat("\n")
    
    # For time series tables, show column structure
    if (tbl %in% c("link_model_based", "metric", "event_label")) {
      cat("Sample of value columns:\n")
      value_cols <- grep("^value_", names(sample), value = TRUE)
      if (length(value_cols) > 0) {
        cat("Found", length(value_cols), "value columns\n")
        cat("First 5 value columns:", paste(head(value_cols, 5), collapse = ", "), "\n")
        # Show first row's first few values
        if (nrow(sample) > 0) {
          cat("First row, first 5 values:", paste(sample[1, head(value_cols, 5)], collapse = ", "), "\n")
        }
      }
      cat("\n")
    }
  }
  
  cat("---\n\n")
}

dbDisconnect(con)

