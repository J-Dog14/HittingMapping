# Install required packages for Pelvis Mapping.R
packages <- c(
  "xml2",
  "purrr",
  "dplyr",
  "readr",
  "stringr",
  "tibble",
  "tidyr",
  "DBI",
  "RSQLite",
  "uuid"
)

# Check which packages are already installed
installed <- installed.packages()[,"Package"]
to_install <- packages[!packages %in% installed]

if (length(to_install) > 0) {
  cat("Installing packages:", paste(to_install, collapse = ", "), "\n")
  install.packages(to_install, repos = "https://cran.rstudio.com/", dependencies = TRUE)
  cat("Installation complete!\n")
} else {
  cat("All packages are already installed.\n")
}

# Verify installation
cat("\nVerifying package installation:\n")
for (pkg in packages) {
  if (requireNamespace(pkg, quietly = TRUE)) {
    cat("✓", pkg, "\n")
  } else {
    cat("✗", pkg, "- FAILED\n")
  }
}

