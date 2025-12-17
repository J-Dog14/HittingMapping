# ==== Minimal deps ====
library(xml2)
library(purrr)
library(dplyr)
library(readr)
library(stringr)
library(tibble)
library(tidyr)
library(DBI)
library(RSQLite)
library(uuid)
library(tools)

# ---------- Configuration ----------
# Set to NULL to use current directory, or specify path
DATA_ROOT <- NULL  # Set to your pitching data directory path, NULL for testing with local files
DB_FILE <- "pitching_data.db"

# ---------- Helpers ----------
`%||%` <- function(a, b) if (!is.null(a)) a else b
nzchr <- function(x) ifelse(is.na(x) | x == "", NA_character_, x)
nznum <- function(x) suppressWarnings(readr::parse_number(x))
read_xml_robust <- function(path) {
  tryCatch(
    {
      if (grepl("\\.gz$", path, ignore.case = TRUE)) {
        txt <- paste(readLines(gzfile(path), warn = FALSE), collapse = "\n")
        read_xml(txt)
      } else {
        read_xml(path)
      }
    },
    error = function(e) {
      txt <- if (grepl("\\.gz$", path, ignore.case = TRUE)) {
        paste(readLines(gzfile(path), warn = FALSE), collapse = "\n")
      } else {
        readr::read_file(path)
      }
      end_tag <- "</Subject>"
      m <- regexpr(end_tag, txt, fixed = TRUE)
      if (m > 0) {
        trimmed <- substr(txt, 1, m + nchar(end_tag) - 1)
        read_xml(trimmed)
      } else {
        # Try v3d root
        end_tag <- "</v3d>"
        m <- regexpr(end_tag, txt, fixed = TRUE)
        if (m > 0) {
          trimmed <- substr(txt, 1, m + nchar(end_tag) - 1)
          read_xml(trimmed)
        } else stop(e)
      }
    }
  )
}

# ---------- Extract athlete info from session.xml ----------
extract_athlete_info <- function(path) {
  doc <- tryCatch(read_xml_robust(path), error = function(e) NULL)
  if (is.null(doc)) return(NULL)
  
  root <- xml_root(doc)
  if (!identical(xml_name(root), "Subject")) return(NULL)
  
  # Extract subject fields
  fields <- xml_find_first(root, "./Fields")
  if (inherits(fields, "xml_missing")) return(NULL)
  
  # Core fields
  id <- nzchr(xml_text(xml_find_first(fields, "./ID")))
  name <- nzchr(xml_text(xml_find_first(fields, "./Name")))
  dob <- nzchr(xml_text(xml_find_first(fields, "./Date_of_birth")))
  gender <- nzchr(xml_text(xml_find_first(fields, "./Gender")))
  height <- nzchr(xml_text(xml_find_first(fields, "./Height")))
  weight <- nzchr(xml_text(xml_find_first(fields, "./Weight")))
  creation_date <- nzchr(xml_text(xml_find_first(fields, "./Creation_date")))
  creation_time <- nzchr(xml_text(xml_find_first(fields, "./Creation_time")))
  
  # Extract all other fields (excluding core fields to avoid duplicates)
  core_field_names <- c("ID", "Name", "Date_of_birth", "Gender", "Height", "Weight", 
                        "Creation_date", "Creation_time")
  all_fields <- xml_children(fields)
  field_list <- list()
  for (f in all_fields) {
    fname <- xml_name(f)
    # Skip core fields that we've already extracted
    if (fname %in% core_field_names) next
    fval <- nzchr(trimws(xml_text(f)))
    if (!is.na(fval) && fval != "") {
      field_list[[fname]] <- fval
    }
  }
  
  # Calculate age if DOB available
  age <- NA_real_
  if (!is.na(dob) && dob != "") {
    tryCatch({
      dob_date <- as.Date(dob, format = "%m/%d/%Y")
      if (!is.na(dob_date)) {
        age <- as.numeric(difftime(Sys.Date(), dob_date, units = "days")) / 365.25
      }
    }, error = function(e) NULL)
  }
  
  tibble(
    athlete_id = id,
    name = name,
    date_of_birth = dob,
    age = age,
    gender = gender,
    height = nznum(height),
    weight = nznum(weight),
    creation_date = creation_date,
    creation_time = creation_time,
    source_file = basename(path),
    source_path = path,
    !!!field_list
  )
}

# ---------- Extract time series data from session_data.xml ----------
parse_comma_data <- function(data_str) {
  if (is.na(data_str) || data_str == "") return(numeric(0))
  vals <- strsplit(data_str, ",", fixed = TRUE)[[1]]
  suppressWarnings(as.numeric(trimws(vals)))
}

# ---------- Extract ALL metric variables from session_data.xml ----------
extract_metric_data <- function(doc, owner_name) {
  root <- xml_root(doc)
  if (!identical(xml_name(root), "v3d")) return(tibble())
  
  owners <- xml_find_all(root, "./owner")
  if (!length(owners)) return(tibble())
  
  all_data <- list()
  
  for (own in owners) {
    own_val <- xml_attr(own, "value")
    if (own_val != owner_name) next
    
    metric_types <- xml_find_all(own, "./type[@value='METRIC']")
    if (!length(metric_types)) next
    
    for (mt in metric_types) {
      folders <- xml_find_all(mt, "./folder")
      for (fol in folders) {
        folder_val <- xml_attr(fol, "value")
        
        # Skip AT_EVENT folder
        if (!is.na(folder_val) && folder_val == "AT_EVENT") {
          next
        }
        
        names <- xml_find_all(fol, "./name")
        
        for (nm in names) {
          metric_name <- xml_attr(nm, "value")
          
          # Skip variables containing excluded strings
          excluded_patterns <- c(
            "Back_Foot_wrt_Lab",
            "Back_Ankle_Angle",
            "Lead_Ankle_Angle",
            "Back_Hip_Angle",
            "Glove_Elbow_Angle",
            "Glove_Shoulder_Angle",
            "Lead_Hip_Angle",
            "Back_Knee_Ang_Vel",
            "COM wrt Lead Heel_vel",
            "Combined COP wrt Lead Heel"
          )
          
          skip_variable <- FALSE
          for (pattern in excluded_patterns) {
            if (grepl(pattern, metric_name, ignore.case = TRUE)) {
              skip_variable <- TRUE
              break
            }
          }
          
          if (skip_variable) {
            next
          }
          
          # Extract metrics (filtered)
          
          comps <- xml_find_all(nm, "./component")
          for (comp in comps) {
            frames_attr <- xml_attr(comp, "frames")
            data_attr <- xml_attr(comp, "data") %||% xml_text(comp)
            
            if (is.na(frames_attr) || frames_attr == "") next
            
            frames <- suppressWarnings(as.integer(frames_attr))
            if (is.na(frames)) next
            
            # Handle both single values and time series
            values <- parse_comma_data(data_attr)
            if (length(values) == 0) next
            
            # Ensure lengths match
            n_vals <- length(values)
            if (n_vals != frames) {
              if (n_vals > frames) {
                values <- values[1:frames]
              } else {
                values <- c(values, rep(NA_real_, frames - n_vals))
              }
            }
            
            # Store time series data as separate columns (one per frame)
            # Create column names for values
            value_cols <- paste0("value_", 1:frames)
            
            # Create a list with all the data
            row_data <- list(
              owner = own_val,
              folder = folder_val,
              variable = metric_name
            )
            
            # Add value columns
            for (i in 1:frames) {
              row_data[[value_cols[i]]] <- values[i]
            }
            
            all_data[[length(all_data) + 1]] <- as_tibble(row_data)
          }
        }
      }
    }
  }
  
  if (length(all_data) == 0) return(tibble())
  bind_rows(all_data)
}

# ---------- Main processing function ----------
process_all_files <- function() {
  # Determine root directory
  root_dir <- if (is.null(DATA_ROOT)) {
    # Default to Pitching folder in current directory
    pitching_dir <- file.path(getwd(), "Pitching")
    if (dir.exists(pitching_dir)) {
      pitching_dir
    } else {
      getwd()
    }
  } else {
    DATA_ROOT
  }
  
  cat("Scanning for XML files in:", root_dir, "\n")
  
  # Find all session.xml and session_data.xml files (including .gz)
  session_files <- list.files(root_dir, pattern = "(?i)session\\.xml$", recursive = TRUE, full.names = TRUE)
  session_data_files <- list.files(root_dir, pattern = "(?i)session_data\\.xml$", recursive = TRUE, full.names = TRUE)
  
  # Also check for .gz files
  session_files <- c(session_files, list.files(root_dir, pattern = "(?i)session\\.xml\\.gz$", recursive = TRUE, full.names = TRUE))
  session_data_files <- c(session_data_files, list.files(root_dir, pattern = "(?i)session_data\\.xml\\.gz$", recursive = TRUE, full.names = TRUE))
  
  cat("Found", length(session_files), "session.xml files\n")
  cat("Found", length(session_data_files), "session_data.xml files\n")
  
  if (length(session_files) == 0 && length(session_data_files) == 0) {
    cat("\nERROR: No XML files found in", root_dir, "\n")
    cat("Trying alternative search...\n")
    # Try without recursive
    session_files <- list.files(root_dir, pattern = "(?i)session\\.xml$", recursive = FALSE, full.names = TRUE)
    session_data_files <- list.files(root_dir, pattern = "(?i)session_data\\.xml$", recursive = FALSE, full.names = TRUE)
    cat("Found (non-recursive):", length(session_files), "session.xml,", length(session_data_files), "session_data.xml\n")
    
    if (length(session_files) == 0 && length(session_data_files) == 0) {
      stop("No XML files found in ", root_dir)
    }
  }
  
  # Create database connection
  # Close any existing connections first
  tryCatch({
    if (file.exists(DB_FILE)) {
      # Try to disconnect any existing connections
      tryCatch({
        existing_con <- dbConnect(RSQLite::SQLite(), DB_FILE)
        dbDisconnect(existing_con)
      }, error = function(e) NULL)
      
      # Now try to remove
      Sys.sleep(0.1)  # Brief pause
      if (file.exists(DB_FILE)) {
        file.remove(DB_FILE)
        cat("Removed existing database file\n")
      }
    }
  }, error = function(e) {
    cat("Warning: Could not remove existing database file:", conditionMessage(e), "\n")
    cat("Will try to overwrite instead...\n")
  })
  con <- DBI::dbConnect(RSQLite::SQLite(), DB_FILE)
  
  # Process session.xml files to get athlete info
  athlete_list <- list()
  owner_mapping <- list()  # Map owner names to athlete IDs
  
  for (sf in session_files) {
    cat("Processing athlete info from:", sf, "\n")
    doc_athlete <- tryCatch(read_xml_robust(sf), error = function(e) NULL)
    if (is.null(doc_athlete)) {
      cat("  Could not read file\n")
      next
    }
    
    athlete_info <- extract_athlete_info(sf)
    if (!is.null(athlete_info) && nrow(athlete_info) > 0) {
      # Generate UID
      athlete_info$uid <- uuid::UUIDgenerate()
      
      dir_path <- dirname(sf)
      athlete_list[[length(athlete_list) + 1]] <- athlete_info
      
      # Map by directory path - this will be used to match session_data.xml files
      dir_path_normalized <- normalizePath(dir_path, winslash = "/", mustWork = FALSE)
      owner_mapping[[dir_path_normalized]] <- athlete_info$uid[1]
      
      # Also try to extract measurement filenames from session.xml to map owners
      # Look for Measurement elements with Filename attributes
      root_athlete <- xml_root(doc_athlete)
      measurements <- xml_find_all(root_athlete, ".//Measurement")
      if (length(measurements) > 0) {
        for (meas in measurements) {
          meas_filename <- xml_attr(meas, "Filename")
          if (!is.na(meas_filename) && meas_filename != "") {
            owner_mapping[[meas_filename]] <- athlete_info$uid[1]
            # Also map without extension
            meas_no_ext <- tools::file_path_sans_ext(meas_filename)
            owner_mapping[[meas_no_ext]] <- athlete_info$uid[1]
            cat("    Mapped measurement:", meas_filename, "\n")
          }
        }
      }
      
      cat("  Mapped athlete", athlete_info$name[1], "to UID:", athlete_info$uid[1], "\n")
      cat("  Directory:", dir_path_normalized, "\n")
    }
  }
  
  if (length(athlete_list) > 0) {
    athletes_df <- bind_rows(athlete_list)
    DBI::dbWriteTable(con, "athletes", athletes_df, overwrite = TRUE)
    cat("Created athletes table with", nrow(athletes_df), "rows\n")
  } else {
    # Create empty athletes table
    athletes_df <- tibble(
      uid = character(),
      athlete_id = character(),
      name = character(),
      date_of_birth = character(),
      age = numeric(),
      gender = character(),
      height = numeric(),
      weight = numeric(),
      creation_date = character(),
      creation_time = character(),
      source_file = character(),
      source_path = character()
    )
    DBI::dbWriteTable(con, "athletes", athletes_df, overwrite = TRUE)
  }
  
  # Process session_data.xml files
  metric_data_list <- list()
  
  for (sdf in session_data_files) {
    cat("Processing metric data from:", sdf, "\n")
    doc <- tryCatch(read_xml_robust(sdf), error = function(e) {
      cat("Error reading", sdf, ":", conditionMessage(e), "\n")
      NULL
    })
    if (is.null(doc)) next
    
    # Try to find owner names
    root <- xml_root(doc)
    if (identical(xml_name(root), "v3d")) {
      owners <- xml_find_all(root, "./owner")
      owner_names <- xml_attr(owners, "value")
      
      # Try to match owner to athlete
      dir_path <- dirname(sdf)
      
      # Look for matching athlete based on directory structure
      matched_uid <- NA_character_
      cat("  Found", length(owner_names), "owners in this file:", paste(owner_names, collapse = ", "), "\n")
      
      for (owner_name in owner_names) {
        matched_uid <- NA_character_
        
        # Extract base name from owner (remove path if present, keep extension)
        owner_base <- basename(owner_name)
        owner_no_ext <- tools::file_path_sans_ext(owner_base)
        
        # Try direct match first
        if (owner_base %in% names(owner_mapping)) {
          matched_uid <- owner_mapping[[owner_base]]
          cat("    Matched owner", owner_name, "to UID via direct match\n")
        } else if (owner_no_ext %in% names(owner_mapping)) {
          matched_uid <- owner_mapping[[owner_no_ext]]
          cat("    Matched owner", owner_name, "to UID via base name match\n")
        } else {
          # Try partial matching
          for (uid_key in names(owner_mapping)) {
            if (grepl(owner_no_ext, uid_key, ignore.case = TRUE) || grepl(uid_key, owner_no_ext, ignore.case = TRUE)) {
              matched_uid <- owner_mapping[[uid_key]]
              cat("    Matched owner", owner_name, "to UID via partial match with", uid_key, "\n")
              break
            }
          }
        }
        
        # If no match found, try to match by directory
        if (is.na(matched_uid) && length(athlete_list) > 0) {
          # Check if directory path is in mapping
          dir_path_normalized <- normalizePath(dir_path, winslash = "/", mustWork = FALSE)
          if (dir_path_normalized %in% names(owner_mapping)) {
            matched_uid <- owner_mapping[[dir_path_normalized]]
            cat("    Matched owner", owner_name, "to UID via directory path\n")
          } else {
            # Find session.xml in same directory or parent directories
            check_dirs <- c(dir_path, dirname(dir_path))
            for (check_dir in check_dirs) {
              session_xml <- file.path(check_dir, "session.xml")
              if (file.exists(session_xml)) {
                athlete_info <- extract_athlete_info(session_xml)
                if (!is.null(athlete_info) && nrow(athlete_info) > 0) {
                  # Generate UID if not exists
                  if (!"uid" %in% names(athlete_info)) {
                    athlete_info$uid <- uuid::UUIDgenerate()
                  }
                  matched_uid <- athlete_info$uid[1]
                  # Add to mapping for future owners
                  owner_mapping[[dir_path_normalized]] <- matched_uid
                  cat("    Matched owner", owner_name, "to UID via session.xml in", check_dir, "\n")
                  break
                }
              }
            }
          }
        }
        
        # If still no match, use the first athlete (since they're in the same directory)
        if (is.na(matched_uid) && length(athlete_list) > 0) {
          matched_uid <- athlete_list[[1]]$uid[1]
          cat("    Using first athlete UID for owner", owner_name, "\n")
        }
        
        if (is.na(matched_uid)) {
          cat("    WARNING: Could not match owner", owner_name, "to any athlete\n")
        }
        
        # Extract ALL metric data for this owner
        metric_data <- extract_metric_data(doc, owner_name)
        if (nrow(metric_data) > 0) {
          cat("      Extracted", nrow(metric_data), "rows of METRIC data\n")
          metric_data$uid <- matched_uid
          metric_data$source_file <- basename(sdf)
          metric_data$source_path <- sdf
          metric_data_list[[length(metric_data_list) + 1]] <- metric_data
        } else {
          cat("      No METRIC data found for", owner_name, "\n")
        }
      }
    }
  }
  
  # Combine and write to database
  # For time series data, we need to ensure all rows have the same columns
  # First, find maximum frame count across all data before binding
  
  if (length(metric_data_list) > 0) {
    # Find max frames before binding
    max_frames <- 0
    for (df in metric_data_list) {
      if (nrow(df) > 0) {
        # Find the maximum value column index
        value_cols <- grep("^value_\\d+$", names(df), value = TRUE)
        if (length(value_cols) > 0) {
          # Extract numbers from column names
          frame_nums <- suppressWarnings(as.integer(gsub("value_", "", value_cols)))
          frame_nums <- frame_nums[!is.na(frame_nums)]
          if (length(frame_nums) > 0) {
            max_frames <- max(max_frames, max(frame_nums), na.rm = TRUE)
          }
        }
      }
    }
    cat("Maximum frames in METRIC data:", max_frames, "\n")
    
    # Pad each dataframe to max_frames before binding
    meta_cols <- c("uid", "owner", "folder", "variable", "source_file", "source_path")
    value_cols <- paste0("value_", 1:max_frames)
    
    padded_list <- list()
    for (df in metric_data_list) {
      # Remove any time_ or frame_ columns that might exist (if any)
      time_frame_cols <- grep("^(time_|frame_)", names(df), value = TRUE)
      if (length(time_frame_cols) > 0) {
        df <- df %>% select(-any_of(time_frame_cols))
      }
      
      # Add missing columns with NA
      for (i in 1:max_frames) {
        value_col <- paste0("value_", i)
        if (!value_col %in% names(df)) {
          df[[value_col]] <- NA_real_
        }
      }
      
      # Reorder columns - only keep meta_cols and value_cols
      df <- df %>% select(any_of(c(meta_cols, value_cols)))
      padded_list[[length(padded_list) + 1]] <- df
    }
    
    metric_df <- bind_rows(padded_list)
    
    DBI::dbWriteTable(con, "metrics", metric_df, overwrite = TRUE)
    cat("Created metrics table with", nrow(metric_df), "rows and", ncol(metric_df), "columns\n")
    
    # Create indexes for faster queries
    cat("Creating indexes for faster queries...\n")
    tryCatch({
      DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_metrics_uid ON metrics(uid)")
      DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_metrics_variable ON metrics(variable)")
      DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_metrics_folder ON metrics(folder)")
      DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_metrics_uid_variable ON metrics(uid, variable)")
      cat("  Created indexes on uid, variable, folder, and (uid, variable)\n")
    }, error = function(e) {
      cat("  Warning: Could not create all indexes:", conditionMessage(e), "\n")
    })
  } else {
    metric_df <- tibble(
      uid = character(),
      owner = character(),
      folder = character(),
      variable = character(),
      source_file = character(),
      source_path = character()
    )
    DBI::dbWriteTable(con, "metrics", metric_df, overwrite = TRUE)
    cat("Created empty metrics table\n")
  }
  
  # Create index on athletes table
  if (length(athlete_list) > 0) {
    tryCatch({
      DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_athletes_uid ON athletes(uid)")
      DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_athletes_name ON athletes(name)")
      cat("  Created indexes on athletes table\n")
    }, error = function(e) {
      cat("  Warning: Could not create athlete indexes:", conditionMessage(e), "\n")
    })
  }
  
  # Close database connection
  DBI::dbDisconnect(con)
  cat("\nDatabase created successfully:", DB_FILE, "\n")
  cat("Total athletes:", length(athlete_list), "\n")
  cat("Total metric records:", if (length(metric_data_list) > 0) nrow(metric_df) else 0, "\n")
}

# ---------- Run ----------
cat("Starting pitching data extraction...\n")
cat("Database file:", DB_FILE, "\n")
cat("Data root:", if (is.null(DATA_ROOT)) "Current directory (Pitching folder)" else DATA_ROOT, "\n\n")

tryCatch({
  process_all_files()
  cat("\nProcessing completed successfully!\n")
}, error = function(e) {
  cat("\nError during processing:\n")
  cat(conditionMessage(e), "\n")
  traceback()
})

