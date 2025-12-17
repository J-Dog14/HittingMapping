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

# ---------- Configuration ----------
# Set to NULL to use current directory, or specify path
DATA_ROOT <- NULL  # Set to "D:\\Hitting\\Data" for production, NULL for testing with local files
DB_FILE <- "hitting_data.db"

# Variables to extract
LINK_MODEL_BASED_VARS <- c("Pelvis_Angle", "Pelvis_Shoulders_Separation", "Trunk_Angle")
METRIC_VARS <- c("Max_RPV_CGPos_VLab_Linear_Vel")
EVENT_LABEL_VARS <- c("MaxKneeHeight", "Contact", "CONTACT_100MSAFTER", "Lead_Foot_Down", "Lead_Foot_Off", "Downswing")

# ---------- Helpers ----------
`%||%` <- function(a, b) if (!is.null(a)) a else b
nzchr <- function(x) ifelse(is.na(x) | x == "", NA_character_, x)
nznum <- function(x) suppressWarnings(readr::parse_number(x))
read_xml_robust <- function(path) {
  tryCatch(
    read_xml(path),
    error = function(e) {
      txt <- readr::read_file(path)
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

extract_link_model_based <- function(doc, owner_name) {
  root <- xml_root(doc)
  if (!identical(xml_name(root), "v3d")) return(tibble())
  
  owners <- xml_find_all(root, "./owner")
  if (!length(owners)) return(tibble())
  
  all_data <- list()
  
  for (own in owners) {
    own_val <- xml_attr(own, "value")
    if (own_val != owner_name) next
    
    link_types <- xml_find_all(own, "./type[@value='LINK_MODEL_BASED']")
    if (!length(link_types)) {
      cat("      No LINK_MODEL_BASED type found for owner", owner_name, "\n")
      next
    }
    
    for (lt in link_types) {
      folders <- xml_find_all(lt, "./folder")
      cat("      Found", length(folders), "folders in LINK_MODEL_BASED\n")
      for (fol in folders) {
        folder_val <- xml_attr(fol, "value")
        names <- xml_find_all(fol, "./name")
        cat("      Folder:", folder_val, "has", length(names), "variables\n")
        
        for (nm in names) {
          metric_name <- xml_attr(nm, "value")
          if (!metric_name %in% LINK_MODEL_BASED_VARS) next
          cat("        Processing variable:", metric_name, "\n")
          
          comps <- xml_find_all(nm, "./component")
          for (comp in comps) {
            axis <- xml_attr(comp, "value")
            frames_attr <- xml_attr(comp, "frames")
            data_attr <- xml_attr(comp, "data") %||% xml_text(comp)
            frame_start <- xml_attr(comp, "Frame_Start")
            frame_end <- xml_attr(comp, "Frame_End")
            time_start <- xml_attr(comp, "Time_Start")
            time_end <- xml_attr(comp, "Time_End")
            
            if (is.na(frames_attr) || frames_attr == "" || frames_attr == "1") {
              cat("          Skipping component", axis, "- frames =", frames_attr, "(not time series)\n")
              next
            }
            
            frames <- suppressWarnings(as.integer(frames_attr))
            if (is.na(frames) || frames <= 1) {
              cat("          Skipping component", axis, "- frames =", frames, "(not time series)\n")
              next
            }
            cat("          Found time series data: axis =", axis, ", frames =", frames, "\n")
            
            values <- parse_comma_data(data_attr)
            if (length(values) == 0) next
            
            # Create time points
            if (!is.na(time_start) && !is.na(time_end)) {
              time_start_num <- suppressWarnings(as.numeric(time_start))
              time_end_num <- suppressWarnings(as.numeric(time_end))
              if (!is.na(time_start_num) && !is.na(time_end_num) && frames > 1) {
                time_points <- seq(time_start_num, time_end_num, length.out = frames)
              } else {
                time_points <- 0:(frames - 1)
              }
            } else {
              time_points <- 0:(frames - 1)
            }
            
            # Create frame numbers
            if (!is.na(frame_start) && !is.na(frame_end)) {
              frame_start_num <- suppressWarnings(as.integer(frame_start))
              frame_end_num <- suppressWarnings(as.integer(frame_end))
              if (!is.na(frame_start_num) && !is.na(frame_end_num)) {
                frame_numbers <- seq(frame_start_num, frame_end_num, length.out = frames)
              } else {
                frame_numbers <- 1:frames
              }
            } else {
              frame_numbers <- 1:frames
            }
            
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
            # Create column names for values, time, and frame numbers
            value_cols <- paste0("value_", 1:frames)
            time_cols <- paste0("time_", 1:frames)
            frame_cols <- paste0("frame_", 1:frames)
            
            # Create a list with all the data
            row_data <- list(
              owner = own_val,
              folder = folder_val,
              variable = metric_name,
              axis = axis,
              frame_count = frames,
              frame_start = if (!is.na(frame_start)) suppressWarnings(as.integer(frame_start)) else NA_integer_,
              frame_end = if (!is.na(frame_end)) suppressWarnings(as.integer(frame_end)) else NA_integer_,
              time_start = if (!is.na(time_start)) suppressWarnings(as.numeric(time_start)) else NA_real_,
              time_end = if (!is.na(time_end)) suppressWarnings(as.numeric(time_end)) else NA_real_
            )
            
            # Add value columns
            for (i in 1:frames) {
              row_data[[value_cols[i]]] <- values[i]
            }
            
            # Add time columns
            for (i in 1:frames) {
              row_data[[time_cols[i]]] <- time_points[i]
            }
            
            # Add frame number columns
            for (i in 1:frames) {
              row_data[[frame_cols[i]]] <- frame_numbers[i]
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
        names <- xml_find_all(fol, "./name")
        
        for (nm in names) {
          metric_name <- xml_attr(nm, "value")
          if (!metric_name %in% METRIC_VARS) next
          
          comps <- xml_find_all(nm, "./component")
          for (comp in comps) {
            frames_attr <- xml_attr(comp, "frames")
            data_attr <- xml_attr(comp, "data") %||% xml_text(comp)
            frame_start <- xml_attr(comp, "Frame_Start")
            frame_end <- xml_attr(comp, "Frame_End")
            time_start <- xml_attr(comp, "Time_Start")
            time_end <- xml_attr(comp, "Time_End")
            
            if (is.na(frames_attr) || frames_attr == "") next
            
            frames <- suppressWarnings(as.integer(frames_attr))
            if (is.na(frames)) next
            
            # Handle both single values and time series
            values <- parse_comma_data(data_attr)
            if (length(values) == 0) next
            
            # Create time points
            if (frames == 1) {
              time_points <- if (!is.na(time_start)) suppressWarnings(as.numeric(time_start)) else 0.0
              frame_numbers <- if (!is.na(frame_start)) suppressWarnings(as.integer(frame_start)) else 1L
              # Ensure they are vectors, not scalars
              time_points <- c(time_points)
              frame_numbers <- c(frame_numbers)
            } else {
              if (!is.na(time_start) && !is.na(time_end)) {
                time_start_num <- suppressWarnings(as.numeric(time_start))
                time_end_num <- suppressWarnings(as.numeric(time_end))
                if (!is.na(time_start_num) && !is.na(time_end_num) && frames > 1) {
                  time_points <- seq(time_start_num, time_end_num, length.out = frames)
                } else {
                  time_points <- 0:(frames - 1)
                }
              } else {
                time_points <- 0:(frames - 1)
              }
              
              # Create frame numbers
              if (!is.na(frame_start) && !is.na(frame_end)) {
                frame_start_num <- suppressWarnings(as.integer(frame_start))
                frame_end_num <- suppressWarnings(as.integer(frame_end))
                if (!is.na(frame_start_num) && !is.na(frame_end_num)) {
                  frame_numbers <- seq(frame_start_num, frame_end_num, length.out = frames)
                } else {
                  frame_numbers <- 1:frames
                }
              } else {
                frame_numbers <- 1:frames
              }
            }
            
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

extract_event_label_data <- function(doc, owner_name) {
  root <- xml_root(doc)
  if (!identical(xml_name(root), "v3d")) return(tibble())
  
  owners <- xml_find_all(root, "./owner")
  if (!length(owners)) return(tibble())
  
  all_data <- list()
  
  for (own in owners) {
    own_val <- xml_attr(own, "value")
    if (own_val != owner_name) next
    
    event_types <- xml_find_all(own, "./type[@value='EVENT_LABEL']")
    if (!length(event_types)) next
    
    for (et in event_types) {
      folders <- xml_find_all(et, "./folder")
      for (fol in folders) {
        folder_val <- xml_attr(fol, "value")
        names <- xml_find_all(fol, "./name")
        
        for (nm in names) {
          event_name <- xml_attr(nm, "value")
          if (!event_name %in% EVENT_LABEL_VARS) next
          
          comps <- xml_find_all(nm, "./component")
          for (comp in comps) {
            frames_attr <- xml_attr(comp, "frames")
            data_attr <- xml_attr(comp, "data") %||% xml_text(comp)
            frame_start <- xml_attr(comp, "Frame_Start")
            frame_end <- xml_attr(comp, "Frame_End")
            time_start <- xml_attr(comp, "Time_Start")
            time_end <- xml_attr(comp, "Time_End")
            
            if (is.na(frames_attr) || frames_attr == "") next
            
            frames <- suppressWarnings(as.integer(frames_attr))
            if (is.na(frames)) next
            
            # Handle both single values and time series
            values <- parse_comma_data(data_attr)
            if (length(values) == 0) next
            
            # Create time points
            if (frames == 1) {
              time_points <- if (!is.na(time_start)) suppressWarnings(as.numeric(time_start)) else 0.0
              frame_numbers <- if (!is.na(frame_start)) suppressWarnings(as.integer(frame_start)) else 1L
              # Ensure they are vectors, not scalars
              time_points <- c(time_points)
              frame_numbers <- c(frame_numbers)
            } else {
              if (!is.na(time_start) && !is.na(time_end)) {
                time_start_num <- suppressWarnings(as.numeric(time_start))
                time_end_num <- suppressWarnings(as.numeric(time_end))
                if (!is.na(time_start_num) && !is.na(time_end_num) && frames > 1) {
                  time_points <- seq(time_start_num, time_end_num, length.out = frames)
                } else {
                  time_points <- 0:(frames - 1)
                }
              } else {
                time_points <- 0:(frames - 1)
              }
              
              # Create frame numbers
              if (!is.na(frame_start) && !is.na(frame_end)) {
                frame_start_num <- suppressWarnings(as.integer(frame_start))
                frame_end_num <- suppressWarnings(as.integer(frame_end))
                if (!is.na(frame_start_num) && !is.na(frame_end_num)) {
                  frame_numbers <- seq(frame_start_num, frame_end_num, length.out = frames)
                } else {
                  frame_numbers <- 1:frames
                }
              } else {
                frame_numbers <- 1:frames
              }
            }
            
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
              variable = event_name
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
process_all_files <- function(root_dir = DATA_ROOT) {
  # Use current directory if root_dir is NULL
  if (is.null(root_dir)) {
    root_dir <- getwd()
    cat("Using current working directory:", root_dir, "\n")
  }
  
  cat("Scanning for XML files in:", root_dir, "\n")
  cat("Directory exists:", dir.exists(root_dir), "\n")
  
  # List all XML files first for debugging
  all_xmls <- list.files(root_dir, pattern = "\\.xml$", recursive = TRUE, full.names = TRUE, ignore.case = TRUE)
  cat("Total XML files found:", length(all_xmls), "\n")
  if (length(all_xmls) > 0) {
    cat("XML files:\n")
    for (f in all_xmls) cat("  -", f, "\n")
  }
  
  # Find all XML files
  session_files <- list.files(root_dir, pattern = "(?i)session\\.xml$", recursive = TRUE, full.names = TRUE)
  session_data_files <- list.files(root_dir, pattern = "(?i)session_data\\.xml$", recursive = TRUE, full.names = TRUE)
  
  cat("\nFound", length(session_files), "session.xml files\n")
  if (length(session_files) > 0) {
    for (f in session_files) cat("  -", f, "\n")
  }
  
  cat("Found", length(session_data_files), "session_data.xml files\n")
  if (length(session_data_files) > 0) {
    for (f in session_data_files) cat("  -", f, "\n")
  }
  
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
  link_data_list <- list()
  metric_data_list <- list()
  event_data_list <- list()
  
  for (sdf in session_data_files) {
    cat("Processing time series data from:", sdf, "\n")
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
        
        # Skip Static Sports trials - only process Swing trials
        if (grepl("Static", owner_name, ignore.case = TRUE)) {
          cat("      Skipping Static Sports trial:", owner_name, "\n")
          next
        }
        
        # Extract data for this owner
        link_data <- extract_link_model_based(doc, owner_name)
        if (nrow(link_data) > 0) {
          cat("      Extracted", nrow(link_data), "rows of LINK_MODEL_BASED data\n")
          link_data$uid <- matched_uid
          link_data$source_file <- basename(sdf)
          link_data$source_path <- sdf
          link_data_list[[length(link_data_list) + 1]] <- link_data
        } else {
          cat("      No LINK_MODEL_BASED data found for", owner_name, "\n")
        }
        
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
        
        event_data <- extract_event_label_data(doc, owner_name)
        if (nrow(event_data) > 0) {
          cat("      Extracted", nrow(event_data), "rows of EVENT_LABEL data\n")
          event_data$uid <- matched_uid
          event_data$source_file <- basename(sdf)
          event_data$source_path <- sdf
          event_data_list[[length(event_data_list) + 1]] <- event_data
        } else {
          cat("      No EVENT_LABEL data found for", owner_name, "\n")
        }
      }
    }
  }
  
  # Combine and write to database
  # For time series data, we need to ensure all rows have the same columns
  # First, find maximum frame count across all data before binding
  
  if (length(link_data_list) > 0) {
    # Find max frames before binding
    max_frames <- 0
    for (df in link_data_list) {
      if (nrow(df) > 0 && "frame_count" %in% names(df)) {
        max_frames <- max(max_frames, max(df$frame_count, na.rm = TRUE), na.rm = TRUE)
      }
    }
    cat("Maximum frames in LINK_MODEL_BASED:", max_frames, "\n")
    
    # Pad each dataframe to max_frames before binding
    meta_cols <- c("uid", "owner", "folder", "variable", "axis", "frame_count", 
                   "frame_start", "frame_end", "time_start", "time_end", 
                   "source_file", "source_path")
    value_cols <- paste0("value_", 1:max_frames)
    time_cols <- paste0("time_", 1:max_frames)
    frame_cols <- paste0("frame_", 1:max_frames)
    
    padded_list <- list()
    for (df in link_data_list) {
      # Add missing columns with NA
      for (i in 1:max_frames) {
        value_col <- paste0("value_", i)
        time_col <- paste0("time_", i)
        frame_col <- paste0("frame_", i)
        
        if (!value_col %in% names(df)) {
          df[[value_col]] <- NA_real_
        }
        if (!time_col %in% names(df)) {
          df[[time_col]] <- NA_real_
        }
        if (!frame_col %in% names(df)) {
          df[[frame_col]] <- NA_integer_
        }
      }
      
      # Reorder columns
      df <- df %>% select(any_of(c(meta_cols, value_cols, time_cols, frame_cols)))
      padded_list[[length(padded_list) + 1]] <- df
    }
    
    link_df <- bind_rows(padded_list)
    
    DBI::dbWriteTable(con, "link_model_based", link_df, overwrite = TRUE)
    cat("Created link_model_based table with", nrow(link_df), "rows and", ncol(link_df), "columns\n")
  } else {
    link_df <- tibble(
      uid = character(),
      owner = character(),
      folder = character(),
      variable = character(),
      axis = character(),
      frame_count = integer(),
      frame_start = integer(),
      frame_end = integer(),
      time_start = numeric(),
      time_end = numeric(),
      source_file = character(),
      source_path = character()
    )
    DBI::dbWriteTable(con, "link_model_based", link_df, overwrite = TRUE)
  }
  
  if (length(metric_data_list) > 0) {
    # Find max frames before binding
    max_frames <- 0
    for (df in metric_data_list) {
      if (nrow(df) > 0 && "frame_count" %in% names(df)) {
        max_frames <- max(max_frames, max(df$frame_count, na.rm = TRUE), na.rm = TRUE)
      }
    }
    cat("Maximum frames in METRIC:", max_frames, "\n")
    
    # Pad each dataframe to max_frames before binding
    meta_cols <- c("uid", "owner", "folder", "variable", 
                   "source_file", "source_path")
    value_cols <- paste0("value_", 1:max_frames)
    
    padded_list <- list()
    for (df in metric_data_list) {
      # Add missing columns with NA
      for (i in 1:max_frames) {
        value_col <- paste0("value_", i)
        
        if (!value_col %in% names(df)) {
          df[[value_col]] <- NA_real_
        }
      }
      
      # Reorder columns
      df <- df %>% select(any_of(c(meta_cols, value_cols)))
      padded_list[[length(padded_list) + 1]] <- df
    }
    
    metric_df <- bind_rows(padded_list)
    
    DBI::dbWriteTable(con, "metric", metric_df, overwrite = TRUE)
    cat("Created metric table with", nrow(metric_df), "rows and", ncol(metric_df), "columns\n")
  } else {
    metric_df <- tibble(
      uid = character(),
      owner = character(),
      folder = character(),
      variable = character(),
      source_file = character(),
      source_path = character()
    )
    DBI::dbWriteTable(con, "metric", metric_df, overwrite = TRUE)
  }
  
  if (length(event_data_list) > 0) {
    # Find max frames before binding
    max_frames <- 0
    for (df in event_data_list) {
      if (nrow(df) > 0 && "frame_count" %in% names(df)) {
        max_frames <- max(max_frames, max(df$frame_count, na.rm = TRUE), na.rm = TRUE)
      }
    }
    cat("Maximum frames in EVENT_LABEL:", max_frames, "\n")
    
    # Pad each dataframe to max_frames before binding
    meta_cols <- c("uid", "owner", "folder", "variable", 
                   "source_file", "source_path")
    value_cols <- paste0("value_", 1:max_frames)
    
    padded_list <- list()
    for (df in event_data_list) {
      # Add missing columns with NA
      for (i in 1:max_frames) {
        value_col <- paste0("value_", i)
        
        if (!value_col %in% names(df)) {
          df[[value_col]] <- NA_real_
        }
      }
      
      # Reorder columns
      df <- df %>% select(any_of(c(meta_cols, value_cols)))
      padded_list[[length(padded_list) + 1]] <- df
    }
    
    event_df <- bind_rows(padded_list)
    
    DBI::dbWriteTable(con, "event_label", event_df, overwrite = TRUE)
    cat("Created event_label table with", nrow(event_df), "rows and", ncol(event_df), "columns\n")
  } else {
    event_df <- tibble(
      uid = character(),
      owner = character(),
      folder = character(),
      variable = character(),
      source_file = character(),
      source_path = character()
    )
    DBI::dbWriteTable(con, "event_label", event_df, overwrite = TRUE)
  }
  
  DBI::dbDisconnect(con)
  cat("\nDatabase created successfully:", DB_FILE, "\n")
  cat("Tables created: athletes, link_model_based, metric, event_label\n")
}

# ---------- Run ----------
cat("=== Starting data processing ===\n")
cat("Script started at:", format(Sys.time()), "\n")
cat("Current working directory:", getwd(), "\n")
cat("DATA_ROOT is set to:", if(is.null(DATA_ROOT)) "NULL (using current directory)" else DATA_ROOT, "\n")
cat("DB_FILE will be:", DB_FILE, "\n\n")

# Flush output to ensure we see messages
flush.console()

tryCatch({
  result <- process_all_files()
  cat("\n=== Processing complete ===\n")
  cat("Final result:", if(is.null(result)) "NULL" else class(result), "\n")
}, error = function(e) {
  cat("\n=== ERROR ===\n")
  cat("Error message:", conditionMessage(e), "\n")
  cat("Error class:", class(e), "\n")
  print(traceback())
  flush.console()
})
