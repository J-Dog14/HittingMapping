# ==== Minimal deps ====
library(xml2)
library(purrr)
library(dplyr)
library(readr)
library(stringr)
library(tibble)
library(tidyr)
library(tools)
library(DBI)
library(RSQLite)

# ---------- Configuration ----------
# Set to NULL to use current directory, or specify path
DATA_ROOT <- "H:/Pitching/Data"  # Set to your pitching data directory path, NULL for testing with local files
OUTPUT_DB <- "pitching_accel_data_v1.db"

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

parse_comma_data <- function(data_str) {
  if (is.na(data_str) || data_str == "") return(numeric(0))
  vals <- strsplit(data_str, ",", fixed = TRUE)[[1]]
  vals <- trimws(vals)
  # Convert "nodata" to NA, keep empty strings as NA
  vals[vals == "nodata" | vals == ""] <- NA_character_
  suppressWarnings(as.numeric(vals))
}

# Calculate acceleration from velocity using central difference
calculate_acceleration <- function(vel_z, fs = 240) {
  if (length(vel_z) < 3) {
    return(rep(NA_real_, length(vel_z)))
  }
  dt <- 1 / fs
  acc_z <- c(
    NA,
    (vel_z[3:length(vel_z)] - vel_z[1:(length(vel_z) - 2)]) / (2 * dt),
    NA
  )
  return(acc_z)
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
  
  # Calculate age if DOB available (age at current time)
  age <- NA_real_
  if (!is.na(dob) && dob != "") {
    tryCatch({
      dob_date <- as.Date(dob, format = "%m/%d/%Y")
      if (!is.na(dob_date)) {
        age <- as.numeric(difftime(Sys.Date(), dob_date, units = "days")) / 365.25
      }
    }, error = function(e) NULL)
  }
  
  # Calculate age at collection (age at time of data collection)
  age_at_collection <- NA_real_
  if (!is.na(dob) && dob != "" && !is.na(creation_date) && creation_date != "") {
    tryCatch({
      dob_date <- as.Date(dob, format = "%m/%d/%Y")
      # Try common date formats for creation_date
      creation_date_parsed <- NULL
      for (fmt in c("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d")) {
        creation_date_parsed <- as.Date(creation_date, format = fmt)
        if (!is.na(creation_date_parsed)) break
      }
      
      if (!is.na(dob_date) && !is.null(creation_date_parsed) && !is.na(creation_date_parsed)) {
        age_at_collection <- as.numeric(difftime(creation_date_parsed, dob_date, units = "days")) / 365.25
      }
    }, error = function(e) NULL)
  }
  
  tibble(
    athlete_id = id,
    name = name,
    date_of_birth = dob,
    age = age,
    age_at_collection = age_at_collection,
    gender = gender,
    height = nznum(height),
    weight = nznum(weight),
    creation_date = creation_date,
    creation_time = creation_time,
    source_file = basename(path),
    source_path = path
  )
}

# ---------- Extract time series variables from session_data.xml ----------
extract_time_series_data <- function(doc, owner_name) {
  root <- xml_root(doc)
  if (!identical(xml_name(root), "v3d")) return(tibble())
  
  owners <- xml_find_all(root, "./owner")
  if (!length(owners)) return(tibble())
  
  # Variables to extract
  TS_VARS <- c(
    "Shoulder Torque",
    "Elbow Torque",
    "Pitching_Shoulder_Ang_Vel",
    "Pitching_Humerus_Ang_Vel",
    "Pitching_Shoulder_Angle"
  )
  
  all_data <- list()
  
  for (own in owners) {
    own_val <- xml_attr(own, "value")
    if (own_val != owner_name) next
    
    # Look for LINK_MODEL_BASED type (where time series data is stored)
    link_types <- xml_find_all(own, "./type[@value='LINK_MODEL_BASED']")
    if (!length(link_types)) {
      cat("      No LINK_MODEL_BASED type found for owner", own_val, "\n")
      next
    }
    cat("      Found", length(link_types), "LINK_MODEL_BASED type(s)\n")
    
    for (mt in link_types) {
      folders <- xml_find_all(mt, "./folder")
      cat("        Found", length(folders), "folder(s)\n")
      for (fol in folders) {
        folder_val <- xml_attr(fol, "value")
        names <- xml_find_all(fol, "./name")
        cat("        Folder:", folder_val, "- Found", length(names), "name(s)\n")
        
        for (nm in names) {
          metric_name <- xml_attr(nm, "value")
          
          # Debug: print all metric names we encounter
          if (grepl("Shoulder|Elbow|Pitching", metric_name, ignore.case = TRUE)) {
            cat("      Found metric:", metric_name, "\n")
          }
          
          # Check if this is one of our target variables
          if (!metric_name %in% TS_VARS) next
          
          cat("      Processing metric:", metric_name, "\n")
          
          comps <- xml_find_all(nm, "./component")
          
          # For Pitching_Shoulder_Ang_Vel, Pitching_Humerus_Ang_Vel, Pitching_Shoulder_Angle, only get Z component
          # For Shoulder Torque and Elbow Torque, get all components
          target_components <- if (metric_name %in% c("Pitching_Shoulder_Ang_Vel", 
                                                      "Pitching_Humerus_Ang_Vel", 
                                                      "Pitching_Shoulder_Angle")) {
            "Z"
          } else {
            c("X", "Y", "Z")
          }
          
          for (comp in comps) {
            axis <- xml_attr(comp, "value")
            if (!axis %in% target_components) {
              cat("        Skipping component", axis, "- not in target components\n")
              next
            }
            
            frames_attr <- xml_attr(comp, "frames")
            data_attr <- xml_attr(comp, "data") %||% xml_text(comp)
            time_start <- xml_attr(comp, "Time_Start")
            time_end <- xml_attr(comp, "Time_End")
            
            cat("        Component", axis, "- frames:", frames_attr, "\n")
            
            if (is.na(frames_attr) || frames_attr == "" || frames_attr == "1") {
              # Skip scalar values (not time series)
              cat("        Skipping - scalar value (frames =", frames_attr, ")\n")
              next
            }
            
            frames <- suppressWarnings(as.integer(frames_attr))
            if (is.na(frames) || frames <= 1) {
              cat("        Skipping - invalid frames:", frames, "\n")
              next
            }
            
            cat("        Processing - frames:", frames, "\n")
            
            # Parse time series data
            values <- parse_comma_data(data_attr)
            if (length(values) == 0) {
              cat("      Warning: No values found for", metric_name, axis, "\n")
              next
            }
            
            # Ensure lengths match frames attribute
            n_vals <- length(values)
            if (n_vals != frames) {
              if (n_vals > frames) {
                values <- values[1:frames]
              } else {
                # Pad with NA if shorter
                values <- c(values, rep(NA_real_, frames - n_vals))
              }
            }
            
            # Check if we have at least some valid (non-NA) values
            valid_count <- sum(!is.na(values))
            if (valid_count == 0) {
              cat("      Warning: All values are NA for", metric_name, axis, "\n")
              next
            }
            
            # Create time points if available
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
            
            # Store time series data horizontally
            row_data <- list(
              owner = own_val,
              folder = folder_val,
              variable = metric_name,
              component = axis,
              frames = frames,
              time_start = if (!is.na(time_start)) suppressWarnings(as.numeric(time_start)) else NA_real_,
              time_end = if (!is.na(time_end)) suppressWarnings(as.numeric(time_end)) else NA_real_
            )
            
            # Add value columns (value_1, value_2, ..., value_N)
            for (i in 1:frames) {
              row_data[[paste0("value_", i)]] <- values[i]
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

# ---------- Extract event labels from session_data.xml ----------
extract_event_labels <- function(doc, owner_name) {
  root <- xml_root(doc)
  if (!identical(xml_name(root), "v3d")) return(tibble())
  
  owners <- xml_find_all(root, "./owner")
  if (!length(owners)) return(tibble())
  
  # Events to extract
  EVENT_VARS <- c("Footstrike", "Max_Shoulder_Rot", "Release", "Release100msAfter")
  
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
          if (!event_name %in% EVENT_VARS) next
          
          comps <- xml_find_all(nm, "./component")
          # Events typically have one component with the time value
          if (length(comps) > 0) {
            comp <- comps[1]  # Usually just one component
            data_attr <- xml_attr(comp, "data") %||% xml_text(comp)
            time_val <- suppressWarnings(as.numeric(trimws(data_attr)))
            
            if (!is.na(time_val)) {
              row_data <- list(
                owner = own_val,
                folder = folder_val,
                event = event_name,
                time = time_val
              )
              
              all_data[[length(all_data) + 1]] <- as_tibble(row_data)
            }
          }
        }
      }
    }
  }
  
  if (length(all_data) == 0) return(tibble())
  bind_rows(all_data)
}

# ---------- Calculate derived metrics ----------
calculate_derived_metrics <- function(ts_data, event_data) {
  # Check if ts_data is empty or missing required columns
  if (is.null(ts_data) || nrow(ts_data) == 0 || !"variable" %in% names(ts_data)) {
    return(tibble(
      max_ir_velocity = NA_real_,
      max_ir_time = NA_real_,
      time_max_ir_to_mer = NA_real_,
      time_max_ir_to_rel = NA_real_
    ))
  }
  
  # Get Pitching_Shoulder_Ang_Vel Z component
  shoulder_vel_z <- ts_data %>%
    filter(variable == "Pitching_Shoulder_Ang_Vel", component == "Z")
  
  if (nrow(shoulder_vel_z) == 0) {
    return(tibble(
      max_ir_velocity = NA_real_,
      max_ir_time = NA_real_,
      time_max_ir_to_mer = NA_real_,
      time_max_ir_to_rel = NA_real_
    ))
  }
  
  # Get event times
  footstrike_time <- event_data %>% filter(event == "Footstrike") %>% pull(time) %>% first()
  max_shoulder_rot_time <- event_data %>% filter(event == "Max_Shoulder_Rot") %>% pull(time) %>% first()
  release_time <- event_data %>% filter(event == "Release") %>% pull(time) %>% first()
  
  if (any(is.na(c(footstrike_time, max_shoulder_rot_time, release_time)))) {
    return(tibble())
  }
  
  # Extract time series values and times
  value_cols <- grep("^value_\\d+$", names(shoulder_vel_z), value = TRUE)
  if (length(value_cols) == 0) return(tibble())
  
  # Get time points from time_start and time_end
  time_start <- shoulder_vel_z$time_start[1]
  time_end <- shoulder_vel_z$time_end[1]
  frames <- shoulder_vel_z$frames[1]
  
  if (is.na(time_start) || is.na(time_end) || is.na(frames)) {
    return(tibble())
  }
  
  time_points <- seq(time_start, time_end, length.out = frames)
  values <- as.numeric(shoulder_vel_z[1, value_cols])
  
  # Find indices for events
  idx_max_shoulder_rot <- which.min(abs(time_points - max_shoulder_rot_time))
  idx_release <- which.min(abs(time_points - release_time))
  
  # Find max IR velocity between Max_Shoulder_Rot and Release
  if (idx_max_shoulder_rot < idx_release) {
    window_values <- values[idx_max_shoulder_rot:idx_release]
    window_times <- time_points[idx_max_shoulder_rot:idx_release]
    
    max_ir_idx <- which.max(abs(window_values))
    max_ir_velocity <- window_values[max_ir_idx]
    max_ir_time <- window_times[max_ir_idx]
    
    # Calculate time differences
    time_max_ir_to_mer <- max_ir_time - max_shoulder_rot_time  # MER = Max External Rotation = Max_Shoulder_Rot
    time_max_ir_to_rel <- release_time - max_ir_time  # Time from max IR to release
    
    tibble(
      max_ir_velocity = max_ir_velocity,
      max_ir_time = max_ir_time,
      time_max_ir_to_mer = time_max_ir_to_mer,
      time_max_ir_to_rel = time_max_ir_to_rel
    )
  } else {
    tibble(
      max_ir_velocity = NA_real_,
      max_ir_time = NA_real_,
      time_max_ir_to_mer = NA_real_,
      time_max_ir_to_rel = NA_real_
    )
  }
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
    stop("No XML files found in ", root_dir)
  }
  
  # Process session.xml files to get athlete info and velocity data
  athlete_list <- list()
  owner_mapping <- list()
  velocity_mapping <- list()  # Map filename (without extension) to velocity
  
  for (sf in session_files) {
    cat("Processing athlete info from:", sf, "\n")
    athlete_info <- extract_athlete_info(sf)
    if (!is.null(athlete_info) && nrow(athlete_info) > 0) {
      dir_path <- dirname(sf)
      dir_path_normalized <- normalizePath(dir_path, winslash = "/", mustWork = FALSE)
      
      # Also try to extract measurement filenames and velocity from session.xml
      doc_athlete <- tryCatch(read_xml_robust(sf), error = function(e) NULL)
      if (!is.null(doc_athlete)) {
        root_athlete <- xml_root(doc_athlete)
        measurements <- xml_find_all(root_athlete, ".//Measurement")
        if (length(measurements) > 0) {
          for (meas in measurements) {
            meas_filename <- xml_attr(meas, "Filename")
            if (!is.na(meas_filename) && meas_filename != "") {
              owner_mapping[[meas_filename]] <- athlete_info
              meas_no_ext <- tools::file_path_sans_ext(meas_filename)
              owner_mapping[[meas_no_ext]] <- athlete_info
              
              # Extract velocity from Comments
              meas_fields <- xml_find_first(meas, "./Fields")
              if (!inherits(meas_fields, "xml_missing")) {
                comments <- xml_text(xml_find_first(meas_fields, "./Comments"))
                if (!is.na(comments) && comments != "" && trimws(comments) != "") {
                  # Try to parse as numeric (velocity)
                  velocity <- suppressWarnings(as.numeric(trimws(comments)))
                  if (!is.na(velocity)) {
                    # Store velocity mapped by filename without extension (to match .c3d files)
                    velocity_mapping[[meas_no_ext]] <- velocity
                    cat("    Found velocity", velocity, "for", meas_filename, "\n")
                  }
                }
              }
            }
          }
        }
      }
      
      owner_mapping[[dir_path_normalized]] <- athlete_info
      athlete_list[[length(athlete_list) + 1]] <- athlete_info
    }
  }
  
  # Process session_data.xml files
  all_pitch_data <- list()
  
  for (sdf in session_data_files) {
    cat("Processing data from:", sdf, "\n")
    doc <- tryCatch(read_xml_robust(sdf), error = function(e) {
      cat("Error reading", sdf, ":", conditionMessage(e), "\n")
      NULL
    })
    if (is.null(doc)) next
    
    root <- xml_root(doc)
    if (!identical(xml_name(root), "v3d")) next
    
    owners <- xml_find_all(root, "./owner")
    owner_names <- xml_attr(owners, "value")
    
    # Try to match owner to athlete
    dir_path <- dirname(sdf)
    dir_path_normalized <- normalizePath(dir_path, winslash = "/", mustWork = FALSE)
    
    for (owner_name in owner_names) {
      # Skip Static trials and only process Fastball trials
      owner_base <- basename(owner_name)
      if (grepl("Static", owner_name, ignore.case = TRUE)) {
        cat("    Skipping Static trial:", owner_name, "\n")
        next
      }
      if (!grepl("Fastball", owner_name, ignore.case = TRUE)) {
        cat("    Skipping non-Fastball trial:", owner_name, "\n")
        next
      }
      
      matched_athlete <- NULL
      
      # Extract base name from owner (already extracted above)
      owner_no_ext <- tools::file_path_sans_ext(owner_base)
      
      # STRICT MATCHING: Only match based on directory location
      # Owner names are NOT unique across directories, so we MUST match by directory only
      
      matched_athlete <- NULL
      
      # 1. First check if we've already mapped this directory
      if (dir_path_normalized %in% names(owner_mapping)) {
        matched_athlete <- owner_mapping[[dir_path_normalized]]
        cat("    Matched owner", owner_name, "via directory path (already mapped)\n")
      } else {
        # 2. Look for session.xml in the EXACT same directory as session_data.xml
        session_xml_path <- file.path(dir_path, "session.xml")
        if (file.exists(session_xml_path)) {
          athlete_info <- extract_athlete_info(session_xml_path)
          if (!is.null(athlete_info) && nrow(athlete_info) > 0) {
            matched_athlete <- athlete_info
            # Add to mapping for future owners in this directory
            owner_mapping[[dir_path_normalized]] <- athlete_info
            athlete_list[[length(athlete_list) + 1]] <- athlete_info
            
            # Also extract velocity data from this session.xml
            doc_athlete <- tryCatch(read_xml_robust(session_xml_path), error = function(e) NULL)
            if (!is.null(doc_athlete)) {
              root_athlete <- xml_root(doc_athlete)
              measurements <- xml_find_all(root_athlete, ".//Measurement")
              if (length(measurements) > 0) {
                for (meas in measurements) {
                  meas_filename <- xml_attr(meas, "Filename")
                  if (!is.na(meas_filename) && meas_filename != "") {
                    meas_no_ext <- tools::file_path_sans_ext(meas_filename)
                    
                    # Extract velocity from Comments
                    meas_fields <- xml_find_first(meas, "./Fields")
                    if (!inherits(meas_fields, "xml_missing")) {
                      comments <- xml_text(xml_find_first(meas_fields, "./Comments"))
                      if (!is.na(comments) && comments != "" && trimws(comments) != "") {
                        velocity <- suppressWarnings(as.numeric(trimws(comments)))
                        if (!is.na(velocity)) {
                          velocity_mapping[[meas_no_ext]] <- velocity
                        }
                      }
                    }
                  }
                }
              }
            }
            
            cat("    Found and matched session.xml in same directory:", basename(dir_path), "\n")
          }
        }
      }
      
      # If no match found, skip this owner (don't use fallback matching)
      if (is.null(matched_athlete)) {
        cat("  WARNING: Could not match owner", owner_name, "to any athlete\n")
        cat("    Directory:", dir_path_normalized, "\n")
        cat("    No session.xml found in this directory - SKIPPING\n")
        next
      }
      
      # Extract time series data
      cat("    Extracting time series data for owner:", owner_name, "\n")
      ts_data <- extract_time_series_data(doc, owner_name)
      cat("      Found", nrow(ts_data), "time series rows\n")
      
      # Extract event labels
      cat("    Extracting event labels for owner:", owner_name, "\n")
      event_data <- extract_event_labels(doc, owner_name)
      cat("      Found", nrow(event_data), "event rows\n")
      
      if (nrow(ts_data) == 0 && nrow(event_data) == 0) {
        cat("  No data found for owner", owner_name, "\n")
        next
      }
      
      # Calculate Pitching_Shoulder_Accel from Pitching_Shoulder_Ang_Vel Z component
      if (nrow(ts_data) > 0 && "variable" %in% names(ts_data)) {
        shoulder_vel_z <- ts_data %>%
          filter(variable == "Pitching_Shoulder_Ang_Vel", component == "Z")
        
        if (nrow(shoulder_vel_z) > 0) {
        # Extract values
        value_cols <- grep("^value_\\d+$", names(shoulder_vel_z), value = TRUE)
        if (length(value_cols) > 0) {
          vel_values <- as.numeric(shoulder_vel_z[1, value_cols])
          acc_values <- calculate_acceleration(vel_values, fs = 240)
          
          # Create acceleration row
          acc_row <- shoulder_vel_z[1, ]
          acc_row$variable <- "Pitching_Shoulder_Accel"
          acc_row$component <- "Z"
          
          # Replace values
          for (i in seq_along(value_cols)) {
            acc_row[[value_cols[i]]] <- acc_values[i]
          }
          
          ts_data <- bind_rows(ts_data, acc_row)
        }
        }
      }
      
      # Calculate derived metrics
      if (nrow(event_data) > 0 && nrow(ts_data) > 0) {
        derived_metrics <- calculate_derived_metrics(ts_data, event_data)
      } else {
        derived_metrics <- tibble(
          max_ir_velocity = NA_real_,
          max_ir_time = NA_real_,
          time_max_ir_to_mer = NA_real_,
          time_max_ir_to_rel = NA_real_
        )
      }
      
      # Combine athlete info with data - ONE ROW PER VARIABLE-COMPONENT COMBINATION
      if (nrow(ts_data) > 0) {
        # Get velocity for this owner (match by filename without extension)
        owner_base <- basename(owner_name)
        owner_no_ext <- tools::file_path_sans_ext(owner_base)
        velocity <- if (owner_no_ext %in% names(velocity_mapping)) {
          velocity_mapping[[owner_no_ext]]
        } else {
          NA_real_
        }
        
        # Create base athlete info that will be repeated for each variable
        base_info <- matched_athlete %>%
          mutate(
            owner = owner_name,
            velocity = velocity,
            source_data_file = basename(sdf),
            source_data_path = sdf
          )
        
        # Add event times to base info
        if (nrow(event_data) > 0) {
          for (evt in c("Footstrike", "Max_Shoulder_Rot", "Release", "Release100msAfter")) {
            evt_time <- event_data %>% filter(event == evt) %>% pull(time) %>% first()
            base_info[[paste0("event_", tolower(evt), "_time")]] <- if (is.na(evt_time)) NA_real_ else evt_time
          }
        } else {
          base_info$event_footstrike_time <- NA_real_
          base_info$event_max_shoulder_rot_time <- NA_real_
          base_info$event_release_time <- NA_real_
          base_info$event_release100msafter_time <- NA_real_
        }
        
        # Add derived metrics to base info
        if (nrow(derived_metrics) > 0) {
          base_info <- bind_cols(base_info, derived_metrics)
        } else {
          base_info$max_ir_velocity <- NA_real_
          base_info$max_ir_time <- NA_real_
          base_info$time_max_ir_to_mer <- NA_real_
          base_info$time_max_ir_to_rel <- NA_real_
        }
        
        # Create one row per variable-component combination
        for (i in seq_len(nrow(ts_data))) {
          ts_row <- ts_data[i, ]
          
          # Combine base info with this variable's data
          var_row <- bind_cols(
            base_info,
            ts_row %>% dplyr::select(variable, component, frames, time_start, time_end, starts_with("value_"))
          )
          
          all_pitch_data[[length(all_pitch_data) + 1]] <- var_row
        }
      }
    }
  }
  
  if (length(all_pitch_data) == 0) {
    cat("No data extracted. Creating empty database.\n")
    # Create empty database
    con <- DBI::dbConnect(RSQLite::SQLite(), OUTPUT_DB)
    empty_df <- tibble(
      athlete_id = character(),
      name = character(),
      date_of_birth = character(),
      age = numeric(),
      age_at_collection = numeric(),
      gender = character(),
      height = numeric(),
      weight = numeric(),
      creation_date = character(),
      creation_time = character(),
      source_file = character(),
      source_path = character(),
      owner = character(),
      velocity = numeric(),
      source_data_file = character(),
      source_data_path = character()
    )
    DBI::dbWriteTable(con, "pitches", empty_df, overwrite = TRUE)
    DBI::dbDisconnect(con)
    return(invisible(NULL))
  }
  
  # Find max frames across all rows to ensure consistent column structure
  max_frames <- 0
  for (df in all_pitch_data) {
    if (nrow(df) > 0 && "frames" %in% names(df)) {
      max_frames <- max(max_frames, max(df$frames, na.rm = TRUE), na.rm = TRUE)
    }
  }
  cat("Maximum frames across all variables:", max_frames, "\n")
  
  # Pad all dataframes to max_frames before binding
  padded_list <- list()
  for (df in all_pitch_data) {
    if (nrow(df) > 0) {
      # Add missing value columns with NA
      for (i in 1:max_frames) {
        value_col <- paste0("value_", i)
        if (!value_col %in% names(df)) {
          df[[value_col]] <- NA_real_
        }
      }
      
      # Ensure value columns are in order
      value_cols <- paste0("value_", 1:max_frames)
      meta_cols <- setdiff(names(df), value_cols)
      df <- df %>% select(any_of(c(meta_cols, value_cols)))
      
      padded_list[[length(padded_list) + 1]] <- df
    }
  }
  
  # Combine all data
  final_df <- bind_rows(padded_list)
  
  # Write to SQLite database
  cat("\nWriting data to database:", OUTPUT_DB, "\n")
  cat("Total columns:", ncol(final_df), "\n")
  con <- DBI::dbConnect(RSQLite::SQLite(), OUTPUT_DB)
  DBI::dbWriteTable(con, "pitches", final_df, overwrite = TRUE)
  
  # Create indexes
  tryCatch({
    DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_pitches_owner ON pitches(owner)")
    DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_pitches_name ON pitches(name)")
    cat("  Created indexes\n")
  }, error = function(e) {
    cat("  Warning: Could not create indexes:", conditionMessage(e), "\n")
  })
  
  DBI::dbDisconnect(con)
  cat("Data written successfully!\n")
  cat("Total rows:", nrow(final_df), "\n")
  cat("Total columns:", ncol(final_df), "\n")
  
  invisible(final_df)
}

# ---------- Run ----------
cat("Starting pitching acceleration data extraction...\n")
cat("Output database:", OUTPUT_DB, "\n")
cat("Data root:", if (is.null(DATA_ROOT)) "Current directory (Pitching folder)" else DATA_ROOT, "\n\n")

tryCatch({
  process_all_files()
  cat("\nProcessing completed successfully!\n")
}, error = function(e) {
  cat("\nError during processing:\n")
  cat(conditionMessage(e), "\n")
  traceback()
})

