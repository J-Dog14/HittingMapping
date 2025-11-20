# ==== Minimal deps ====
library(xml2)
library(purrr)
library(dplyr)
library(readr)
library(stringr)
library(tibble)
library(tidyr)
library(fs)
library(tools)

# Root should be the "xml Data" folder
candidates <- c(
  path_expand("~/Desktop/Pitching Data/xml Data/"),
  fs::path(wd, "Pitching Data", "xml Data"),  # optional variant
  wd
)

root_dir <- candidates[dir_exists(candidates)][1]
if (is.na(root_dir)) stop("No viable root directory found.")
message("Root scan dir: ", root_dir)

xml_files <- unique(c(
  dir_ls(
    root_dir,
    recurse = TRUE,
    type = "file",
    glob = "**/Baseball Right-handed/*.xml"
  ),
  dir_ls(
    root_dir,
    recurse = TRUE,
    type = "file",
    glob = "**/Baseball Left-handed/*.xml"
  ),
  dir_ls(
    root_dir,
    recurse = TRUE,
    type = "file",
    glob = "**/Baseball Right-handed/*.xml.gz"
  ),
  dir_ls(
    root_dir,
    recurse = TRUE,
    type = "file",
    glob = "**/Baseball Left-handed/*.xml.gz"
  )
))


message("Found ", length(xml_files), " XML files under scan root: ", root_dir)
if (!length(xml_files)) stop("No XML files found under: ", root_dir)




root_dir <- candidates[dir_exists(candidates)][1]
if (is.na(root_dir)) stop("No viable root directory found.")
message("Root scan dir: ", root_dir)
# Where to write the CSV outputs
out_dir <- fs::path(root_dir, "exports")   # saves alongside your data, in a new "exports" folder
fs::dir_create(out_dir)                    # makes sure the folder exists


# ---------- helpers ----------
`%||%` <- function(a, b) if (!is.null(a)) a else b
nzchr <- function(x) ifelse(is.na(x) | x == "", NA_character_, x)

# Robust XML reader; supports .xml and .xml.gz; tolerates trailing garbage after </Subject>
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
      } else stop(e)
    }
  )
}

fallback_from_path <- function(path) {
  d1 <- dirname(path)                 # e.g., ".../Baseball Right-handed"
  b1 <- basename(d1)
  # If the parent starts with "Baseball", go one more level up to get the athlete
  if (grepl("^baseball", b1, ignore.case = TRUE)) {
    return(basename(dirname(d1)))     # e.g., "Christian Baker"
  }
  # Otherwise just use the immediate parent
  basename(d1)
}

hand_folder_from_path <- function(path) {
  basename(dirname(path))             # e.g., "Baseball Right-handed"
}



# Name normalization: "Last, First M" -> "First M Last" (vectorized)
normalize_person_name_one <- function(s) {
  s <- nzchr(s); if (is.na(s)) return(NA_character_)
  had_comma <- grepl(",", s, fixed = TRUE)
  s <- stringr::str_squish(gsub(",", " ", s, fixed = TRUE))
  parts <- strsplit(s, "\\s+")[[1]]
  out <- if (length(parts) >= 2 && had_comma) paste(paste(parts[-1], collapse = " "), parts[1]) else s
  stringr::str_to_title(out)
}
normalize_person_name <- Vectorize(normalize_person_name_one, USE.NAMES = FALSE)

# --- Vectorized date helpers (age as of TODAY) ---
parse_date_flex <- function(x) {
  x <- nzchr(x)
  n <- length(x)
  out <- rep(as.Date(NA), n)
  if (!n) return(out)
  fmts <- c("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y", "%d-%m-%Y")
  for (f in fmts) {
    idx <- is.na(out) & !is.na(x)
    if (!any(idx)) break
    parsed <- suppressWarnings(as.Date(x[idx], format = f))
    fill_idx <- which(idx)[!is.na(parsed)]
    if (length(fill_idx)) out[fill_idx] <- parsed[!is.na(parsed)]
  }
  idx <- is.na(out) & !is.na(x)
  if (any(idx)) {
    parsed <- suppressWarnings(as.Date(x[idx]))
    fill_idx <- which(idx)[!is.na(parsed)]
    if (length(fill_idx)) out[fill_idx] <- parsed[!is.na(parsed)]
  }
  out
}


# ---------- KEY METRICS (YOUR LIST) ----------
metrics_xyz <- c(
  "Pelvis_Angle@MaxKneeHeight",
  "Pelvis_Angle@Footstrike",
  "Pelvis_Angle@Max_Shoulder_Rot",
  "Pelvis_Angle@PelvisRot_Stop",
  "Pelvis_Angle@Rel",
  "Trunk_Angle@MaxKneeHeight",
  "Trunk_Angle@Footstrike",
  "Trunk_Angle@Max_Shoulder_Rot",
  "Trunk_Angle@PelvisRot_Stop",
  "Trunk_Angle@Release",
  "Trunk_wrt_Pelvis_FE@Footstrike",
  "Trunk_wrt_Pelvis_FE@Max_Shoulder_Rot",
  "Trunk_wrt_Pelvis_FE@Release"
)
metrics_x <- c(
  "Trunk Rot wrt Pelvis Rot@Footstrike",
  "Trunk Rot wrt Pelvis Rot@Max_Shoulder_Rot",
  "Trunk Rot wrt Pelvis Rot@Release",
  "Pitching_Shoulder_Angle@Footstrike"
)
metrics_z <- c("Pitching_Shoulder_Angle@Max_Shoulder_Rot")
all_metrics <- c(metrics_xyz, metrics_x, metrics_z)

needed_components <- function(metric) {
  if (metric %in% metrics_xyz) return(c("X","Y","Z"))
  if (metric %in% metrics_x)   return(c("X"))
  if (metric %in% metrics_z)   return(c("Z"))
  character(0)
}

# ---------- STRICT label canonicalization (SAFE LOOKUPS) ----------
normalize_label <- function(s) {
  s %>% tolower() %>%
    stringr::str_replace_all("[\n\r\t]", " ") %>%
    stringr::str_replace_all("[ _-]+", "_") %>%
    stringr::str_replace_all("[^a-z0-9@_]", "") %>%
    stringr::str_trim()
}
.collapse <- function(s) gsub("_", "", s, fixed = TRUE)

# --- Measure map ---
.measure_map <- c(
  "Pelvis_Ang_Acc",
  "Pelvis_Angle",
  "Trunk_Angle",
  "Trunk_wrt_Pelvis_FE",
  "Trunk Rot wrt Pelvis Rot",
  "Pitching_Shoulder_Angle"
)
names(.measure_map) <- .collapse(c(
  "pelvis_ang_acc",
  "pelvis_angle",
  "trunk_angle",
  "trunk_wrt_pelvis_fe",
  "trunk_rot_wrt_pelvis_rot",
  "pitching_shoulder_angle"
))

.event_map <- c(
  "footstrike"            = "Footstrike",
  "foot_strike"           = "Footstrike",
  "lead_foot_down"        = "Footstrike",
  "contact"               = "Footstrike",
  "max_knee_height"       = "MaxKneeHeight",
  "maxkneehigh"           = "MaxKneeHeight",
  "maxkneehight"          = "MaxKneeHeight",
  "maxkneeheight"         = "MaxKneeHeight",   # add this
  "max_shoulder_rot"      = "Max_Shoulder_Rot",
  "max_shoulder_rotation" = "Max_Shoulder_Rot",
  "pelvisrot_stop"        = "PelvisRot_Stop",
  "pelvis_rotation_stop"  = "PelvisRot_Stop",
  "release"               = "Release",
  "rel"                   = "Release"
)





.split_label <- function(nl) {
  parts <- strsplit(nl, "@", fixed = TRUE)[[1]]
  if (length(parts) != 2) return(NULL)
  list(measure = parts[1], event = parts[2])
}

canonical_from_label <- function(raw_label) {
  nl <- normalize_label(raw_label)
  se <- .split_label(nl)
  if (is.null(se)) return(NA_character_)
  
  meas_key   <- .collapse(se$measure)
  meas_canon <- unname(.measure_map[meas_key])
  if (length(meas_canon) == 0 || is.na(meas_canon)) return(NA_character_)
  
  evt_key    <- se$event
  evt_canon  <- unname(.event_map[evt_key])
  if (length(evt_canon) == 0 || is.na(evt_canon)) {
    evt_canon <- unname(.event_map[.collapse(evt_key)])
  }
  if (length(evt_canon) == 0 || is.na(evt_canon)) return(NA_character_)
  
  if (meas_canon == "Pelvis_Angle" && evt_canon == "Release") evt_canon <- "Rel"
  
  candidate <- paste0(meas_canon, "@", evt_canon)
  if (candidate %in% all_metrics) candidate else NA_character_
}

extract_metric_occurrences <- function(path) {
  doc <- read_xml_robust(path)
  
  subject_key <- fallback_from_path(path)       # "Christian Baker"
  folder_val  <- hand_folder_from_path(path)    # "Baseball Right-handed"
  
  name_nodes <- xml_find_all(doc, ".//type[@value='METRIC']//name[@value]")
  if (!length(name_nodes)) name_nodes <- xml_find_all(doc, ".//name[@value]")
  
  purrr::imap_dfr(name_nodes, function(nn, idx_in_file) {
    raw_label <- xml_attr(nn, "value") %||% NA_character_
    if (is.na(raw_label)) return(tibble())
    
    canonical <- canonical_from_label(raw_label)
    if (is.na(canonical)) return(tibble())
    
    comps_needed <- needed_components(canonical)
    comp_nodes   <- xml_find_all(nn, ".//component")
    
    comp_df <- purrr::map_dfr(comp_nodes, function(cc) {
      comp <- xml_attr(cc, "value") %||% "X"
      val  <- xml_attr(cc, "data") %||% xml_text(cc)
      tibble(component = comp, value = suppressWarnings(as.numeric(val)))
    }) %>% filter(component %in% comps_needed)
    
    if (!nrow(comp_df)) comp_df <- tibble(component = comps_needed, value = NA_real_)
    
    wide <- comp_df %>%
      distinct(component, .keep_all = TRUE) %>%
      tidyr::pivot_wider(names_from = component, values_from = value)
    
    for (ax in comps_needed) if (!ax %in% names(wide)) wide[[ax]] <- NA_real_
    
    wide %>%
      mutate(
        subject_key = subject_key,
        file_path   = path,
        folder      = folder_val,                 # directory name, not XML "PROCESSED"
        metric      = canonical,
        raw_label   = raw_label,
        occurrence_in_file_seq = idx_in_file
      ) %>%
      relocate(subject_key, file_path, folder, metric, raw_label, occurrence_in_file_seq)
  })
}


# ---------- find XMLs under root_dir (recursive) ----------
xml_files <- unique(c(
  dir_ls(root_dir, recurse = TRUE, type = "file", glob = "**/*.xml"),
  dir_ls(root_dir, recurse = TRUE, type = "file", glob = "**/*.xml.gz")
))
message("Found ", length(xml_files), " XML files under scan root.")
if (!length(xml_files)) stop("No XML files found under: ", root_dir)

# ---------- Audit labels ----------
label_audit <- purrr::map_dfr(xml_files, function(p) {
  doc <- read_xml_robust(p)
  name_nodes <- xml_find_all(doc, ".//type[@value='METRIC']//name[@value]")
  if (!length(name_nodes)) name_nodes <- xml_find_all(doc, ".//name[@value]")
  tibble(
    file_path = p,
    raw_label = purrr::map_chr(name_nodes, ~ xml_attr(.x, "value") %||% NA_character_)
  ) %>% dplyr::filter(!is.na(raw_label))
}) %>%
  dplyr::mutate(canonical = purrr::map_chr(raw_label, canonical_from_label))

matched   <- label_audit %>% dplyr::filter(!is.na(canonical))
unmatched <- label_audit %>% dplyr::filter(is.na(canonical))
message("Label audit: ", nrow(matched), " matched; ", nrow(unmatched), " unmatched across ",
        dplyr::n_distinct(label_audit$file_path), " files.")

# ---------- Split session and session_data files ----------
session_files      <- xml_files[grepl("session(?!_data).*\\.xml$", xml_files, ignore.case = TRUE, perl = TRUE)]
session_data_files <- setdiff(xml_files, session_files)


players <- purrr::map_dfr(session_files, function(p) {
  doc <- read_xml_robust(p)
  tibble(
    subject_key      = fallback_from_path(p),                          # "Christian Baker"
    subject_id       = nzchr(xml_text(xml_find_first(doc, "//Subject/Fields/ID"))),
    subject_name_raw = nzchr(xml_text(xml_find_first(doc, "//Name"))),
    creation_date    = nzchr(xml_text(xml_find_first(doc, "//Subject/Fields/Creation_date"))),
    Date_of_birth    = nzchr(xml_text(xml_find_first(doc, "//Subject/Fields/Date_of_birth")))
  )
}) %>%
  distinct(subject_key, .keep_all = TRUE) %>%                           # one row per athlete
  mutate(
    athlete  = normalize_person_name(coalesce(subject_name_raw, subject_id, subject_key)),
    dob_date = parse_date_flex(Date_of_birth),
    creation = parse_date_flex(creation_date)
  )

# Helper: compute age in years at a reference date
age_at_date <- function(dob, ref_date) {
  out <- rep(NA_real_, length(dob))
  ok  <- !is.na(dob) & !is.na(ref_date)
  out[ok] <- floor(as.numeric(difftime(ref_date[ok], dob[ok], units = "days")) / 365.25)
  out
}


occ <- purrr::map_dfr(session_data_files,
                      purrr::possibly(extract_metric_occurrences, otherwise = tibble()))

occ <- occ %>%
  left_join(players, by = "subject_key") %>%
  mutate(
    athlete    = coalesce(athlete, subject_key),
    age_years  = age_at_date(dob_date, creation),
    age_bucket = case_when(
      is.na(age_years) ~ NA_character_,
      age_years < 18   ~ "<18",
      age_years <= 22  ~ "18-22",
      TRUE             ~ "22+"
    )
  )



# ---------- (C) OCCURRENCE MATRIX (align rows by per-metric occurrence index) ----------
# Make a per-metric sequence so that row 1 = first occurrence of EVERY metric, row 2 = second occurrence, etc.
occ <- occ %>%
  dplyr::arrange(subject_key, file_path, metric, occurrence_in_file_seq) %>%
  dplyr::group_by(subject_key, file_path, metric) %>%
  dplyr::mutate(metric_seq = dplyr::row_number()) %>%
  dplyr::ungroup()

# Desired metric-component columns (ensure presence & ordering)
desired_metric_cols <- unlist(lapply(all_metrics, function(m) {
  paste0(m, "_", needed_components(m))
}), use.names = FALSE)

# Components actually present in 'occ'
comp_cols <- intersect(c("X","Y","Z"), names(occ))

# Long → keep only components required for each metric
occ_long <- occ %>%
  dplyr::select(subject_key, subject_id, subject_name_raw, athlete, Date_of_birth,
                dob_date, age_years, age_bucket, creation_date,   # <- add here
                file_path, folder, metric, metric_seq, dplyr::all_of(comp_cols)) %>%
  tidyr::pivot_longer(
    cols = dplyr::all_of(comp_cols),
    names_to = "component", values_to = "value",
    values_drop_na = FALSE
  ) %>%
  dplyr::rowwise() %>%
  dplyr::filter(component %in% needed_components(metric)) %>%
  dplyr::ungroup()


# Wide matrix where each row index (metric_seq) holds the k-th occurrence across ALL metrics
occ_matrix <- occ_long %>%
  tidyr::pivot_wider(
    id_cols   = c(subject_key, subject_id, subject_name_raw, athlete, Date_of_birth,
                  dob_date, age_years, age_bucket, file_path, folder, metric_seq, creation_date),
    names_from  = c(metric, component),
    values_from = value,
    names_sep   = "_"
  )

# Ensure ALL desired metric-component columns exist and are ordered
missing_cols <- setdiff(desired_metric_cols, names(occ_matrix))
for (mc in missing_cols) occ_matrix[[mc]] <- NA_real_

id_cols <- c("athlete","subject_id","subject_name_raw","Date_of_birth",
             "age_years","age_bucket","file_path","folder","metric_seq", "creation_date")
other_cols <- setdiff(names(occ_matrix), c(id_cols, desired_metric_cols, "subject_key", "dob_date"))

occ_matrix <- occ_matrix %>%
  dplyr::select(dplyr::any_of(id_cols),
                dplyr::any_of(desired_metric_cols),
                dplyr::any_of(other_cols)) %>%
  dplyr::arrange(file_path, metric_seq)



cat("Wrote age-bucketed matrices:\n")
# Define helper
write_age_csv <- function(df, label) {
  out_path <- fs::path(out_dir, paste0("key_metric_occurrence_matrix_", label, ".csv"))
  readr::write_csv(df, out_path, na = "")
  message("  - ", out_path)
}
# <18
write_age_csv(occ_matrix %>% dplyr::filter(age_bucket == "<18"), "under18")

# 18–22
write_age_csv(occ_matrix %>% dplyr::filter(age_bucket == "18-22"), "18_22")

# 22+
write_age_csv(occ_matrix %>% dplyr::filter(age_bucket == "22+"), "22plus")

# All
write_age_csv(occ_matrix, "all")


handed_dirs <- c("Baseball Right-handed", "Baseball Left-handed")

# Immediate children of root_dir are the athlete folders
athlete_dirs <- fs::dir_ls(root_dir, type = "directory", recurse = FALSE)

# Helper: list only session/session_data files (case-insensitive) in a non-recursive subdir
list_session_files <- function(dir) {
  if (!fs::dir_exists(dir)) return(character())
  files <- fs::dir_ls(dir, type = "file", recurse = FALSE)
  # keep only session.xml / session_data.xml (+ .gz), case-insensitive
  keep <- tolower(fs::path_file(files)) %in%
    c("session.xml", "session_data.xml", "session.xml.gz", "session_data.xml.gz")
  files[keep]
}

# Build the exact targets: <root>/<Athlete>/<Baseball ...>/(session|session_data).xml(.gz)
xml_files <- unlist(lapply(athlete_dirs, function(ad) {
  unlist(lapply(handed_dirs, function(hd) {
    list_session_files(fs::path(ad, hd))
  }), use.names = FALSE)
}), use.names = FALSE)

xml_files <- unique(xml_files)

message("Found ", length(xml_files), " session(.gz) files at exactly two levels under: ", root_dir)
if (!length(xml_files)) stop("No target session files found under: ", root_dir)

