# ==== Minimal deps ====
library(xml2)
library(purrr)
library(dplyr)
library(readr)
library(stringr)
library(tibble)
library(tidyr)
library(tools)

# ---------- helpers ----------
`%||%` <- function(a, b) if (!is.null(a)) a else b
nzchr <- function(x) ifelse(is.na(x) | x == "", NA_character_, x)
nznum <- function(x) suppressWarnings(readr::parse_number(x))
safe_name <- function(x) {
  x %>%
    str_replace_all("@", "_at_") %>%
    str_replace_all("[^A-Za-z0-9]+", "_") %>%
    str_replace_all("^_+|_+$", "") %>%
    tolower()
}
relpath_of <- function(path, root) {
  sub(
    paste0("^", normalizePath(root, winslash = "/", mustWork = TRUE), "/?"),
    "",
    normalizePath(path, winslash = "/", mustWork = FALSE)
  )
}
clean_player <- function(s) {
  ifelse(
    is.na(s), NA_character_,
    s %>% str_replace_all(",", " ") %>% str_squish() %>% str_replace("[ ]?\\d{2}[–-]\\d{2}$", "")
  )
}
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
      } else stop(e)
    }
  )
}
ensure_one_row <- function(df) if (nrow(df)) df[1, , drop = FALSE] else tibble(.rows = 1)
first_non_missing <- function(x) {
  if (is.numeric(x)) {
    idx <- which(!is.na(x)); if (length(idx)) x[idx[1]] else NA_real_
  } else if (inherits(x, "POSIXt")) {
    idx <- which(!is.na(x)); if (length(idx)) x[idx[1]] else as.POSIXct(NA)
  } else if (is.logical(x)) {
    idx <- which(!is.na(x)); if (length(idx)) x[idx[1]] else NA
  } else {
    idx <- which(!is.na(x) & x != ""); if (length(idx)) x[idx[1]] else NA_character_
  }
}
is_all_missing <- function(x) all(is.na(x) | x == "")

get_player_raw <- function(path) {
  nm_xml <- tryCatch({
    doc <- read_xml(path)
    nm  <- xml_text(xml_find_first(doc, "/Subject/Fields/Name"))
    if (!is.na(nm) && nzchar(nm)) nm else NA_character_
  }, error = function(e) NA_character_)
  if (!is.na(nm_xml) && nzchar(nm_xml)) return(nm_xml)
  m <- regexec(".*/[Xx][Mm][Ll][ ]+[Dd]ata/([^/]+)/", path)
  hit <- tryCatch(regmatches(path, m), error = function(e) list())
  nm_path <- if (length(hit) && length(hit[[1]]) >= 2) hit[[1]][2] else NA_character_
  if (!is.na(nm_path) && nzchar(nm_path)) return(nm_path)
  parent <- basename(dirname(path))
  if (!is.na(parent) && nzchar(parent) && parent != "." && parent != "/") return(parent)
  stem <- file_path_sans_ext(file_path_sans_ext(basename(path)))
  if (nzchar(stem)) stem else NA_character_
}

detect_folder_type <- function(relpath) {
  p <- str_detect(relpath, "(?i)/processed/")
  o <- str_detect(relpath, "(?i)/original/")
  type <- dplyr::case_when(p ~ "processed", o ~ "original", TRUE ~ "other")
  tibble(folder_type = type, folder_type_processed = p, folder_type_original = o)
}

# ---------- Subject <Fields>: explicit core + generic (keeps exact tag names) ----------
.extract_fields_tagblock_exact <- function(fields_node) {
  if (inherits(fields_node, "xml_missing") || xml_length(fields_node) == 0) return(tibble(.rows = 1))
  kids <- xml_children(fields_node); if (!length(kids)) return(tibble(.rows = 1))
  lab <- xml_name(kids)                            # keep exact names (e.g., Date_of_birth)
  val <- purrr::map_chr(kids, ~ nzchr(trimws(xml_text(.x))))
  tibble(label = lab, value = val) %>%
    group_by(label) %>%
    summarise(value = dplyr::first(value[!is.na(value)]), .groups = "drop") %>%
    suppressMessages(tidyr::pivot_wider(names_from = label, values_from = value))
}

extract_subject_fields <- function(doc) {
  root <- xml_root(doc)
  if (!identical(xml_name(root), "Subject")) return(tibble(.rows = 1))
  
  out <- tibble(
    subject_type     = xml_attr(root, "Type") %||% NA_character_,
    subject_filename = xml_attr(root, "Filename") %||% NA_character_
  )
  
  # Explicit core fields (guarantee presence when in XML)
  core <- tibble(
    ID             = nzchr(xml_text(xml_find_first(root, "./Fields/ID"))),
    Name           = nzchr(xml_text(xml_find_first(root, "./Fields/Name"))),
    Date_of_birth  = nzchr(xml_text(xml_find_first(root, "./Fields/Date_of_birth"))),
    Height         = nzchr(xml_text(xml_find_first(root, "./Fields/Height"))),
    Weight         = nzchr(xml_text(xml_find_first(root, "./Fields/Weight"))),
    Creation_date  = nzchr(xml_text(xml_find_first(root, "./Fields/Creation_date"))),
    Creation_time  = nzchr(xml_text(xml_find_first(root, "./Fields/Creation_time")))
  )
  
  # Generic: any other subject fields
  f <- xml_find_first(root, "./Fields")
  extra <- if (!inherits(f, "xml_missing") && xml_length(f) > 0) .extract_fields_tagblock_exact(f) else tibble(.rows = 1)
  
  # Merge with core taking precedence
  merged <- bind_cols(core, extra %>% select(-any_of(names(core))))
  
  # Used==True across measurements (convenience)
  used_count <- sum(tolower(trimws(xml_text(xml_find_all(root, ".//Measurement/Fields/Used")))) == "true", na.rm = TRUE)
  merged$measurements_used_true_count <- used_count
  
  bind_cols(out, ensure_one_row(merged))
}

# ---------- Session <Fields> (first session or Baseball Hitting Sports) ----------
extract_session_fields <- function(doc) {
  root <- xml_root(doc)
  if (!identical(xml_name(root), "Subject")) return(tibble(.rows = 1))
  
  sessions <- xml_find_all(root, "./Session")
  if (!length(sessions)) return(tibble(.rows = 1))
  
  ix <- which(str_detect(xml_attr(sessions, "Type") %||% "", regex("Baseball Hitting Sports", ignore_case = TRUE)))
  sess <- if (length(ix)) sessions[[ix[1]]] else sessions[[1]]
  
  s_type <- xml_attr(sess, "Type") %||% NA_character_
  s_file <- xml_attr(sess, "Filename") %||% NA_character_
  s_fields <- xml_find_first(sess, "./Fields")
  s_df <- if (!inherits(s_fields, "xml_missing")) .extract_fields_tagblock_exact(s_fields) else tibble(.rows = 1)
  if (ncol(s_df)) names(s_df) <- paste0("Session_", names(s_df))
  
  tibble(Session_Type = s_type, Session_Filename = s_file) %>% bind_cols(ensure_one_row(s_df))
}

# ---------- Measurements: capture all <Fields> for each measurement ----------
.read_fields_tagblock <- function(fields_node) {
  if (inherits(fields_node, "xml_missing") || xml_length(fields_node) == 0) return(tibble(.rows = 1))
  kids <- xml_children(fields_node); if (!length(kids)) return(tibble(.rows = 1))
  lab <- xml_name(kids)
  val <- purrr::map_chr(kids, ~ nzchr(trimws(xml_text(.x))))
  tibble(label = lab, value = val) %>%
    group_by(label) %>%
    summarise(value = dplyr::first(value[!is.na(value)]), .groups = "drop") %>%
    suppressMessages(tidyr::pivot_wider(names_from = label, values_from = value))
}

extract_measurement_fields <- function(doc) {
  meas <- xml_find_all(doc, ".//Measurement")
  if (!length(meas)) return(tibble(.rows = 0))
  
  purrr::imap_dfr(meas, function(m, idx) {
    m_type     <- xml_attr(m, "Type") %||% NA_character_
    m_filename <- xml_attr(m, "Filename") %||% NA_character_
    f_df <- .read_fields_tagblock(xml_find_first(m, "./Fields"))
    tibble(
      measurement_index    = idx,
      measurement_type     = m_type,
      measurement_filename = m_filename
    ) %>% bind_cols(ensure_one_row(f_df))
  })
}

measurements_wide_by_index <- function(doc) {
  long <- extract_measurement_fields(doc)
  if (!nrow(long)) return(tibble(.rows = 1))
  id_cols  <- c("measurement_index", "measurement_type", "measurement_filename")
  val_cols <- setdiff(names(long), id_cols)
  
  rows <- purrr::pmap_dfr(long, function(...) {
    row <- tibble::as_tibble(list(...))
    idx <- row$measurement_index[1]
    vals <- dplyr::select(row, all_of(val_cols))
    names(vals) <- paste0("m", idx, "_", names(vals))
    if (idx == 1) {
      vals <- bind_cols(
        tibble(
          first_measurement_type     = row$measurement_type[1],
          first_measurement_filename = row$measurement_filename[1]
        ),
        vals
      )
    }
    vals
  })
  dplyr::summarise(rows, dplyr::across(dplyr::everything(), first_non_missing))
}

# ---------- Measurements: summary rollup (column-existence safe) ----------
extract_measurements_summary <- function(doc) {
  long <- extract_measurement_fields(doc)
  if (!nrow(long)) {
    return(tibble(
      Meas_total = 0L,
      Meas_used_true_count = 0L,
      Meas_static_count = 0L,
      Meas_dynamic_count = 0L,
      Meas_other_count = 0L,
      Meas_first_creation = NA_character_,
      Meas_last_creation  = NA_character_,
      Meas_static_files   = NA_character_,
      Meas_dynamic_files  = NA_character_,
      Meas_other_files    = NA_character_
    ))
  }
  
  n <- nrow(long)
  
  # Safely get columns or fill with NA vectors
  used_raw <- if ("Used" %in% names(long)) long[["Used"]] else rep(NA_character_, n)
  mtype_raw <- if ("Measurement_type" %in% names(long)) long[["Measurement_type"]] else rep(NA_character_, n)
  cdate_raw <- if ("Creation_date" %in% names(long)) long[["Creation_date"]] else rep(NA_character_, n)
  ctime_raw <- if ("Creation_time" %in% names(long)) long[["Creation_time"]] else rep(NA_character_, n)
  
  # Normalize values
  used_norm <- tolower(trimws(nzchr(used_raw)))
  mt_norm   <- tolower(trimws(nzchr(mtype_raw)))
  
  # Categorize types
  mt_cat <- ifelse(mt_norm == "static", "Static",
                   ifelse(mt_norm == "dynamic", "Dynamic", "Other"))
  
  # Datetime
  ct_str <- paste0(nzchr(cdate_raw), " ", nzchr(ctime_raw))
  cts <- suppressWarnings(as.POSIXct(ct_str, format = "%m/%d/%Y %H:%M:%S", tz = "UTC"))
  
  # Masks (avoid NA in any())
  static_mask  <- !is.na(mt_cat) & mt_cat == "Static"
  dynamic_mask <- !is.na(mt_cat) & mt_cat == "Dynamic"
  other_mask   <- !is.na(mt_cat) & mt_cat == "Other"
  
  tibble(
    Meas_total           = n,
    Meas_used_true_count = sum(used_norm == "true", na.rm = TRUE),
    Meas_static_count    = sum(static_mask,  na.rm = TRUE),
    Meas_dynamic_count   = sum(dynamic_mask, na.rm = TRUE),
    Meas_other_count     = sum(other_mask,   na.rm = TRUE),
    Meas_first_creation  = if (all(is.na(cts))) NA_character_ else format(min(cts, na.rm = TRUE), "%Y-%m-%d %H:%M:%S"),
    Meas_last_creation   = if (all(is.na(cts))) NA_character_ else format(max(cts, na.rm = TRUE), "%Y-%m-%d %H:%M:%S"),
    Meas_static_files    = if (isTRUE(any(static_mask)))  paste(unique(long$measurement_filename[static_mask]),  collapse = "; ") else NA_character_,
    Meas_dynamic_files   = if (isTRUE(any(dynamic_mask))) paste(unique(long$measurement_filename[dynamic_mask]), collapse = "; ") else NA_character_,
    Meas_other_files     = if (isTRUE(any(other_mask)))   paste(unique(long$measurement_filename[other_mask]),   collapse = "; ") else NA_character_
  )
}

# ---------- v3d METRIC scalars ----------
extract_v3d_metric_scalars <- function(doc) {
  root <- xml_root(doc)
  if (!identical(xml_name(root), "v3d")) return(tibble(.rows = 1))
  owners <- xml_find_all(root, "./owner")
  if (!length(owners)) return(tibble(.rows = 1))
  
  rows <- purrr::map_dfr(owners, function(own) {
    purrr::map_dfr(xml_find_all(own, "./type[@value='METRIC']"), function(typ) {
      purrr::map_dfr(xml_find_all(typ, "./folder[@value='PROCESSED' or @value='AT_EVENT']"), function(fol) {
        folder_val <- xml_attr(fol, "value")
        purrr::map_dfr(xml_find_all(fol, "./name[@value]"), function(nm) {
          metric <- xml_attr(nm, "value")
          comps  <- xml_find_all(nm, "./component")
          if (!length(comps)) return(tibble())
          tibble(
            folder = folder_val,
            metric = metric,
            axis   = xml_attr(comps, "value"),
            frames = xml_attr(comps, "frames"),
            data   = xml_attr(comps, "data") %||% xml_text(comps)
          )
        })
      })
    })
  })
  
  if (!nrow(rows)) return(tibble(.rows = 1))
  rows <- rows %>%
    mutate(
      axis   = trimws(axis),
      frames = suppressWarnings(as.integer(frames)),
      data   = trimws(data)
    ) %>%
    filter(!is.na(axis) & nzchar(axis),
           !is.na(data) & nzchar(data),
           is.na(frames) | frames == 1L) %>%
    mutate(data_first = sub(",.*$", "", data),
           val = suppressWarnings(readr::parse_number(data_first))) %>%
    filter(!is.na(val)) %>%
    mutate(col = paste0(safe_name(folder), "_", safe_name(metric), "_", safe_name(axis))) %>%
    group_by(col) %>%
    summarise(val = val[1], .groups = "drop")
  
  suppressMessages(tidyr::pivot_wider(rows, names_from = col, values_from = val))
}

# ---------- Builder (all files → per-file rows) ----------
build_all_hitting <- function(root = getwd(), filter_regex = NULL) {
  all_xmls <- list.files(root, pattern = "(?i)\\.xml(\\.gz)?$", recursive = TRUE, full.names = TRUE)
  files <- if (!is.null(filter_regex)) {
    kept <- all_xmls[str_detect(all_xmls, regex(filter_regex, ignore_case = TRUE))]
    if (!length(kept)) {
      message("⚠️ Filter kept 0 files. Falling back to ALL XMLs.")
      all_xmls
    } else kept
  } else all_xmls
  
  cat("Found", length(files), "XML files to process\n")
  if (!length(files)) stop("No XML files found under root")
  
  map_dfr(files, function(path) {
    doc <- tryCatch(read_xml_robust(path), error = function(e) NULL)
    if (is.null(doc)) return(tibble())
    
    rootname <- tryCatch(xml_name(xml_root(doc)), error = function(e) "")
    
    subj   <- if (identical(rootname, "Subject")) extract_subject_fields(doc) else tibble(.rows = 1)
    sess   <- if (identical(rootname, "Subject")) extract_session_fields(doc) else tibble(.rows = 1)
    meas_w <- if (identical(rootname, "Subject")) measurements_wide_by_index(doc) else tibble(.rows = 1)
    meas_s <- if (identical(rootname, "Subject")) extract_measurements_summary(doc) else tibble(.rows = 1)
    v3d    <- if (identical(rootname, "v3d"))     extract_v3d_metric_scalars(doc) else tibble(.rows = 1)
    
    merged <- bind_cols(ensure_one_row(subj), ensure_one_row(sess), ensure_one_row(meas_s),
                        ensure_one_row(v3d), ensure_one_row(meas_w))
    
    player_raw <- get_player_raw(path)
    relpath    <- relpath_of(path, root)
    folder_bits <- detect_folder_type(relpath)
    
    tibble(
      player_raw = player_raw,
      player     = clean_player(player_raw),
      file       = basename(path),
      relpath    = relpath
    ) %>%
      bind_cols(ensure_one_row(folder_bits)) %>%
      bind_cols(ensure_one_row(merged))
  })
}

# ---------- Run ----------
root <- getwd()
ds_all <- build_all_hitting(root)

message("Rows built: ", nrow(ds_all))
message("Unique players (raw): ", length(unique(ds_all$player_raw)))
print(head(ds_all %>% select(player_raw, player, file, relpath), 10))

# ---------- Collapse to one row per athlete ----------
key_col <- if ("player" %in% names(ds_all) && any(nzchar(ds_all$player))) {
  "player"
} else if ("player_raw" %in% names(ds_all)) {
  "player_raw"
} else {
  stop("Neither 'player' nor 'player_raw' columns found in ds_all.")
}

ds_all_aug <- ds_all %>%
  mutate(fullpath = file.path(root, relpath),
         file_mtime = suppressWarnings(as.POSIXct(file.info(fullpath)$mtime, tz = "UTC"))) %>%
  arrange(.data[[key_col]], desc(file_mtime), relpath)

trace_cols    <- c("file", "relpath", "fullpath", "file_mtime")
coalesce_cols <- setdiff(names(ds_all_aug), c(trace_cols, key_col))

ds_per_athlete <- ds_all_aug %>%
  group_by(.data[[key_col]]) %>%
  summarise(
    across(any_of(coalesce_cols), first_non_missing),
    source_files = paste(unique(relpath), collapse = " | "),
    .groups = "drop"
  )

names(ds_per_athlete)[names(ds_per_athlete) == key_col] <- "athlete"

# Ensure numeric for vitals if present
if ("Height" %in% names(ds_per_athlete)) ds_per_athlete <- ds_per_athlete %>% mutate(Height = nznum(Height))
if ("Weight" %in% names(ds_per_athlete)) ds_per_athlete <- ds_per_athlete %>% mutate(Weight = nznum(Weight))

# ---------- Order columns: vitals up front, keep everything else ----------
front_cols <- c(
  "athlete", "player_raw",
  "subject_type", "subject_filename",
  "ID", "Name", "Date_of_birth", "Height", "Weight",
  "Creation_date", "Creation_time",
  "Session_Type", "Session_Filename",
  "Session_Creation_date", "Session_Creation_time",
  "Meas_total")
front_cols <- unique(intersect(front_cols, names(ds_per_athlete)))

end_cols <- setdiff(names(ds_per_athlete), front_cols)
end_cols <- c(
  setdiff(end_cols, c("source_files", "data")),
  intersect("source_files", end_cols),
  intersect("data", end_cols)
)

tmp <- ds_per_athlete %>% select(any_of(front_cols), any_of(end_cols))

# Keep vitals & m{idx}_* even if sparse
meas_prefix_re <- "^m\\d+_"
vital_keep <- intersect(c("ID","Name","Date_of_birth","Height","Weight","Creation_date","Creation_time"), names(tmp))
non_all_missing <- vapply(tmp, function(x) !is_all_missing(x), logical(1))
is_measurement  <- grepl(meas_prefix_re, names(tmp))
is_vital        <- names(tmp) %in% vital_keep
keep_cols <- names(tmp)[ non_all_missing | is_measurement | is_vital ]

ds_final <- tmp[, keep_cols, drop = FALSE]

# ---------- Write CSV ----------
out_file <- "hitting_athlete_mapping.csv"
readr::write_csv(ds_final, out_file)
cat("Wrote", nrow(ds_final), "athletes to", out_file, "\n")
