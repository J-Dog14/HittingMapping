# Quick diagnostic script to test file reading and data extraction
library(xml2)
library(dplyr)
library(tibble)

cat("=== DIAGNOSTIC TEST ===\n\n")

# Test 1: Check if files exist
cat("Test 1: Checking if files exist\n")
session_file <- "session.xml"
session_data_file <- "session_data.xml"

cat("  session.xml exists:", file.exists(session_file), "\n")
cat("  session_data.xml exists:", file.exists(session_data_file), "\n\n")

# Test 2: Try to read session.xml
if (file.exists(session_file)) {
  cat("Test 2: Reading session.xml\n")
  tryCatch({
    doc <- read_xml(session_file)
    root <- xml_root(doc)
    cat("  Root element:", xml_name(root), "\n")
    
    # Try to extract athlete info
    fields <- xml_find_first(root, "./Fields")
    if (!inherits(fields, "xml_missing")) {
      name <- xml_text(xml_find_first(fields, "./Name"))
      id <- xml_text(xml_find_first(fields, "./ID"))
      cat("  Name:", name, "\n")
      cat("  ID:", id, "\n")
    }
  }, error = function(e) {
    cat("  ERROR:", conditionMessage(e), "\n")
  })
  cat("\n")
}

# Test 3: Try to read session_data.xml and check structure
if (file.exists(session_data_file)) {
  cat("Test 3: Reading session_data.xml\n")
  tryCatch({
    doc <- read_xml(session_data_file)
    root <- xml_root(doc)
    cat("  Root element:", xml_name(root), "\n")
    
    # Check for owners
    owners <- xml_find_all(root, "./owner")
    cat("  Number of owners:", length(owners), "\n")
    
    if (length(owners) > 0) {
      for (i in 1:min(3, length(owners))) {
        own <- owners[[i]]
        own_val <- xml_attr(own, "value")
        cat("  Owner", i, ":", own_val, "\n")
        
        # Check for LINK_MODEL_BASED
        link_types <- xml_find_all(own, "./type[@value='LINK_MODEL_BASED']")
        cat("    LINK_MODEL_BASED types found:", length(link_types), "\n")
        
        if (length(link_types) > 0) {
          for (lt in link_types) {
            folders <- xml_find_all(lt, "./folder")
            cat("    Folders:", length(folders), "\n")
            
            for (fol in folders) {
              folder_val <- xml_attr(fol, "value")
              names <- xml_find_all(fol, "./name")
              cat("      Folder '", folder_val, "' has", length(names), "variables\n", sep = "")
              
              # Check for our target variables
              for (nm in names) {
                var_name <- xml_attr(nm, "value")
                if (var_name %in% c("Pelvis_Angle", "Pelvis_Shoulders_Separation", "Trunk_Angle")) {
                  cat("        Found variable:", var_name, "\n")
                  comps <- xml_find_all(nm, "./component")
                  cat("          Components:", length(comps), "\n")
                  
                  for (comp in comps) {
                    axis <- xml_attr(comp, "value")
                    frames <- xml_attr(comp, "frames")
                    data_attr <- xml_attr(comp, "data")
                    cat("            Axis:", axis, "| Frames:", frames, "| Has data:", !is.na(data_attr), "\n")
                    if (!is.na(data_attr) && nchar(data_attr) > 0) {
                      # Show first few values
                      vals <- strsplit(data_attr, ",", fixed = TRUE)[[1]]
                      cat("            First 3 values:", paste(head(vals, 3), collapse = ", "), "\n")
                    }
                  }
                }
              }
            }
          }
        }
        
        # Check for METRIC
        metric_types <- xml_find_all(own, "./type[@value='METRIC']")
        cat("    METRIC types found:", length(metric_types), "\n")
        
        # Check for EVENT_LABEL
        event_types <- xml_find_all(own, "./type[@value='EVENT_LABEL']")
        cat("    EVENT_LABEL types found:", length(event_types), "\n")
      }
    }
  }, error = function(e) {
    cat("  ERROR:", conditionMessage(e), "\n")
    print(traceback())
  })
  cat("\n")
}

cat("=== END DIAGNOSTIC ===\n")

