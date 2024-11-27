library(RSQLite)
library(parallel)

# Define search patterns
search_pattern <- "^how\\t"
context_window <- 10
placeholder <- NA
db_path <- "C:/Users/chadi/Corpora/COHA-Source/COHAdb.sqlite"
text_id <- 1
decade <- 1860

################################################################################
# Function to get context lines
get_context_lines <- function(all_lines, line_number, context_window, 
                              placeholder = NA) {
  # Extract context lines, excluding line_number = 1
  sapply(-context_window:context_window, function(offset) {
    # Calculate the target line number
    target_line_number <- line_number + offset
    
    # Skip if the target line number is 1
    if (target_line_number == 1) {
      return(placeholder)
    }
    
    # Retrieve the line content for the target line number
    target_line <- 
      all_lines$line_content[all_lines$line_number == target_line_number]
    
    # Return placeholder if no line is found
    if (length(target_line) == 0) {
      return(placeholder)
    } else {
      return(target_line)
    }
  })
}

################################################################################
# Function to produce the concordance DF for a single text
process_text <- function(text_id, db_path, search_pattern, context_window, 
                         placeholder) {
  # Connect to the database
  con <- dbConnect(SQLite(), dbname = db_path)
  
  # Fetch all lines for the current text
  query_all_lines <- paste0("SELECT * FROM text_lines WHERE text_id = ", 
                            text_id)
  all_lines <- dbGetQuery(con, query_all_lines)
  
  # Filter lines that match the search pattern
  matches <- all_lines[grepl(search_pattern, all_lines$line_content, 
                             perl = TRUE, ignore.case = TRUE), ]
  
  # Disconnect from the database
  dbDisconnect(con)
  
  # Skip if no matches
  if (nrow(matches) == 0) return(NULL)
  
  # Extract context for each match
  concordance <- lapply(1:nrow(matches), function(i) {
    match <- matches[i, ]
    line_number <- match$line_number
    
    # Get context lines (see get_context_lines above)
    context <- get_context_lines(all_lines, line_number, 
                                 context_window, placeholder)
    
    # Combine into a single row
    c(text_id = text_id, context)
  })
  
  # Combine concordances for the current text into a DataFrame
  col_names <- c("text_id", paste0("L", context_window:1), 
                 "KEYWORD", paste0("R", 1:context_window))
  concordance_df <- do.call(rbind, concordance)
  colnames(concordance_df) <- col_names
  
  return(concordance_df)
}

################################################################################
# Function to create a concordance for a specific decade
create_concordance_for_decade <- function(db_path, search_pattern, 
                                          context_window = 5, 
                                          placeholder = NA, decade) {
  # Connect to the database
  con <- dbConnect(SQLite(), dbname = db_path)
  
  # Fetch text IDs for the specified decade
  query_text_ids <- paste0(
    "SELECT texts.text_id FROM texts ",
    "JOIN metadata ON texts.metadata_id = metadata.metadata_id ",
    "WHERE metadata.decade_id = ", decade
  )
  text_ids <- dbGetQuery(con, query_text_ids)$text_id
  
  dbDisconnect(con)
  
  # Create a cluster for parallel processing
  cl <- makeCluster(detectCores() - 1)  
  
  # Export necessary variables and functions to the cluster
  clusterExport(cl, c("db_path", "search_pattern", "context_window", 
                      "placeholder", "get_context_lines", "process_text"))
  clusterEvalQ(cl, library(RSQLite))  # Load RSQLite on each worker
  
  # Use parLapply to process each text in parallel
  all_concordances <- parLapply(cl, text_ids, function(text_id) {
    process_text(text_id, db_path, search_pattern, context_window, placeholder)
  })
  
  # Stop the cluster
  stopCluster(cl)
  
  # Combine all concordances into a single DataFrame
  final_concordance <- do.call(rbind, all_concordances)
  
  return(as.data.frame(final_concordance, stringsAsFactors = FALSE))
}



# Test the function
system.time({
  concordance_decade <- create_concordance_for_decade(
    db_path = db_path,
    search_pattern = search_pattern,
    context_window = 10,
    placeholder = NA,
    decade = 1860
  )
})


