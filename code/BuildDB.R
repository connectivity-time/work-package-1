library(readxl)
library(dplyr)
library(DBI)
library(RSQLite)

# Paths
zip_files <- list.files("C:/Users/chadi/Corpora/COHA-Source", 
                        pattern = "\\.zip$", full.names = TRUE)
excel_path <- "C:/Users/chadi/Corpora/COHA-Source/sources_coha.xlsx"
extract_dir <- "C:/Users/chadi/Corpora/COHA-Source/extracted"
db_path <- "C:/Users/chadi/Corpora/COHA-Source/COHAdb.sqlite"


# Load metadata
metadata <- read_excel(excel_path, sheet = "cohaTexts")

# Function to homoginize author names
clean_author_names <- function(author) {
  if (is.na(author)) {
    return(NA)
  }
  # Trim whitespace
  author <- trimws(author)
  # Replace spaces and punctuation with underscores
  author <- gsub("[[:punct:]]|\\s+", "_", author)
  # Remove multiple consecutive underscores
  author <- gsub("_+", "_", author)
  # Remove trailing or leading underscores
  author <- gsub("^_|_$", "", author)
  return(author)
}

# Filter metadata for the 1860-2000 range
metadata <- metadata %>%
  filter(year >= 1860 & year <= 2000) %>%
  mutate(decade = floor(year / 10) * 10)

# Clean the author names in metadata
metadata <- metadata %>%
  mutate(author = sapply(author, clean_author_names))

################################################################################
# Create SQLite database
db <- dbConnect(SQLite(), db_path)

# Create normalized tables: decades, metadata, and texts
dbExecute(db, "CREATE TABLE decades (
                decade INTEGER PRIMARY KEY)");

dbExecute(db, "CREATE TABLE IF NOT EXISTS metadata (
                metadata_id INTEGER PRIMARY KEY,
                year INTEGER,
                genre TEXT,
                title TEXT,
                num_words INTEGER,
                author TEXT,
                decade_id INTEGER,
                FOREIGN KEY (decade_id) REFERENCES decades(decade_id))");

dbExecute(db, "CREATE TABLE IF NOT EXISTS texts (
                text_id INTEGER PRIMARY KEY,
                file_name TEXT,
                content TEXT,
                metadata_id INTEGER,
                FOREIGN KEY (metadata_id) REFERENCES metadata(metadata_id))");

# Extract decades
unique_decades <- metadata %>%
  distinct(decade)

# Insert decades into the database
dbWriteTable(db, "decades", unique_decades, append = TRUE, row.names = FALSE)

################################################################################
# Populate the database
for (zip_file in zip_files) {
  # Extract decade from ZIP file name
  decade <- as.integer(gsub("\\D", "", basename(zip_file)))
  
  # Extract files to a temporary folder
  unzip(zip_file, exdir = extract_dir)
  
  # Get the files from the extracted folder
  file_list <- list.files(extract_dir, pattern = "\\.txt$", full.names = TRUE)
  
  # Filter metadata for the current decade
  decade_metadata <- metadata %>% filter(decade == !!decade)
  
  # Process and insert metadata and texts
  for (i in seq_len(nrow(decade_metadata))) {
    row <- decade_metadata[i, ]
    text_id <- row$textID
    
    # Match file
    matched_file <- file_list[grepl(paste0(text_id, "\\.txt$"), file_list)]
    if (length(matched_file) == 0) next
    
    # Read content
    content <- paste(readLines(matched_file[1], warn = FALSE), collapse = "\n")
    
    # Insert metadata
    metadata_row <- data.frame(
      year = as.integer(row$year),
      genre = as.character(row$genre),
      title = as.character(row$title),
      num_words = as.integer(row$words),
      author = as.character(row$author),
      decade_id = as.integer(row$decade),
      stringsAsFactors = FALSE,
      row.names = NULL
    )
    dbWriteTable(db, "metadata", metadata_row, append = TRUE, row.names = FALSE)
    
    # Get metadata_id
    metadata_id <- dbGetQuery(db, "SELECT last_insert_rowid()")$`last_insert_rowid()`
    
    # Insert text
    text_row <- data.frame(
      file_name = basename(matched_file[1]),
      content = content,
      metadata_id = metadata_id,
      stringsAsFactors = FALSE
    )
    dbWriteTable(db, "texts", text_row, append = TRUE, row.names = FALSE)
  }
  
  # Clean up extracted files
  unlink(file_list)
}

# Query to retrieve all texts
texts <- dbGetQuery(con, "SELECT text_id, content FROM texts")

# Create the new table in the database
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS text_lines (
    line_id INTEGER PRIMARY KEY AUTOINCREMENT,
    text_id INTEGER NOT NULL,
    line_number INTEGER NOT NULL,
    line_content TEXT NOT NULL,
    FOREIGN KEY (text_id) REFERENCES texts (text_id)
  )
")

# Prepare to insert split lines into the new table
dbExecute(con, "DELETE FROM text_lines") # Clear the table if it exists


# Split the content of each text into lines
texts <- dbGetQuery(con, "SELECT text_id, content FROM texts")

split_lines <- do.call(rbind, lapply(seq_len(nrow(texts)), function(i) {
  text_id <- texts$text_id[i]
  
  # Sanitize the content to remove invalid characters
  sanitized_content <- iconv(texts$content[i], from = "UTF-8", to = "UTF-8", sub = "")
  
  # Split sanitized content into lines
  lines <- unlist(strsplit(sanitized_content, "\\n"))
  
  # Return a data frame of split lines
  data.frame(
    text_id = text_id,
    line_number = seq_along(lines),
    line_content = lines,
    stringsAsFactors = FALSE
  )
}))

# Insert split lines into the new table
dbWriteTable(con, "text_lines", split_lines, append = TRUE, row.names = FALSE)

dbDisconnect(db)
