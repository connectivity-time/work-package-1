library(RSQLite)

# Connect to the Database
con <- dbConnect(RSQLite::SQLite(), 
                 "C:/Users/chadi/Corpora/COHA-Source/COHAdb.sqlite")

# List all tables in the database
tables <- dbListTables(con)
print(tables)

# Explore the structure of a specific table

# retrieve the schema for each table
schemas <- setNames(
  lapply(tables, function(table) {
    dbGetQuery(con, paste("PRAGMA table_info(", table, ");", sep = ""))
  }),
  tables
)

# Print the list of schemas
print(schemas)


# Fetch the first few rows from a table, truncating text for the "texts" table
few_rows_list <- setNames(
  lapply(tables, function(table) {
    if (table == "texts") {
      # Truncate text column for texts table
      dbGetQuery(con, paste(
        "SELECT text_id, substr(content, 1, 50) AS content FROM", table, "LIMIT 10;"
      ))
    } else {
      # Fetch all columns for other tables
      dbGetQuery(con, paste("SELECT * FROM", table, "LIMIT 10;"))
    }
  }),
  tables
)

# Print the result
print(few_rows_list)

# Close the connection
dbDisconnect(con)
