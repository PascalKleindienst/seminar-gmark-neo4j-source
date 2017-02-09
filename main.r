#!/usr/bin/env Rscript

# 
# Helpers
# 
printf <- function(...) cat(sprintf(...))

#
# Load packages and create dirs
# 
setup <- function(output, workload) {
	# install R binding for Neo4j
	if (!require("RNeo4j")) {
		install.packages("RNeo4j", repos="http://cran.r-project.org", dependencies=TRUE)
		library("RNeo4j")
	}

	if (!require("XML")) {
		install.packages("XML", repos="http://cran.r-project.org")
		library("XML")
		library("methods")
	}
	
	# create dirs
	if (!file.exists(output)) {
		dir.create(output)
	}

	if (!file.exists(workload)) {
		dir.create(workload)
	}
}

# 
# Run gMark
# 
gmark <- function(output, workload, config, gMarkPath, graph="graph.csv", workloadXML="workload.xml") {
	# args for gmark
	args <- list(
		"c"=paste0('-c "', config, '"'),
		"g"=paste0('-g "', file.path(output, graph), '"'),
		"w"=paste0('-w "', file.path(output, workloadXML), '"'),
		"r"=paste0('-r "', output, '"'),
		"o"=paste0('-o "', workload, '"')
	)

	# create graph instance csv and workload in internal format
	command <- paste0('"', file.path(gMarkPath, "test"), '"')
	system(paste(command, args["c"], args["g"], args["w"], args["r"], "-a"))
	
	# create translated workload
	command <- paste0('"', file.path(gMarkPath, "querytranslate", "test"), '"')
	system(paste(command, args["w"], args["o"]))
}

# 
# Import Data into neo4j
# 
import <- function(graph, output, csv="graph.csv") {
	# create index
	cypher(graph, "CREATE INDEX ON :node(id)")

	# get relationships from csv
	data <- read.csv(file = file.path(output, csv), sep=" ")
	relationships <- levels(factor(data[[2]]))

	for (rel in relationships) {
		# split csv by relationships
		printf("Importing relationship: %s\n", rel)
		rows <- subset(data, data[[2]] %in% rel)
		file <- file.path(output, paste0("graph.", rel, ".csv"))
		write.table(rows, file, row.names=FALSE, col.names=FALSE)

		# import data for relationship
		query = sprintf("
			USING PERIODIC COMMIT
			LOAD CSV FROM \"file://%s\"  AS line
			FIELDTERMINATOR ' '
			MERGE (n:node {id: toInteger(line[0])})
			MERGE (m:node {id: toInteger(line[2])})
			CREATE (n)-[:%s]->(m);",
			gsub(" ", '%20', file, fixed=TRUE),
			paste0('p', rel)
		)
		cypher(graph, query)

		# cleanup
		unlink(file)
	}
}


#
# Workload
#
run_workload <- function(graph, queries, workload) {
	# load xml workload metadata
	data <- xmlParse(file=workload)
	df <- data.frame(
	    "number" = sapply(data["//metadata/number"], xmlValue),
	    "arity" = sapply(data["//metadata/arity"], xmlValue),
	    "selectivity" = sapply(data["//metadata/selectivity"], xmlValue),
	    "multiplicity" = sapply(data["//metadata/multiplicity"], xmlValue),
	    "conjunct" = sapply(data["//metadata/conjunct"], xmlValue),
	    "disjuncts" = sapply(data["//metadata/disjuncts"], xmlValue),
	    "length" = sapply(data["//metadata/length"], xmlValue)
	)

	# add empty query and time columns
	namevector <- c("query","time")
	df[,namevector] <- NA

	# query files
	files <- Sys.glob(file.path(queries, 'query-*.cypher'))
	cat("number,arity,selectivity,multiplicity,conjunct,disjuncts,length,query,time",file="result.csv",sep="\n")

	# run workload
	f <- function(row, files, queries) {
		fileName <- file.path(queries, sprintf("query-%s.cypher", row$number))

		if (fileName %in% files) {
			query <- readChar(fileName, file.info(fileName)$size)
			query <- gsub("\r?\n|\r", " ", query) # remove line breaks from query
			printf("Running query %s\n", row$number)

			# try to execute query and measure execution time, else time = -1
			result <- tryCatch({
				start.time <- Sys.time()
				data <- cypher(graph, query)
				end.time <- Sys.time()

				time <- (end.time - start.time)
			}, error = function(err) {
				printf("\tInvalid cypher query in query-%s.cypher\n", row$number)
				return(-1)
			})

			# write data into file
			row$query <- query
			row$time <- result
			write.table(row, file="result.csv", sep = ",", row.names = FALSE, col.names = FALSE, append = TRUE)
		}
	}

	by(df, 1:nrow(df), f, files=files, queries=queries)
}

#
# Main
# 
main <- function() {
	# Config
	wd <- getwd()
	output <- file.path(wd, "result")
	workload <- file.path(output, "workload-queries")
	config <- file.path(wd, "../gmark/use-cases/social-network.xml")

	# Setup
	setup(output, workload)
	#gmark(output, workload, config, file.path(wd, "../gmark/src"))
	
	# Prepare graph and run workloads
	graph <- startGraph("http://localhost:7474/db/data", username="neo4j", password="seminar")
	#import(graph, output)
	run_workload(graph, workload, file.path(output, "workload.xml"))
}

main()