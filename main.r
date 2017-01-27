#!/usr/bin/env Rscript

#
# Load packages and create dirs
# 
setup <- function(output, workload) {
	# install R binding for Neo4j
	if (!require("RNeo4j")) {
		install.packages("RNeo4j", repos="http://cran.r-project.org", dependencies=TRUE)
		library("RNeo4j")
	}

	#if (!require("XML")) {
	#	install.packages("XML", repos="http://cran.r-project.org")
	#	library("XML")
	#	library("methods")
	#}
	
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
	#system(paste(command, args["c"], args["g"], args["w"], args["r"], "-a"))
	
	# create translated workload
	command <- paste0('"', file.path(gMarkPath, "querytranslate", "test"), '"')
	#system(paste(command, args["w"], args["o"]))
}

# 
# Import Data into neo4j
# 
import <- function(output, csv="graph.csv") {
	# create graph instance
	graph <- startGraph("http://localhost:7474/db/data", username="neo4j", password="seminar")

	# create index
	cypher(graph, "CREATE INDEX ON :node(id)")

	# get relationships from csv
	data <- read.csv(file = file.path(output, csv), sep=" ")
	relationships <- levels(factor(data[[2]]))

	for (rel in relationships) {
		# split csv by relationships
		print(rel)
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
# Main
# 
main <- function() {
	# Config
	wd <- getwd()
	output <- file.path(wd, "result")
	workload <- file.path(output, "workload-queries")

	setup(output, workload)
	gmark(output, workload, file.path(wd, "../gmark/use-cases/shop.xml"), file.path(wd, "../gmark/src"))
	import(output)
}

main()