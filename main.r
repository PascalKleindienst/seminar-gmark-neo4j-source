#!/usr/bin/env Rscript

# Config
wd <- getwd()
gMarkPath <- file.path(wd, "../gmark/src")
configFile <- file.path(wd, "../gmark/use-cases/shop.xml")
outputDir <- file.path(wd, "result")
workload <- file.path(outputDir, "workload-queries")

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

# load graph
graph <- startGraph("http://localhost:7474/db/data", username="neo4j", password="seminar")

# run gMark via system command
if (!file.exists(outputDir)) {
	dir.create(outputDir)
}

if (!file.exists(workload)) {
	dir.create(workload)
}

args <- list()
args["c"] <- paste0('-c "', configFile, '"')
args["g"] <- paste0('-g "', file.path(outputDir, "graph.csv"), '"')
args["w"] <- paste0('-w "', file.path(outputDir, "workload.xml"), '"')
args["r"] <- paste0('-r "', outputDir, '"')
args["o"] <- paste0('-o "', workload, '"')

command <- paste0('"', file.path(gMarkPath, "test"), '"')
#system(paste(command, args["c"], args["g"], args["w"], args["r"], "-a"))

command <- paste0('"', file.path(gMarkPath, "querytranslate", "test"), '"')
#system(paste(command, args["w"], args["o"]))


# reformat csv
data <- read.csv(file = file.path(outputDir, "graph.csv"), sep=" ")
relationships <- levels(factor(data[[2]]))

for (rel in relationships) {
	print(rel)
	rows <- subset(data, data[[2]] %in% rel)
	file <- file.path(outputDir, paste0("graph.", rel, ".csv"))
	write.table(rows, file, row.names=FALSE, col.names=FALSE)
	unlink(file)
}

# import graph.csv into neo4j

#cypher(graph, "CREATE INDEX ON :node(id)")
query = sprintf("
	LOAD CSV FROM \"file://%s\"  AS line
FIELDTERMINATOR ' '
MERGE (n:node {id: toInteger(line[0])})
MERGE (m:node {id: toInteger(line[2])})
CREATE (n)-[r]-(m)
RETURN n,m
LIMIT 1
", gsub(" ", '%20', file.path(outputDir, "graph.csv"), fixed=TRUE))

#cypher(graph, query)
