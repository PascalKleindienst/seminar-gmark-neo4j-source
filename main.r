#!/usr/bin/env Rscript

# Config
wd = getwd()
gMarkPath = file.path(wd, "../gmark/src")
configFile = file.path(wd, "../gmark/use-cases/shop.xml")
outputDir = file.path(wd, "result")

# install R binding for Neo4j
if (!require("RNeo4j")) {
	install.packages("RNeo4j", repos="http://cran.r-project.org", dependencies=TRUE)
	library(RNeo4j)
}

# load graph
graph = startGraph("http://localhost:7474/db/data", username="neo4j", password="seminar")

# run gMark via system command

if (!file.exists(outputDir)) {
	dir.create(outputDir)
}

args = list()
args["c"] = paste0('-c "', configFile, '"')
args["g"] = paste0('-g "', file.path(outputDir, "graph.csv"), '"')
args["w"] = paste0('-w "', file.path(outputDir, "workload.xml"), '"')
args["r"] = paste0('-r "', outputDir, '"')

command = paste0('"', file.path(gMarkPath, "test"), '"')
system(paste(command, args["c"], args["g"], args["w"], args["r"], "-a"))
