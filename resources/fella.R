args<-commandArgs(T)
mtbls_id <- args[1]
org <- args[2]
output_folder <-args[3]


mtbls_id
org
output_folder

#mtbls_id <- 'MTBLS2'
#org <- "hsa"
#output_folder <- '/Users/jkchang/Downloads/'

list.of.packages <- c("httr", "jsonlite","igraph", "visNetwork","magrittr")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

if (!require('FELLA'))
if (!requireNamespace("BiocManager", quietly = TRUE))
    install.packages("BiocManager")
    BiocManager::install("FELLA")

library(httr)
library(jsonlite)
library(FELLA)
library(igraph)
library(visNetwork)
library(magrittr)

# path.hsa <- "./instance/Datasets/FELLAdatabases/hsa"
path.hsa <- paste('./instance/Datasets/FELLAdatabases/',org,sep='')
path.hsa

if (!dir.exists(path.hsa)) {
  g.hsa <- buildGraphFromKEGGREST(organism = org, filter.path = NULL)
  buildDataFromGraph(g.hsa, databaseDir = path.hsa, 
                     internalDir = FALSE, niter = 1000) 
  gc()
}

extract_KEGG_id <- function(mtbls_id) {
  url <- paste('http://wp-np3-15.ebi.ac.uk:5000/metabolights/ws/ebi-internal/keggid?studyID=',mtbls_id,'&kegg_only=true',sep='')
  dy <- POST(url)
  data = fromJSON(rawToChar(dy$content))
  kegg.ids <- unique(data)
  return (kegg.ids)
}

kegg.ids <- extract_KEGG_id (mtbls_id)

res <- FELLA::enrich(
  compounds = kegg.ids, methods = "diffusion",
  databaseDir = path.hsa, internalDir = FALSE)

g <- FELLA::generateResultsGraph(
  format = "igraph", object = res$user, data = res$data)

mapSolidColor <- c(`1` = "#CD0000", 
                   `2` = "#CD96CD", 
                   `3` = "#FFA200",
                   `4` = "#8DB6CD",
                   `5` = "#548B54")

V(g)$shape <- rep("dot", vcount(g))
V(g)$shape[V(g)$input] <- "square"
V(g)$color <- mapSolidColor[V(g)$com]

# Visualize the graph
vg<- visNetwork::visIgraph(g, idToLabel = FALSE)

t <- FELLA::generateResultsTable(method = 'diffusion',
                                 nlimit = 1000,
                                 object = res$user,
                                 data=res$data)

o_g <- paste(output_folder,mtbls_id,'.html',sep='')
htmlwidgets::saveWidget(vg, o_g)

o_t <-  paste(output_folder,mtbls_id,'.csv',sep='')
write.csv(t,file=o_t)
