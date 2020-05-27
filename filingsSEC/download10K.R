suppressWarnings(require(edgarWebR))
suppressWarnings(require(stringr))
suppressWarnings(require(dplyr))
suppressWarnings(require(openxlsx))
suppressWarnings(require(edgar))

args <- commandArgs(trailingOnly=TRUE)

################################
##- verify matches (ticker, company name)
##- 10KSB* filings (see HP) -> reason for "missing" files?
##- extract business descriptions
##- investigate cases where filingsList.csv is empty
##- complete notfound cases
##- delete "duplicates", e.g. */A (appendix?)
##- time-varying CIK: CIK numbers remain unique to the filer; they are not recycled.
##--> match with CRSP!


createData <- function(){
    ##index for sec filings
    ##edgar::getMasterIndex(2019)
    ind <- load(paste0(getwd(),"/Master Indexes/1998master.Rda"))
    yearmaster <- year.master
    for (y in 1999:2019){
        ind <- load(paste0(getwd(),"/Master Indexes/",y,"master.Rda"))
        yearmaster <- rbind(yearmaster, year.master)
    }
    ##only 10-K
    yearmaster1 <- yearmaster[grep("10-?K",yearmaster$form.type),]
    yearmaster1 <- unique(yearmaster1[,c("cik","company.name")])
    ## yearmaster1$company.name <- toupper(yearmaster1$company.name)
    ## yearmaster1$company.name <- gsub("( Corp.*|,? Inc.*)","",yearmaster1$company.name,perl=T,ignore.case=T)
    ## yearmaster1$company.name <- gsub("[-.,& ]","",yearmaster1$company.name)

    ##cik -> ciq
    cik <- read.csv("ciq_cik.csv")
    ##factor to char
    yearmaster1$cik <- as.character(yearmaster1$cik)
    cikciq <- merge(yearmaster1, cik, by="cik", all.x=T, all.y=F)
    ##which(is.na(test$companyid))

    ##ciq -> isin (nearly complete for orbis data)
    ciq <- read.csv("isin_ciq.csv")
    ciq$ISIN <- as.character(ciq$ISIN)
    ciq <- ciq[!is.na(ciq$companyid),]
    ciqisin <- merge(cikciq, ciq, by="companyid", all.x=T, all.y=F)
    ##which(is.na(test1$cik))

    ##read orbis data
    orbisdata <- read.csv("../dataMichael/Orbis/Orbis_US_public.txt",fileEncoding="utf-16",sep=";")
    orbisdata$Ticker.symbol <- as.character(orbisdata$Ticker.symbol)
    orbisdata$BvD.ID.number <- as.character(orbisdata$BvD.ID.number)
    orbisdata$Country.ISO.Code <- as.character(orbisdata$Country.ISO.Code)
    orbisdata$Company.name <- as.character(orbisdata$Company.name)
    orbisdata$ISIN.number <- as.character(orbisdata$ISIN.number)
    ##delete rows with no company name
    orbisdata <- orbisdata[nchar(orbisdata$Company.name)>0,]
    orbisdata <- orbisdata[nchar(orbisdata$ISIN.number)>0,]
    ##only keep US
    orbisdata <- orbisdata[orbisdata[,"Country.ISO.Code"]=="US",]
    ##write.csv(unique(orbisdata$ISIN.

    ##merge with orbis data
    isinweb <- merge(ciqisin, orbisdata, by.x="ISIN", by.y="ISIN.number", all.x=T, all.y=F)
    return(isinweb)
}
##TO-DO: ADDITIONAL MATCHES?
## ##Get ciq gvkey mapping
## gvkey <- read.csv("GVKEY_CIQID_Mapping_03112016.csv")
## gvkeyIsin <- merge(ciq, gvkey, by.x="companyid", by.y="ciq_id", all.x=T, all.y=F)
## ## which(is.na(gvkeyIsin$gvkey))
## ##match with compustat
## compustat <- read.csv("compustat.csv")
## compustat <- unique(compustat[,c("conm","gvkey","cik","sic","city","loc")])
## ##############
## ##exact isin match with capital iq
## ciq <- read.csv("CustomerSupplierData_CIQ_07122015_unique_ids_sic_isin.csv")
## ciq$isin <- gsub("^I_","",ciq$isin,perl=T)
## orbisdata1 <- merge(orbisdata, ciq, by.x="ISIN.number", by.y="isin", all.x=T, all.y=F)
## orbisdata1 <- orbisdata1[is.na(orbisdata1$gvkey),]


################################
##extract business description
extract <- function(){
    if (!"parseddata"%in%list.files(getwd())) dir.create("parseddata")
    folders <- list.files(paste0(getwd(),"/output"))
    for (f in folders){
        if (!f%in%list.files(paste0(getwd(),"/parseddata"))) dir.create(paste0(getwd(),"/parseddata/",f))
        ##list all html files
        htmls <- list.files(paste0(getwd(),"/output/",f))
        ##if empty or only filingsList.csv, skip
        if (length(htmls)<=1) next
        htmls <- htmls[grep("txt$",htmls)]
        ##remove duplicated filings per year, e.g. amendment
        filings <- read.csv(paste0(getwd(),"/output/",f,"/filingsList.csv"))
        filings$year <- as.numeric(substr(filings$filing_date,1,4))
        ##iterate through txt files and segment text - BY YEAR
        ##TO-DO: validation needed for parse_filing function, e.g. check length of business section
        for (y in unique(filings$year)){
            subfilings <- filings[filings$year==y,]
            ##if extracted (then move file to parseddata dir)
            if (any(subfilings$extracted==1)) next
            found <- FALSE
            for (sf in 1:NROW(subfilings)){
                ##find item 1. business
                final <- helpExtract(paste0(getwd(),"/output/",f,"/",subfilings$accession_number[sf],".txt"))
                ##if no extraction, do not save anything & next
                if (final=="nothing found") next
                ##write to file
                write.table(final,paste0(getwd(),"/parseddata/",f,"/",subfilings$accession_number[sf],".txt"),row.names=F,col.names=F)
                ##update filingsList if successful
                filings[filings$accession_number==subfilings$accession_number[sf],"extracted"] <- 2
                found <- TRUE
            }
            if (!found) print(paste("no business description found for:",f,"in",y))
        }
        ##and write update to file
        write.csv(filings,paste0(getwd(),"/output/",f,"/filingsList.csv"),row.names=F)
    }
    ##delete: "7", "Table of Contents" (by line), html code, etc.
}


################################
##helper function to extract business description section
##TO-DO: mark if helpExtract was used -> check "name" of starting and ending item
helpExtract <- function(path){
    text <- readLines(path)
    ##look for start of item 1 (until next item)
    result <- str_extract_all(text, "(?s)(?i)°Item(&[^ ]+;| )*(1| I)[^AB0123456789].*?°Item(&[^ ]+;| )*(1A|1B|2|II).{20,20}")
    ##check how often this pops up (in amendment): 0001021408-00-001361.html; 0000004127-19-000005.html
    ##none: 0001021408-00-001361.html
    ##check length of result
    if (length(result[[1]])==0) return("nothing found")
    ##heuristics for TOC
    ##if (length(result[[1]])>2) print(paste("Too many results for:",path))
    if (nchar(gsub("[.]","",result[[1]][1]))<500) {
        return(result[[1]][2])
    } else {return(result[[1]][1])}
}


################################
##https://github.com/EricHe98/Financial-Statements-Text-Analysis/tree/master/Documentation
autoParse <- function(fname,cik,href){
    paste0(href) %>% # Step 1
    readLines(encoding = "UTF-8") %>% # Step 2
    ##when line is starting with item     
    str_replace_all(pattern = "(?s)(?i)(?m)^ *I?tem", replacement = "°Item") %>% # Step 7
    str_c(collapse = " ") %>% # Step 3
    str_extract(pattern = "(?s)(?m)<TYPE>10-?K.*?(</TEXT>)") %>% # Step 4
    str_replace(pattern = "((?i)<TYPE>).*?(?=<)", replacement = "") %>% # Step 5
    str_replace(pattern = "((?i)<SEQUENCE>).*?(?=<)", replacement = "") %>% # Step 6
    str_replace(pattern = "((?i)<FILENAME>).*?(?=<)", replacement = "") %>%
    str_replace(pattern = "((?i)<DESCRIPTION>).*?(?=<)", replacement = "") %>%
    str_replace(pattern = "(?s)(?i)<head>.*?</head>", replacement = "") %>%
    str_replace(pattern = "(?s)(?i)<(table).*?(</table>)", replacement = "") %>%
    str_replace_all(pattern = "(?s)(?i)(?m)> *I?tem", replacement = ">°Item") %>% # Step 7
    str_replace(pattern = "</TEXT>", replacement = "°</TEXT>") %>%
    str_replace_all(pattern = "(?s)<.*?>", replacement = " ") %>% # Step 8
    str_replace_all(pattern = "&(.{2,6});", replacement = " ") %>% # Step 9
    str_replace_all(pattern = "(?s) +", replacement = " ") %>% # Step 10
    write(file = paste0("output/",cik,"/",fname,".txt"), sep = "") # Step 11
}


################################
downloadData <- function(i){
    if (!"output"%in%list.files(getwd())) dir.create("output")
    notunique <- vector()
    isinweb <- createData()
    ##only download ciks filings with orbis match
    i <- as.numeric(i)
    ciks <- unique(isinweb[!is.na(isinweb$BvD.ID.number),"cik"])
    interval <- ceiling(length(ciks)/5)
    end <- min(c(i*interval,length(ciks)))
    ciks <- ciks[(1+(i-1)*interval):end]
    print(paste("Start crawling: ",length(ciks),"..."))
    for (cik in ciks){
        ##make dir for firm
        if (!cik%in%list.files(paste0(getwd(),"/output"))) dir.create(paste0(getwd(),"/output/",cik))
        ##if already data available, check for completeness
        files <- list.files(paste0(getwd(),"/output/",cik))
        haben <- c("")
        ## if ("filingsList.csv"%in%files) {
        ##     haben <- files[grep("html",files)]
        ##     soll <- read.csv(paste0(getwd(),"/output/",cik,"/filingsList.csv"),header=T)
        ##     soll$accession_number <- as.character(soll$accession_number)
        ##     ##if all results downloaded
        ##     if (all(paste0(soll$accession_number,".html")%in%haben)) next
        ## }
        ##search by ticker and get list of 10-?Ks
        filings1 <- tryCatch({company_filings(x=cik,type="10-K",count=100)}, error = function(e) {NA})
        filings2 <- tryCatch({company_filings(x=cik,type="10K",count=100)}, error = function(e) {NA})
        filings <- rbind(filings1, filings2)
        filings$extracted <- 0
        ##if df empty, write empty df
        if (NROW(filings)==0) {print(paste("no result returned for:",cik)); next}
        for (f in 1:NROW(filings)){
            accNumber <- filings[f,"accession_number"]
            ##if file exists, skip
            if (paste0(accNumber,".html")%in%haben) next
            ##get date of filing and link
            ddate <- filings[f,"filing_date"]
            link <- filings[f,"href"]
            ##follow link and download 10-K
            result <- filing_documents(link)
            ##look for 10-K*
            subd <- result[grep("Complete submission text file",result$description,ignore.case=T),]
            ##check uniqueness of results
            if (NROW(subd)>1 | NROW(subd)==0) {notunique <- c(notunique, c(link)); next}
            finallink <- subd$href[1]
            ##try segmentation using edgarWebR
            ##download finallink, name unique accession_number
            segm <- tryCatch({parse_text_filing(finallink)}, error=function(e) {paste("Error: ",finallink,f)})
            ##find item 1. business
            manual <- F
            if (!is.character(segm)) {
                ##TO-DO: output needs to be cleaned, see steps in autoParse
                final <- segm[grep("(Item 1.*Business|Item 1[.]|Item 1$)",segm$item.name,perl=T,ignore.case=T), "text"]
                ##if found
                if (NROW(final)>0) {
                    output <- paste0(final,collapse=" ")
                    write(output, file = paste0("output/",cik,"/",accNumber,".txt"))
                    filings$extracted[f] <- 1
                }                
            }
            ##if not found using parse_filing
            if (filings$extracted[f]==0 | is.character(segm)) {
                autoParse(accNumber,cik,finallink)
                filings$extracted[f] <- 0
            }            
            ##add size to filings
            filings$size[f] <- subd$size[1]
        }
        write.csv(filings, paste0(getwd(),"/output/",cik,"/filingsList.csv"),row.names=F)
    }
}




