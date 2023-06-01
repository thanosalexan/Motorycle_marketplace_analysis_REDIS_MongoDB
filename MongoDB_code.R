setwd(dir='/Users/thanosalexandris/Downloads/BIKES')



install.packages("mongolite")
library("mongolite")

##load paths
files <- read.table("/Users/thanosalexandris/Downloads/BIKES/files_list.txt")

View(files)

str(files)

library("jsonlite")



## create lists with the fielads of data that we are interested in
all_prices<-c()
all_mileage<-c()
all_age<-c()
all_colours<-c()
all_brands<-c()
all_models<-c()



#load our data in r and creation of the respective lists

for (i in 1:nrow(files)){
  from_jason<- fromJSON(readLines(files[i,], encoding="UTF-8"))
  all_prices[i] <- from_jason$ad_data$Price
  if (!is.null(from_jason$ad_data$Mileage)){
    all_mileage[i] <- from_jason$ad_data$Mileage}
  else { all_mileage[i]<-"?"}
  all_age[i]<-from_jason$ad_data$Registration
  all_colours[i]<-from_jason$ad_data$Color
  all_brands[i]<-from_jason$metadata$brand
  all_models[i]<-from_jason$metadata$model
}


##cleaning process

all_prices<-gsub("€", "", all_prices)
all_prices<-gsub(".", "", all_prices,fixed = TRUE)
all_prices<-gsub("Askforprice", "-1", all_prices)
all_prices<-as.numeric(all_prices)
sorted_prices<-sort(as.numeric(all_prices))
summary(sorted_prices[sorted_prices>0])

all_mileage<- gsub(" km", "", all_mileage)
all_mileage<- gsub(",", "", all_mileage)
sorted_mileage<-sort(as.numeric(all_mileage[all_mileage!="?"]))
summary(sorted_mileage)


all_prices_unique<-unique(sorted_prices)
all_mileage_unique<-unique(sorted_mileage)
all_age_unique<-unique(all_age)
all_colours_unique<-unique(all_colours)
all_brands_unique<-unique(all_brands)
all_models_unique<-unique(all_models)


##price plots
par(mfrow=c(1,2))
h1 <- hist(all_prices, main=names(all_prices), col='pink')


boxplot(all_prices
)


q1_prices <- quantile(all_prices, 0.25)  # 1st quartile
q3_prices <- quantile(all_prices, 0.75)  # 3rd quartile

q0.05_prices<-quantile(all_prices, 0.05)
q0.95_prices<-quantile(all_prices, 0.95)

##mileage plots

h1 <- hist(sorted_mileage, main=names(sorted_mileage), col='pink')
boxplot(sorted_mileage)


q1_mileage <- quantile(sorted_mileage, 0.25)  # 1st quartile
q3_mileage <- quantile(sorted_mileage, 0.75)  # 3rd quartile

q0.05<-quantile(sorted_mileage, 0.05)
q0.95<-quantile(sorted_mileage, 0.95)


#load data to mongo
dfm<-c()

for (i in 1:nrow(files)){
  A<- fromJSON(readLines(files[i,], encoding="UTF-8"))
  A$ad_data$Month <- as.numeric(substr(A$ad_data$Registration, start = 1, stop = 2))
  A$ad_data$Year <- as.numeric(substr(A$ad_data$Registration, start = 5, stop = 9))
  A$ad_data$Price <- gsub("€", "", A$ad_data$Price)
  A$ad_data$Price <- gsub(".", "", A$ad_data$Price, fixed = TRUE)
  A$ad_data$Price <- gsub("Askforprice", "-1", A$ad_data$Price)
  A$ad_data$Price <- as.numeric(A$ad_data$Price)
  if(!is.null(A$ad_data$Mileage)){
    A$ad_data$Mileage <- gsub(" km", "", A$ad_data$Mileage)
    A$ad_data$Mileage <- gsub(",", "", A$ad_data$Mileage)
    A$ad_data$Mileage <- as.numeric(A$ad_data$Mileage)
  }
  B <- toJSON(A, auto_unbox = TRUE)
  dfm<-c(dfm,B)
}

MB <- mongo(collection = "col_bikes",  db = "db_bikes", url = "mongodb://localhost")

MB$insert(dfm)


#########Questions

#Question 2
MB$count()

#Question 3
MB$aggregate('[{"$match": {"ad_data.Price" : {"$gte": 260, "$lte":8000}}}, {"$group": {"_id": null, "avg_price": {"$avg": "$ad_data.Price"}, "count_bikes":{"$sum": 1}}}]')

#Question 4
MB$aggregate('[{"$match": {"ad_data.Price" : {"$gte": 260, "$lte":8000}}}, {"$group": {"_id": null, "min_price": {"$min": "$ad_data.Price"}, "max_price":{"$max": "$ad_data.Price"}}}]')

#Question 5
MB$aggregate('[{"$match": {"metadata.model" : {"$regex" : "Negotiable"}}}, {"$group": {"_id": null,  "count_negotiable_listings": {"$sum": 1}}}]')

#Question 6
MB$aggregate('[{"$group": {"_id": {"brand": "$metadata.brand","negotiable": { "$cond": [{ "$regexMatch": { "input": "$metadata.model", "regex": "Negotiable" } }, 1, 0] }},
      "count": { "$sum": 1 }}},{
    "$group": {
      "_id": "$_id.brand",
      "count_negotiable": { "$sum": { "$cond": [{ "$eq": ["$_id.negotiable", 1] }, "$count", 0] } },
      "count_all": { "$sum": "$count" }}},{
    "$project": {"_id": 0,"brand": "$_id","percentage_negotiable": { "$multiply": [{ "$divide": ["$count_negotiable", "$count_all"] }, 100] }}}]')


#Question 7
MB$aggregate('[{"$match": {"ad_data.Price" : {"$gte": 260, "$lte":8000}}},{
    "$group": {"_id": "$metadata.brand","avg_price": {"$avg": "$ad_data.Price"}}},
  { "$sort": { "avg_price": -1 } },
  { "$limit": 1 },
  { "$project": { "_id": "$_id", "max_brand_average": "$avg_price" } }]')


#Question 8
MB$aggregate('[{"$match": {"ad_data.Price": {"$gte": 260,"$lte": 8000}}},{
    "$group": {"_id": "$metadata.model","avg_age": {"$avg": {"$add": [2023,{"$multiply": [-1,"$ad_data.Year"]}]} }}},{ "$sort": { "avg_age": -1 } },{ "$limit": 10 },
  { "$project": { "_id": "$_id", "top10_models_age_average": "$avg_age" } }]')

#Question 9
MB$aggregate('[{"$match": {"extras": {"$regex": "ABS"},"ad_data.Price": {"$gte": 260,"$lte": 8000}}},
             {"$group": {"_id": null,"count_bikes_with_ABS": {"$sum": 1}}}]')


#Question 10
MB$aggregate('[{"$match": {"extras": {"$regex": "ABS"},"extras": {"$regex": "Led lights"},"ad_data.Price": {"$gte": 260,"$lte": 8000},"ad_data.Mileage": {"$gte": 22,"$lte": 83000}}},
             {"$group": {"_id": null,"average_mileage": {"$avg": "$ad_data.Mileage"}}}]')

