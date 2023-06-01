install.packages("redux")
library("redux")

#Remote Connection
r <- redux::hiredis(
  redux::redis_config(
    host = "localhost", 
    port = "6379"
    ))

#load data
emails_sent<- read.csv("/Users/thanosalexandris/Downloads/RECORDED_ACTIONS/emails_sent.csv")
modified_listings <- read.csv("/Users/thanosalexandris/Downloads/RECORDED_ACTIONS/modified_listings.csv")


#Overview of datasets,check for nulls or missing values
View(emails_sent)
View(modified_listings)


modified_listings[is.na(modified_listings)]
modified_listings[is.null(modified_listings)]

length(modified_listings[which(modified_listings$UserID=="")])
length(modified_listings[which(modified_listings$MonthID=="")])
length(modified_listings[which(modified_listings$ModifiedListing=="")])


emails_sent[is.na(emails_sent)]
emails_sent[is.na(emails_sent)]


length(emails_sent[which(emails_sent$EmailID=="")])
length(emails_sent[which(emails_sent$UserID=="")])
length(emails_sent[which(emails_sent$MonthID=="")])
length(emails_sent[which(emails_sent$EmailOpened=="")])


str(modified_listings)

summary(modified_listings)

r$FLUSHALL()

##q1 An:9969
for (i in 1:length(modified_listings$UserID)){
  if ((modified_listings$MonthID[i]==1)&(modified_listings$ModifiedListing[i]==1)){
    r$SETBIT("ModificationsJanuary",modified_listings$UserID[i],"1")
  }
}

r$BITCOUNT("ModificationsJanuary")

##q2 An:10031

r$BITOP("NOT","NonModificationsJanuary","ModificationsJanuary")

r$BITCOUNT("NonModificationsJanuary")

9969+10031
length(unique(modified_listings$UserID))

19997/8

for (i in 1:length(modified_listings$UserID)){
  if ((modified_listings$MonthID[i]==1)&(modified_listings$ModifiedListing[i]==0)){
    r$SETBIT("NoModificationsJanuary",modified_listings$UserID[i],"1")
  }
}

r$BITCOUNT("NoModificationsJanuary")


##The amount of unique users is 19999 and the sum of ModificationsJanuary and NonModificationsJanuary is 20.000 .



##Q3 An:2668
for (i in 1:length(emails_sent$UserID)){
  if (emails_sent$MonthID[i]==1){
    r$SETBIT("EmailsJanuary",emails_sent$UserID[i],"1")
  }
}

for (i in 1:length(emails_sent$UserID)){
  if (emails_sent$MonthID[i]==2){
    r$SETBIT("EmailsFebruary",emails_sent$UserID[i],"1")
  }
}

for (i in 1:length(emails_sent$UserID)){
  if (emails_sent$MonthID[i]==3){
    r$SETBIT("EmailsMarch",emails_sent$UserID[i],"1")
  }
}

r$BITOP("AND","AtLeastOnePerMonth",c("EmailsJanuary","EmailsFebruary","EmailsMarch"))

r$BITCOUNT("AtLeastOnePerMonth")


##Q4 An:2417

r$BITOP("NOT","NoneEmailsFebruary","EmailsFebruary")

r$BITOP("AND","AtLeastOneJanMArchNoFeb",c("EmailsJanuary","NoneEmailsFebruary","EmailsMarch"))

r$BITCOUNT("AtLeastOneJanMArchNoFeb")

##Q5 An:2807

for (i in 1:length(emails_sent$UserID)){
  if ((emails_sent$MonthID[i]==1) & (emails_sent$EmailOpened[i]==0)){
    r$SETBIT("EmailsReceivedNotOpenedJanuary",emails_sent$UserID[i],"1")
  }
}

r$BITOP("AND","ModificationsAndNotReceivedJanuary",c("ModificationsJanuary","EmailsReceivedNotOpenedJanuary"))
r$BITCOUNT("ModificationsAndNotReceivedJanuary")

##Q6 An:7221

for (i in 1:length(emails_sent$UserID)){
  if ((emails_sent$MonthID[i]==2) & (emails_sent$EmailOpened[i]==0)){
    r$SETBIT("EmailsReceivedNotOpenedFebruary",emails_sent$UserID[i],"1")
  }
}

for (i in 1:length(modified_listings$UserID)){
  if ((modified_listings$MonthID[i]==2)&(modified_listings$ModifiedListing[i]==1)){
    r$SETBIT("ModificationsFebruary",modified_listings$UserID[i],"1")
  }
}

r$BITOP("AND","ModificationsAndNotReceivedFebruary",c("ModificationsFebruary","EmailsReceivedNotOpenedFebruary"))


for (i in 1:length(emails_sent$UserID)){
  if ((emails_sent$MonthID[i]==3) & (emails_sent$EmailOpened[i]==0)){
    r$SETBIT("EmailsReceivedNotOpenedMarch",emails_sent$UserID[i],"1")
  }
}

for (i in 1:length(modified_listings$UserID)){
  if ((modified_listings$MonthID[i]==3)&(modified_listings$ModifiedListing[i]==1)){
    r$SETBIT("ModificationsMarch",modified_listings$UserID[i],"1")
  }
}

r$BITOP("AND","ModificationsAndNotReceivedMarch",c("ModificationsMarch","EmailsReceivedNotOpenedMarch"))

r$BITOP("OR","ModificationsAndNotReceivedJanOrFebOrMarch",c("ModificationsAndNotReceivedJanuary","ModificationsAndNotReceivedFebruary","ModificationsAndNotReceivedMarch"))

r$BITCOUNT("ModificationsAndNotReceivedJanOrFebOrMarch")



##Q7 An: It is important to mention that from the users that receive at least on email on the period January to March, 100% have at least opened one email. Aditionaly the percentage 
##       of users that opened at least an email, and make changes in the same month, is 50%. Hence we believe that both ratios show that the strategy of the company
##       affects users at an important level and it makes sense. However the company should work on it more, to make this strategy even more efficient.

Amount_of_users_that_received_email<-length(unique(emails_sent$UserID))


for (i in 1:length(emails_sent$UserID)){
  if (emails_sent$EmailOpened[i]==1){
    r$SETBIT("EmailsOpenned",emails_sent$UserID[i],"1")
  }
}

Emails_opened<-r$BITCOUNT("EmailsOpenned")

## 100% ratio of users that received at least an email to those who received at least one and oppened one

ratio1<-Emails_opened/Amount_of_users_that_received_email


for (i in 1:length(emails_sent$UserID)){
  if ((emails_sent$MonthID[i]==1) & (emails_sent$EmailOpened[i]==1)){
    r$SETBIT("EmailsReceivedOpenedJanuary",emails_sent$UserID[i],"1")
  }
}

for (i in 1:length(emails_sent$UserID)){
  if ((emails_sent$MonthID[i]==2) & (emails_sent$EmailOpened[i]==1)){
    r$SETBIT("EmailsReceivedOpenedFebruary",emails_sent$UserID[i],"1")
  }
}

for (i in 1:length(emails_sent$UserID)){
  if ((emails_sent$MonthID[i]==3) & (emails_sent$EmailOpened[i]==1)){
    r$SETBIT("EmailsReceivedOpenedMarch",emails_sent$UserID[i],"1")
  }
}

r$BITOP("AND","ModificationsAndOpenedJanuary",c("ModificationsJanuary","EmailsReceivedOpenedJanuary"))

r$BITOP("AND","ModificationsAndOpenedFebruary",c("ModificationsFebruary","EmailsReceivedOpenedFebruary"))

r$BITOP("AND","ModificationsAndOpenedMarch",c("ModificationsMarch","EmailsReceivedOpenedMarch"))

r$BITOP("OR","ModificationsAndOpenedJanOrFebOrMarch",c("ModificationsAndOpenedJanuary","ModificationsAndOpenedFebruary","ModificationsAndOpenedMarch"))

r$BITCOUNT("ModificationsAndOpenedJanOrFebOrMarch")

ratio_openedANDmodified<-r$BITCOUNT("ModificationsAndOpenedJanOrFebOrMarch")/(r$BITCOUNT("ModificationsAndOpenedJanOrFebOrMarch")+r$BITCOUNT("ModificationsAndNotReceivedJanOrFebOrMarch"))


