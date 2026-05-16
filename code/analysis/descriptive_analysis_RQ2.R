


#RQ 2: DESCRIPTIVE ANALYSIS#
setwd(dirname(dirname(dirname(rstudioapi::getActiveDocumentContext()$path)))) #trying to deal with the directory issue

#load in data 
campaign_text_results_raw = read.csv("data/results/campaign_site_text_results.csv") #encoding to deal with null bytes, but not sure what that did
combined_raw = read.csv("data/results/candidates_with_magnitude.csv")

combined <- combined_raw %>%
  filter(!race_type %in% c("State Supreme Court"),
         year %in% c(2023, 2024, 2025),
         campaign_ai_magnitude != "no_relevant_data")

#add an AI_vs_not column for the comparison between human and AI+mixed
campaign_text_results$AI_vs_not <- ifelse(campaign_text_results$text_AI_result != "Human", "Non-human", campaign_text_results$text_AI_result)

#### BIVARIRATE COMPARISONS FOR HUMAN VS NON-HUMAN ACROSS ALL TEXT AND IMAGE CONTENT #### 
#this should use the bivariate adoption indicator?
#or, keep it at the content level so it's like the table in the appendix. I think do both 

pct_non_human <- function(df, group_var) {
  groups <- split(df, df[[group_var]])
  sapply(groups, function(g) nrow(g[g$AI_vs_not != "Human", ]) / nrow(g))
}

#### BIVARIATE COMPARISONS FOR HUMAN VS NON-HUMAN IN CAMPAIGN TEXT ####


#Loser vs Winner
is_winner_filter <- campaign_text_results[campaign_text_results$is_winner != "unknown",]
pct_non_human(is_winner_filter, "is_winner")
is_winner_table <- table(is_winner_filter$is_winner, is_winner_filter$AI_vs_not)
chisq.test(is_winner_table)

#Dem vs Rep
party_filter <- campaign_text_results[campaign_text_results$party %in% c("Democrat", "Republican"), ]
pct_non_human(party_filter, "party")
party_table <- table(party_filter$party, party_filter$AI_vs_not)
chisq.test(party_table)

#Incumbency status
incumbency_filter <- campaign_text_results[campaign_text_results$incumbency_status != "Unknown",]
pct_non_human(incumbency_filter, "incumbency_status")
incumbency_table <- table(incumbency_filter$incumbency_status, incumbency_filter$AI_vs_not)
chisq.test(incumbency_table)

#Special election
pct_non_human(campaign_text_results, "special_election")
special_election_table <- table(campaign_text_results$special_election, campaign_text_results$AI_vs_not)
chisq.test(special_election_table)

#Compliance required 
pct_non_human(campaign_text_results, "required_compliance")
compliance_table <- table(campaign_text_results$required_compliance, campaign_text_results$AI_vs_not)
chisq.test(compliance_table)

#### TWEETS ####
tweet_results_raw = read.csv("data/results/tweets_results.csv")
tweet_results <- tweet_results_raw %>%
  filter(!race_type %in% c("State Supreme Court"),
         year %in% c(2023, 2024, 2025))

text_tweets <- tweet_results[!is.na(tweet_results$text_AI_result) & tweet_results$text_AI_result != "", ]

text_tweets$AI_vs_not <- ifelse(text_tweets$text_AI_result != "Human", "Non-human", text_tweets$text_AI_result)
View(text_tweets)

#Loser vs Winner
is_winner_filter_tweets <- text_tweets[text_tweets$is_winner != "unknown",]
pct_non_human(is_winner_filter_tweets, "is_winner")
is_winner_table_tweets <- table(is_winner_filter_tweets$is_winner, is_winner_filter_tweets$AI_vs_not)
chisq.test(is_winner_table_tweets)

#Dem vs Rep
party_filter_tweets <- text_tweets[text_tweets$party %in% c("Democrat", "Republican"), ]
pct_non_human(party_filter_tweets, "party")
party_table_tweets <- table(party_filter_tweets$party, party_filter_tweets$AI_vs_not)
chisq.test(party_table_tweets)

#Incumbency status
incumbency_filter_tweets <- text_tweets[text_tweets$incumbency_status != "Unknown",]
pct_non_human(incumbency_filter_tweets, "incumbency_status")
incumbency_table_tweets <- table(incumbency_filter_tweets$incumbency_status, incumbency_filter_tweets$AI_vs_not)
chisq.test(incumbency_table_tweets)

#Special election
#I lowkey think we can't consider the special election for tweets because the data we gather isn't necessarily at the time of the election. While a campaign site has a higher 
#likelihood of being stable at whatever time we find it 
pct_non_human(text_tweets, "special_election")
special_election_table_tweets <- table(text_tweets$special_election, text_tweets$AI_vs_not)
chisq.test(special_election_table_tweets)

#Compliance required 
pct_non_human(text_tweets, "required_compliance")
compliance_table <- table(text_tweets$required_compliance, text_tweets$AI_vs_not)
chisq.test(compliance_table)
