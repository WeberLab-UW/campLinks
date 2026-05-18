


#RQ 2: DESCRIPTIVE ANALYSIS#
setwd(dirname(dirname(dirname(rstudioapi::getActiveDocumentContext()$path)))) #trying to deal with the directory issue

#load in data 
campaign_results_raw = read.csv("data/results/campaign_site_final.csv") 
tweets_raw =  read.csv("data/results/tweets_final.csv")
combined_raw = read.csv("data/results/candidates_with_magnitude.csv")


campaign_results <- campaign_results_raw %>%
  filter(!race_type %in% c("State Supreme Court"),
         year %in% c(2023, 2024, 2025))

tweets <- tweets_raw %>%
  filter(!race_type %in% c("State Supreme Court"),
         year %in% c(2023, 2024, 2025))

combined <- combined_raw %>%
  filter(!race_type %in% c("State Supreme Court"),
         year %in% c(2023, 2024, 2025),
         campaign_ai_magnitude != "no_relevant_data")


#drop rows that are API errors
campaign_results <- campaign_results[
  !(campaign_results$content_type == "image" & 
      !is.na(campaign_results$image_AI_result) & 
      campaign_results$image_AI_result == "API error"), 
]

#add in AI_detected column for campaign results
campaign_results$AI_detected <- ifelse(
    (campaign_results$content_type == "image" & campaign_results$image_AI_result == "yes") |
      (campaign_results$content_type == "text"  & campaign_results$text_AI_result  != "Human"),
    1, 0)

#add in AI_detected column for tweets (no API errors in this dataframe)

#but first, do need to drop rows where there is no image_AI_result AND no text_AI_result because that means its text < 70 tokens with no image
tweets_results <- tweets[
  !((is.na(tweets$image_AI_result) | tweets$image_AI_result == "") &
      (is.na(tweets$text_AI_result)  | tweets$text_AI_result  == "")), 
]

### NEED TO GO BACK TO RQ1 AND USE THIS
tweets_results$AI_detected <- ifelse(
  ((tweets_results$text_AI_result  != "" & tweets_results$text_AI_result  != "Human") |
     (tweets_results$image_AI_result != "" & tweets_results$image_AI_result == "yes")),
  1, 0
)


#split each modalility by text and image
text_camp_site <- campaign_results[campaign_results$text_AI_result %in% c("Mixed", "Human", "AI"), ]
unique(text_camp_site$text_AI_result)
nrow(text_camp_site)
image_camp_site <- campaign_results[campaign_results$image_AI_result %in% c("yes", "no"), ]

text_tweets <- tweets[tweets$text_AI_result %in% c("Mixed", "Human", "AI"), ]
nrow(text_tweets)

unique(text_tweets$text_AI_result)
image_tweets <- tweets[tweets$image_AI_result %in% c("yes", "no"), ]

# turn the "yes" and "no" of image_AI_detection to yes = 1 and no = 0 (1 = AI detected)
image_camp_site$image_AI_result <- ifelse(image_camp_site$image_AI_result == "yes", 1, 0)
unique(image_camp_site$image_AI_result)

image_tweets$image_AI_result  <- ifelse(image_tweets$image_AI_result == "yes", 1, 0)        
unique(image_tweets$image_AI_result)

#### BIVARIRATE COMPARISONS FOR HUMAN VS NON-HUMAN ACROSS ALL TEXT AND IMAGE CONTENT #### 
#this should use the bivariate adoption indicator?
#or, keep it at the content level so it's like the table in the appendix. I think do both 

#CONTENT LEVEL 
text_camp_won <- split(text_camp_site, text_camp_site[["race_outcome"]])
colnames(text_camp_won)
nrow(text_camp_won[text_camp_won$text_AI_result != "Human"])
colnames(text_camp_site)

text_camp_won <- text_camp_site[text_camp_site$race_outcome == "won",] 
text_tweets_won <- text_tweets[text_tweets$race_outcome == "won",]
image_camp_won <- image_camp_site[image_camp_site$race_outcome == "won",] 
image_tweets_won <- image_tweets[image_tweets$race_outcome == "won",]

ai_usage_winners <- (nrow(text_camp_won[text_camp_won$text_AI_result != "Human",]) +  nrow(text_tweets_won[text_tweets_won$text_AI_result != "Human",]) +
                          nrow(image_camp_won[image_camp_won$image_AI_result == 1,]) +nrow(image_tweets_won[image_tweets_won$image_AI_result == 1,]))
total_winners <- (nrow(text_camp_won) + nrow(text_tweets_won) + nrow(image_camp_won) + nrow(image_tweets_won))

ai_usage_winners / total_winners

text_camp_lost <- text_camp_site[text_camp_site$race_outcome == "lost",] 
nrow(text_camp_lost[text_camp_lost$text_AI_result != "Human",]) / nrow(text_camp_lost)

ai_usage_rate <- function(filter_col, filter_val) {
  
  # Filter each df by the grouping condition (e.g. race_outcome == "won")
  text_camp_f  <- text_camp_site[text_camp_site[[filter_col]] == filter_val, ]
  text_tweet_f <- text_tweets[text_tweets[[filter_col]] == filter_val, ]
  image_camp_f <- image_camp_site[image_camp_site[[filter_col]] == filter_val, ]
  image_tweet_f <- image_tweets[image_tweets[[filter_col]] == filter_val, ]
  
  # Count AI usage rows
  ai_count <- (
    nrow(text_camp_f[text_camp_f$text_AI_result != "Human", ]) +
      nrow(text_tweet_f[text_tweet_f$text_AI_result != "Human", ]) +
      nrow(image_camp_f[image_camp_f$image_AI_result == 1, ]) +
      nrow(image_tweet_f[image_tweet_f$image_AI_result == 1, ])
  )
  
  total <- nrow(text_camp_f) + nrow(text_tweet_f) + nrow(image_camp_f) + nrow(image_tweet_f)
  
  return(ai_count / total)
}

unique(text_camp_site$required_compliance)

ai_usage_rate("required_compliance", "")

pct_non_human(text_camp_site, "race outcome")


#BIVARIATE COMPARISONS FOR HUMAN VS NON-HUMAN FOR CAMPAIGN CONTENT
pct_non_human_modality <- function(df, group_var) {
  groups <- split(df, df[[group_var]])
  sapply(groups, function(g) nrow(g[g$AI_detected == 1, ]) / nrow(g))
}

#loser vs winner
is_winner_filter <- campaign_results[campaign_results$race_outcome != "unknown",]
pct_non_human_modality(is_winner_filter, "race_outcome")
is_winner_table <- table(is_winner_filter$race_outcome, is_winner_filter$AI_detected)
chisq.test(is_winner_table)

#dem vs republican
party_filter <- campaign_results[campaign_results$party %in% c("Democrat", "Republican"), ]
pct_non_human_modality(party_filter, "party")
party_table <- table(party_filter$party, party_filter$AI_detected)
chisq.test(party_table)

#Incumbency status
incumbency_filter <- campaign_results[campaign_results$incumbency_status != "Unknown",]
pct_non_human_modality(incumbency_filter, "incumbency_status")
incumbency_table <- table(incumbency_filter$incumbency_status, incumbency_filter$AI_detected)
chisq.test(incumbency_table)

pairwise_chisq <- function(tbl) {
  groups <- rownames(tbl)
  pairs <- combn(groups, 2, simplify = FALSE)
  
  results <- lapply(pairs, function(pair) {
    subtable <- tbl[pair, ]
    test <- chisq.test(subtable, correct = FALSE)
    data.frame(
      group1  = pair[1],
      group2  = pair[2],
      chi_sq  = round(test$statistic, 3),
      p_value = round(test$p.value, 4)
    )
  })
  
  result_df <- do.call(rbind, results)
  result_df$p_adjusted <- round(p.adjust(result_df$p_value, method = "bonferroni"), 10)
  return(result_df)
}

pairwise_chisq(incumbency_table)

#Special election
pct_non_human_modality(campaign_results, "special_election")
special_election_table <- table(campaign_results$special_election, campaign_results$AI_detected)
chisq.test(special_election_table)

#Compliance required 
pct_non_human_modality(campaign_results, "required_compliance")
compliance_table <- table(campaign_results$required_compliance, campaign_results$AI_detected)
chisq.test(compliance_table)x


#BIVARIATE COMPARISONS FOR HUMAN VS NON-HUMAN FOR X

#loser vs winner
is_winner_filter_X <- tweets_results[tweets_results$race_outcome != "unknown",]
pct_non_human_modality(is_winner_filter_X, "race_outcome")
is_winner_table_X <- table(is_winner_filter_X$race_outcome, is_winner_filter_X$AI_detected)
chisq.test(is_winner_table_X)

#dem vs republican
party_filter_X <- tweets_results[tweets_results$party %in% c("Democrat", "Republican"), ]
pct_non_human_modality(party_filter_X, "party")
party_table_X <- table(party_filter_X$party, party_filter_X$AI_detected)
chisq.test(party_table_X)

#Incumbency status
incumbency_filter_X <- tweets_results[tweets_results$incumbency_status != "Unknown",]
pct_non_human_modality(incumbency_filter_X, "incumbency_status")
incumbency_table_X <- table(incumbency_filter_X$incumbency_status, incumbency_filter_X$AI_detected)
chisq.test(incumbency_table_X)

#pairwise
pairwise_chisq(incumbency_table_X)

#Special election
pct_non_human_modality(tweets_results, "special_election")
special_election_table_X <- table(tweets_results$special_election, tweets_results$AI_detected)
chisq.test(special_election_table_X)

#Compliance required 
pct_non_human_modality(tweets_results, "required_compliance")
compliance_table_X <- table(tweets_results$required_compliance, tweets_results$AI_detected)
chisq.test(compliance_table_X)


#### DIFFERENCES IN FUNDING ####

#who do we have funding data on
who_has_funding <- combined[!is.na(combined$total_funding), ]
nrow(who_has_funding)
unique(who_has_funding$race_type)

#tweets and funding averages 
tweets_detected_AI <- tweets_results[tweets_results$AI_detected == 1,]
mean(tweets_detected_AI$total_funding, na.rm = TRUE)

tweets_no_detected <- tweets_results[tweets_results$AI_detected == 0,]
mean(tweets_no_detected$total_funding, na.rm = TRUE)

#campaign sites and funding averages 
campaign_sites_AI <- campaign_results[campaign_results$AI_detected ==1,]
mean(campaign_sites_AI$total_funding, na.rm = TRUE)

campaign_sites_no_AI <- campaign_results[campaign_results$AI_detected ==0,]
mean(campaign_sites_no_AI$total_funding, na.rm = TRUE)

#### BIVARIATE COMPARISONS FOR HUMAN VS NON-HUMAN IN CAMPAIGN TEXT ####
pct_non_human <- function(df, group_var) {
  groups <- split(df, df[[group_var]])
  sapply(groups, function(g) nrow(g[g$text_AI_result != "Human", ]) / nrow(g))
}

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
