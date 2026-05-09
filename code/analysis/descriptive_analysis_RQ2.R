


#RQ 2: DESCRIPTIVE ANALYSIS#
setwd(dirname(dirname(dirname(rstudioapi::getActiveDocumentContext()$path)))) #trying to deal with the directory issue

#load in data 
campaign_text_results_raw = read.csv("data/results/campaign_site_text_results.csv") #encoding to deal with null bytes, but not sure what that did
View(campaign_text_results_raw)

campaign_text_results <- campaign_text_results_raw %>%
  filter(!race_type %in% c("State Supreme Court", "Attorney General"),
         year %in% c(2023, 2024, 2025))

#add an AI_vs_not column for the comparison between human and AI+mixed
campaign_text_results$AI_vs_not <- ifelse(campaign_text_results$AI_label != "Human", "Non-human", campaign_text_results$AI_label)

#### BIVARIATE COMPARISONS FOR HUMAN VS NON-HUMAN ####
pct_non_human <- function(df, group_var) {
  groups <- split(df, df[[group_var]])
  sapply(groups, function(g) nrow(g[g$AI_vs_not != "Human", ]) / nrow(g))
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
