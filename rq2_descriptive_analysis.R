
#RQ 2: DESCRIPTIVE ANALYSIS#
combined_df = read.csv("/Users/agueorg/Desktop/WeberLab/campLinks/detection/campaign_site_content_export.csv")
View(combined_df)

data_with_funding <- combined_df[!is.na(combined_df$total_funding), ]
View(data_with_funding)

table(combined_df$party)
test <- combined_df[combined_df$race_type == "State House",]

View(test[test$special_election == 1,])

#### BIVARIATE COMPARISON BETWEEN LOSERS AND WINNERS ####
losers <- combined_df[combined_df$is_winner == "lost",]
winners <- combined_df[combined_df$is_winner == "won",]

nrow(losers[losers$AI_label != "Human",]) / nrow(losers)
nrow(winners[winners$AI_label != "Human",]) / nrow(winners)

#not sure how to compare their significance - need to see stanford stuff 


#### BIVARIATE COMPARISON BETWEEN HUMAN AND NON-HUMAN ####
human_labeled <- combined_df[combined_df$AI_label == "Human",]
non_human_labeled <- combined_df[combined_df$AI_label != "Human",]

nrow(human_labeled[human_labeled$is_winner == "won",]) / nrow(human_labeled)
nrow(non_human_labeled[non_human_labeled$is_winner == "won",]) / nrow(non_human_labeled)



ggplot(combined_df) +
  aes(x = incumbency_status, fill = AI_label) +
  geom_bar()

#run into difference of proportion