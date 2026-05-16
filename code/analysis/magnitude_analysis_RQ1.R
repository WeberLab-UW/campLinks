
library(ggplot2)
library(dplyr)

# Source - https://stackoverflow.com/a/25313354
# Posted by ManneR, modified by community. See post 'Timeline' for change history
# Retrieved 2026-04-23, License - CC BY-SA 3.0
detach(package:plyr)    
library(dplyr)
library(car) #Levene test

setwd(dirname(dirname(dirname(rstudioapi::getActiveDocumentContext()$path)))) #trying to deal with the directory issue

#load in data 
campaign_results_raw = read.csv("data/results/campaign_site_results.csv") 
tweets_raw =  read.csv("data/results/tweets_ai_labeled.csv")
combined_raw = read.csv("data/results/candidates_with_magnitude.csv")

#### DATA CLEANING ####

#drop state supreme court
#only keep years 2023, 2024, 2025
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

#keep a combined dataframe for broad data analysis
#this will also need to combine by candidate so that we keep features of candidates (e.g. political party, funding) and don't accidentally repeat

#split each modalility by text and image
text_camp_site <- campaign_results[campaign_results$content_type == "text",]
image_camp_site <- campaign_results[campaign_results$content_type == "image",]

text_tweets <- tweets[!is.na(tweets$text_AI_result) & tweets$text_AI_result != "", ]
image_tweets <- tweets[!is.na(tweets$image_AI_result) & tweets$image_AI_result != "", ]
View(image_tweets)

nrow(image_tweets[image_tweets$image_AI_result == "yes",]) / nrow(image_tweets)

#remove image detection that was errors + turn the "yes" and "no" of image_AI_detection to yes = 1 and no = 0 (1 = AI detected)
campaign_results <- campaign_results[!campaign_results$image_AI_result %in% c("output error", "API error"), ]
campaign_results$image_AI_result <- ifelse(campaign_results$image_AI_result == "yes", 1, 0)

image_tweets <- tweets[!tweets$image_AI_result %in% c("output error", "API error"), ]
image_tweets$image_AI_result  <- ifelse(tweets$image_AI_result == "yes", 1, 0)            

#total samples:
nrow(text_camp_site)
nrow(image_camp_site)
nrow(text_tweets)
nrow(image_tweets)

#total candidates:
length(unique(text_camp_site$candidate_name))
length(unique(image_camp_site$candidate_name))
length(unique(text_tweets$candidate_name))

#average token length:
mean(text_camp_site$token_length)
mean(text_tweets$token_length)

#breakdown of page types
prop.table(table(campaign_results$page_type)) * 100

#breakdown of subject to specific laws
table(campaign_results$required_compliance)
table(tweets$required_compliance)


#### MAGNITUDE FUNCTIONS ####

#magnitude for text content
mean_ai_assistance_text <- function(table, candidate) {
  given_rows <- table[table$candidate_name == candidate, ]
  
  numerator_text <- sum(given_rows$token_length * given_rows$assistance_score)
  denom_text <- sum(given_rows$token_length)
  m_prob <- numerator_text / denom_text
  
  return(m_prob)
}

#magnitude for image content
mean_ai_assistance_image  <- function(table, candidate) {
  given_rows <- table[table$candidate_name == candidate, ]
  m_prob <- sum(given_rows$image_AI_result) / nrow(given_rows)
  
  return(m_prob)
}



#### OVERALL MAGNITUDE ANALYSIS ####
#data table being used: combined

graph_combined <-  combined %>%
  filter(!race_type %in% c("Attorney General", "Governor"))

graph_combined$campaign_ai_magnitude <- as.numeric(graph_combined$campaign_ai_magnitude)

graph_combined %>%
  group_by(year, race_type) %>%
  summarise(avg_magnitude = mean(campaign_ai_magnitude, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = factor(year), y = avg_magnitude, color = race_type, group = race_type)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2.5) +
  scale_x_discrete(
    breaks = unique(graph_combined$year),
    expand = c(0.05, 0.05)
  ) +
  scale_y_continuous(
    limits = c(0, 0.3),
    breaks = seq(0, 0.3, by = 0.1)
  ) +
  labs(
    title = "Magnitude of AI Detection in Campaign Text and Images",
    x = "Year",
    y = "Average Magnitude of AI Assistance",
    color = "Race Type"
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(size = 27, hjust = 0.5, face = "bold"),
    axis.title.x = element_text(size = 23),
    axis.title.y = element_text(size = 23, color = "black"),
    axis.text.x = element_text(angle = 0, hjust = 0.5, size = 20, color = "black"),
    axis.text.y = element_text(size = 20, color = "black"),
    legend.title = element_blank(),
    legend.text = element_text(size = 22),
    legend.position = c(0.25, 0.75),
    legend.key.spacing.y = unit(5, "pt"),
    legend.box.background = element_blank(),
    axis.line = element_line(color = "black"),
  )


graph_data <- graph_combined %>%
  group_by(year, race_type) %>%
  summarise(avg_magnitude = mean(campaign_ai_magnitude, na.rm = TRUE), .groups = "drop")

label_data <- graph_data %>%
  filter((race_type == "US Senate" & year == 2024) |
           (race_type != "US Senate" & year == 2025))

ggplot(graph_data, aes(x = factor(year), y = avg_magnitude, color = race_type, 
                       group = race_type, linetype = race_type)) +
  geom_line(linewidth = 1.8) +
  geom_point(aes(shape = race_type), size = 4) +
  geom_text(data = label_data, aes(label = race_type), hjust = -0.1, size = 6) +
  scale_color_manual(values = c("grey10", "grey20", "grey30", "grey40", "grey50")) +
  scale_linetype_manual(values = c("solid", "dashed", "dotted", "dotdash", "longdash")) +
  scale_shape_manual(values = c(16, 17, 15, 18, 8)) +
  scale_x_discrete(
    breaks = unique(graph_combined$year),
    expand = expansion(add = c(0.3, 0.7))
  ) +
  scale_y_continuous(
    limits = c(0, 0.32),
    breaks = seq(0, 0.3, by = 0.1)
  ) +
  labs(
    title = "Change in Magnitude over Time",
    x = "Year",
    y = "Average Magnitude of AI Assistance"
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(size = 25, hjust = 0.5, face = "bold", margin = margin(b = 20)),
    axis.title.y = element_text(size = 23, color = "black", margin = margin(r = 20)),
    axis.title.x = element_text(size = 23, color = "black", margin = margin(t = 15)),
    axis.text.x = element_text(angle = 0, hjust = 0.5, size = 20, color = "black"),
    axis.text.y = element_text(size = 20, color = "black"),
    legend.position = "none",
    axis.line = element_line(color = "black"),
  )

ggsave("overall_magnitude.svg")

#Digging deeper into some of these trends:
us_house <- combined[combined$race_type == "US House",]
nrow(us_house[us_house$year == 2024,])

View(us_house[us_house$year == 2025,])

#this will have to be updated to first be a text only content_type table
nrow(text_camp_site[text_camp_site$text_AI_result != "Human",]) / nrow(text_camp_site)
nrow(image_camp_site[image_camp_site$image_AI_result == "yes",]) / nrow(image_camp_site)
nrow(text_tweets[text_tweets$text_AI_result != "Human",]) / nrow(text_tweets)

#add together rows from TEXT campaign_results and X_results where text_AI_result = non-human or image_AI_detection = year 
(nrow(text_camp_site[text_camp_site$text_AI_result != "Human",]) + nrow(image_camp_site[image_camp_site$image_AI_result == "yes",]) ) /nrow(campaign_results)


#add together rows from IMAGES campaign_results and X_results where text_AI_result = non-human or image_AI_detection = year 

#add together rows from CAMPAIGNS and X where TEXT = AI
(nrow(text_tweets[text_tweets$text_AI_result != "Human",]) + nrow(text_camp_site[text_camp_site$text_AI_result != "Human",]))/  (nrow(text_tweets) +  nrow(text_camp_site))

#### LABELS ANALYSIS ####

#this will have to be updated to be text_AI_result != "Human" and also where image_AI_detection == "yes"
# Calculate percentage of race_types per page_type where text_AI_result != "Human"

df_filtered_campaign_text <- campaign_text_results %>%
  dplyr::group_by(page_type, race_type) %>%
  dplyr::summarise(
    total = dplyr::n(),
    non_human = sum(text_AI_result != "Human"),
    percentage = non_human / total * 100,
    .groups = "drop"
  ) %>%
  dplyr::mutate(
    race_type = as.factor(race_type),
    page_type = as.factor(page_type)
  )


# Bar plot
ggplot(df_filtered_campaign_text, aes(x = race_type, y = percentage, fill = page_type)) +
  geom_bar(stat = "identity", position = position_dodge(preserve = "single"), color = "black") +
  geom_text(
    aes(label = sprintf("%.1f%%", percentage)),
    position = position_dodge(width = 0.9),
    vjust = -0.5,
    size = 5
  ) +
  scale_y_continuous(limits = c(0, 13), expand = c(0, 0)) +
  labs(
    title = "Detected AI Use Across Campaign Text by Page Type",
    x = "Race Type",
    y = "% AI label ≠ Human",
    fill = "Page Type"
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(size = 27, hjust = 0.5, face = "bold"),
    axis.title.x = element_text(size = 23),
    axis.title.y = element_text(size = 23, color = "black"),
    axis.text.x = element_text(angle = 45, hjust = 1, size = 20, color = "black"),
    axis.text.y = element_text(size = 20, color = "black"),
    legend.title = element_blank(),
    legend.text = element_text(size = 22),
    legend.position = c(0.95, 0.95),
    legend.key.spacing.y = unit(5, "pt"),
    legend.justification = c("right", "top"),
    legend.box.background = element_blank(),
    axis.line = element_line(color = "black"),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )
ggsave("AI_across_campaign_pages.pdf")

#tests of signifiance per race for if the page types are significantly different percentages 



#### MAGNITUDE ANALYSIS ####
magnitude_per_camp_site <- campaign_results %>%
  dplyr::distinct(candidate_name, race_type, year) %>%
  dplyr::group_by(candidate_name) %>%
  dplyr::slice(1) %>%  # keeps first occurrence if duplicates
  ungroup() %>%
  rowwise() %>%
  dplyr::mutate(
    mean_ai_assist = mean_ai_assistance(campaign_results, candidate_name)
  ) %>%
  ungroup()

#graph change in magnitude over time
race_and_year_camp_site_averaging <- magnitude_per_camp_site %>%
  group_by(race_type, year) %>%
  summarise(mean_ai_assist = mean(unlist(mean_ai_assist), na.rm = TRUE))

race_and_year_camp_site_averaging <-  race_and_year_camp_site_averaging %>%
  filter(!race_type %in% c("Governor", "Attorney General"))

View(race_and_year_camp_site_averaging)

# Plot
ggplot(race_and_year_camp_site_averaging, aes(x = factor(year), y = mean_ai_assist, color = race_type, group = race_type)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2.5) +
  scale_x_discrete(breaks = unique(campaign_text_results$year)) +
  labs(
    title = "Magnitude of AI Detection in Campaign Text and Images",
    x = "Year",
    y = "Average Magnitude of AI Assistance",
    color = "Race Type"
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(size = 27, hjust = 0.5, face = "bold"),
    axis.title.x = element_text(size = 23),
    axis.title.y = element_text(size = 23, color = "black"),
    axis.text.x = element_text(angle = 0, hjust = 0.5, size = 20, color = "black"),
    axis.text.y = element_text(size = 20, color = "black"),
    legend.title = element_blank(),
    legend.text = element_text(size = 22),
    legend.position = "right",
    legend.key.spacing.y = unit(5, "pt"),
    legend.box.background = element_blank(),
    axis.line = element_line(color = "black"),
  )

#Plot just text trends for campaign websites
magnitude_per_camp_text <- text_camp_site %>%
  dplyr::distinct(candidate_name, race_type, year) %>%
  dplyr::group_by(candidate_name) %>%
  dplyr::slice(1) %>%  # keeps first occurrence if duplicates
  ungroup() %>%
  rowwise() %>%
  dplyr::mutate(
    mean_ai_assist = mean_ai_assistance_text(text_camp_site, candidate_name)
  ) %>%
  ungroup()

race_and_year_camp_text <- magnitude_per_camp_text %>%
  group_by(race_type, year) %>%
  summarise(mean_ai_assist = mean(unlist(mean_ai_assist), na.rm = TRUE))

race_and_year_camp_text <-  race_and_year_camp_text %>%
  filter(!race_type %in% c("Governor", "Attorney General"))

ggplot(race_and_year_camp_text, aes(x = factor(year), y = mean_ai_assist, color = race_type, group = race_type)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2.5) +
  scale_x_discrete(breaks = unique(race_and_year_camp_text$year)) +
  labs(
    title = "Magnitude of AI Detection in Campaign Text",
    x = "Year",
    y = "Average Magnitude of AI Assistance",
    color = "Race Type"
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(size = 27, hjust = 0.5, face = "bold"),
    axis.title.x = element_text(size = 23),
    axis.title.y = element_text(size = 23, color = "black"),
    axis.text.x = element_text(angle = 0, hjust = 0.5, size = 20, color = "black"),
    axis.text.y = element_text(size = 20, color = "black"),
    legend.title = element_blank(),
    legend.text = element_text(size = 22),
    legend.position = "right",
    legend.key.spacing.y = unit(5, "pt"),
    legend.box.background = element_blank(),
    axis.line = element_line(color = "black"),
  )


#Plot just image trends for campaign websites
magnitude_per_camp_image <- image_camp_site %>%
  dplyr::distinct(candidate_name, race_type, year) %>%
  dplyr::group_by(candidate_name) %>%
  dplyr::slice(1) %>%  # keeps first occurrence if duplicates
  ungroup() %>%
  rowwise() %>%
  dplyr::mutate(
    mean_ai_assist = mean_ai_assistance_image(image_camp_site, candidate_name)
  ) %>%
  ungroup()
View(magnitude_per_camp_image)

race_and_year_camp_image <- magnitude_per_camp_image %>%
  group_by(race_type, year) %>%
  summarise(mean_ai_assist = mean(unlist(mean_ai_assist), na.rm = TRUE))

race_and_year_camp_image <-  race_and_year_camp_image %>%
  filter(!race_type %in% c("Governor", "Attorney General"))

ggplot(race_and_year_camp_image, aes(x = factor(year), y = mean_ai_assist, color = race_type, group = race_type)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2.5) +
  scale_x_discrete(breaks = unique(race_and_year_camp_text$year)) +
  labs(
    title = "Magnitude of AI Detection in Campaign Images",
    x = "Year",
    y = "Average Magnitude of AI Assistance",
    color = "Race Type"
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(size = 27, hjust = 0.5, face = "bold"),
    axis.title.x = element_text(size = 23),
    axis.title.y = element_text(size = 23, color = "black"),
    axis.text.x = element_text(angle = 0, hjust = 0.5, size = 20, color = "black"),
    axis.text.y = element_text(size = 20, color = "black"),
    legend.title = element_blank(),
    legend.text = element_text(size = 22),
    legend.position = "right",
    legend.key.spacing.y = unit(5, "pt"),
    legend.box.background = element_blank(),
    axis.line = element_line(color = "black"),
  )


#Plot text trends for tweet text
magnitude_per_text_tweet<- text_tweets %>%
  dplyr::distinct(candidate_name, race_type, year) %>%
  dplyr::group_by(candidate_name) %>%
  dplyr::slice(1) %>%  # keeps first occurrence if duplicates
  ungroup() %>%
  rowwise() %>%
  dplyr::mutate(
    mean_ai_assist = mean_ai_assistance_text(text_tweets, candidate_name)
  ) %>%
  ungroup()

race_and_year_text_tweets <- magnitude_per_text_tweet %>%
  group_by(race_type, year) %>%
  summarise(mean_ai_assist = mean(unlist(mean_ai_assist), na.rm = TRUE))

race_and_year_text_tweets <-  race_and_year_text_tweets %>%
  filter(!race_type %in% c("Governor", "Attorney General"))

ggplot(race_and_year_text_tweets, aes(x = factor(year), y = mean_ai_assist, color = race_type, group = race_type)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2.5) +
  scale_x_discrete(breaks = unique(race_and_year_text_tweets$year)) +
  labs(
    title = "Magnitude of AI Detection in X Posts (Text)",
    x = "Year",
    y = "Average Magnitude of AI Assistance",
    color = "Race Type"
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(size = 27, hjust = 0.5, face = "bold"),
    axis.title.x = element_text(size = 23),
    axis.title.y = element_text(size = 23, color = "black"),
    axis.text.x = element_text(angle = 0, hjust = 0.5, size = 20, color = "black"),
    axis.text.y = element_text(size = 20, color = "black"),
    legend.title = element_blank(),
    legend.text = element_text(size = 22),
    legend.position = "right",
    legend.key.spacing.y = unit(5, "pt"),
    legend.box.background = element_blank(),
    axis.line = element_line(color = "black"),
  )



#measuring significance between magnitude for campaigns in 2023 to campaigns in 2024 
state_house_2023 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "State House" & magnitude_per_camp_site$year == 2023, ]
state_house_2024 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "State House" & magnitude_per_camp_site$year == 2024, ]

state_senate_2023 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "State Senate" & magnitude_per_camp_site$year == 2023, ]
state_senate_2024 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "State Senate" & magnitude_per_camp_site$year == 2024, ]
state_senate_2025 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "State Senate" & magnitude_per_camp_site$year == 2025, ]

wilcox.test(state_senate_2023$mean_ai_assist, state_senate_2024$mean_ai_assist)

mayor_2023 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "Mayor" & magnitude_per_camp_site$year == 2023, ]
mayor_2024 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "Mayor" & magnitude_per_camp_site$year == 2024, ]
mayor_2025 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "Mayor" & magnitude_per_camp_site$year == 2025, ]

wilcox.test(mayor_2023$mean_ai_assist, mayor_2024$mean_ai_assist)
wilcox.test(mayor_2024$mean_ai_assist, mayor_2025$mean_ai_assist)
wilcox.test(mayor_2023$mean_ai_assist, mayor_2025$mean_ai_assist)


#Plot just text trends for tweets
magnitude_per_tweet_text <- text_tweets %>%
  dplyr::distinct(candidate_name, race_type, year) %>%
  dplyr::group_by(candidate_name) %>%
  dplyr::slice(1) %>%  # keeps first occurrence if duplicates
  ungroup() %>%
  rowwise() %>%
  dplyr::mutate(
    mean_ai_assist = mean_ai_assistance_text_tweets(text_tweets, candidate_name)
  ) %>%
  ungroup()

race_and_year_text_tweets <- magnitude_per_tweet_text %>%
  group_by(race_type, year) %>%
  summarise(mean_ai_assist = mean(unlist(mean_ai_assist), na.rm = TRUE))

race_and_year_text_tweets <-  race_and_year_text_tweets %>%
  filter(!race_type %in% c("Governor", "Attorney General"))

ggplot(race_and_year_text_tweets, aes(x = factor(year), y = mean_ai_assist, color = race_type, group = race_type)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2.5) +
  scale_x_discrete(breaks = unique(race_and_year_camp_text$year)) +
  labs(
    title = "Magnitude of AI Detection in Text in Tweets",
    x = "Year",
    y = "Average Magnitude of AI Assistance",
    color = "Race Type"
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(size = 27, hjust = 0.5, face = "bold"),
    axis.title.x = element_text(size = 23),
    axis.title.y = element_text(size = 23, color = "black"),
    axis.text.x = element_text(angle = 0, hjust = 0.5, size = 20, color = "black"),
    axis.text.y = element_text(size = 20, color = "black"),
    legend.title = element_blank(),
    legend.text = element_text(size = 22),
    legend.position = "right",
    legend.key.spacing.y = unit(5, "pt"),
    legend.box.background = element_blank(),
    axis.line = element_line(color = "black"),
  )







#### BINARY ADOPTION INDICATOR ####

#average amount of text samples per candidate

#arbitrarily setting the binary adoption indicator as ?












