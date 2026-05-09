
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
campaign_text_results_raw = read.csv("data/results/campaign_site_text_results.csv") 

#drop state supreme court
#only keep years 2023, 2024, 2025
campaign_text_results <- campaign_text_results_raw %>%
  filter(!race_type %in% c("State Supreme Court", "Attorney General"),
         year %in% c(2023, 2024, 2025))

#total samples:
nrow(campaign_text_results)

#total candidates:
length(unique(campaign_text_results$candidate_name))

#average token length:
mean(campaign_text_results$token_length)

#breakdown of page types
prop.table(table(campaign_text_results$page_type)) * 100

#breakdown of subject to specific laws
table(campaign_text_results$required_compliance)

#### LABELS ANALYSIS ####
# Calculate percentage of race_types per page_type where AI_label != "Human"
df_filtered_campaign_text <- campaign_text_results %>%
  dplyr::group_by(page_type, race_type) %>%
  dplyr::summarise(
    total = dplyr::n(),
    non_human = sum(AI_label != "Human"),
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

#magnitude per a candidates campaign:
mean_detection_probability <- function(table, candidate) {
  given_rows <- table[table$candidate_name == candidate, ]
  numerator <- sum(given_rows$token_length * given_rows$assistance_score)
  denom <- sum(given_rows$token_length)
  m_prob <- numerator / denom
  return(m_prob)
}

magnitude_per_camp_site <- campaign_text_results %>%
  dplyr::distinct(candidate_name, race_type, year) %>%
  dplyr::group_by(candidate_name) %>%
  dplyr::slice(1) %>%  # keeps first occurrence if duplicates
  ungroup() %>%
  rowwise() %>%
  dplyr::mutate(
    mean_det_prob = mean_detection_probability(campaign_text_results, candidate_name)
  ) %>%
  ungroup()

View(magnitude_per_camp_site)


#graph change in magnitude over time
race_and_year_camp_site_averaging <- magnitude_per_camp_site %>%
  group_by(race_type, year) %>%
  summarise(mean_det_prob = mean(unlist(mean_det_prob), na.rm = TRUE))

race_and_year_camp_site_averaging <-  race_and_year_camp_site_averaging %>%
  filter(!race_type %in% c("Governor"))

View(race_and_year_camp_site_averaging)

# Plot
ggplot(race_and_year_camp_site_averaging, aes(x = factor(year), y = mean_det_prob, color = race_type, group = race_type)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2.5) +
  scale_x_discrete(breaks = unique(campaign_text_results$year)) +
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

#measuring significance between magnitude for campaigns in 2023 to campaigns in 2024 
state_house_2023 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "State House" & magnitude_per_camp_site$year == 2023, ]
state_house_2024 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "State House" & magnitude_per_camp_site$year == 2024, ]

state_senate_2023 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "State Senate" & magnitude_per_camp_site$year == 2023, ]
state_senate_2024 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "State Senate" & magnitude_per_camp_site$year == 2024, ]
state_senate_2025 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "State Senate" & magnitude_per_camp_site$year == 2025, ]

wilcox.test(state_senate_2023$mean_det_prob, state_senate_2024$mean_det_prob)


mayor_2023 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "Mayor" & magnitude_per_camp_site$year == 2023, ]
mayor_2024 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "Mayor" & magnitude_per_camp_site$year == 2024, ]
mayor_2025 <- magnitude_per_camp_site[magnitude_per_camp_site$race_type == "Mayor" & magnitude_per_camp_site$year == 2025, ]

wilcox.test(mayor_2023$mean_det_prob, mayor_2024$mean_det_prob)
wilcox.test(mayor_2024$mean_det_prob, mayor_2025$mean_det_prob)
wilcox.test(mayor_2023$mean_det_prob, mayor_2025$mean_det_prob)

#### BINARY ADOPTION INDICATOR ####

#average amount of text samples per candidate

#arbitrarily setting the binary adoption indicator as ?












