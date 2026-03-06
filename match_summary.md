# Candidate Name Match Summary

Source: `candidate_names_23_to_25.csv` vs `camplinks.db`

Candidate_names_23_to_25.csv contains federal:house, federal:senate, state:governor, and state:ag names. These are either from validated external sources (MIT election lab) or manually collected (Anna collected for AG and governor). We do have data from followthemoney on state:house and state:senate, but it is not as organized / less validated. 

Below is comparison across the validated name data we do have for 2023-2025 to our database: 

## Match Status

| Status | Count | % of Total |
|---|---|---|
| Matched (name + year) | 597 | 59.5% |
| Incorrect year (name found, year mismatch) | 10 | 1.0% |
| Not found | 396 | 39.5% |
| **Total** | **1,003** | **100%** |

## Contact Link Coverage (among matched candidates, n=597)

| Link Type | Candidates with Link | % of Matched |
|---|---|---|
| campaign_site | 579 | 97.0% |
| campaign_facebook | 188 | 31.5% |
| campaign_x | 165 | 27.6% |
| campaign_instagram | 161 | 27.0% |
| personal_facebook | 166 | 27.8% |
| personal_linkedin | 158 | 26.5% |
| personal_website | 7 | 1.2% |

## Race Type Breakdown (unmatched + incorrect year, n=406)

| Race Type | Count | % of Unmatched |
|---|---|---|
| federal:house | 299 | 73.6% |
| federal:senate | 64 | 15.8% |
| state:ag | 26 | 6.4% |
| state:governor | 17 | 4.2% |

(reminder that the above table does not include state:house and state:senate because we don't have that data in the validated named dataset)