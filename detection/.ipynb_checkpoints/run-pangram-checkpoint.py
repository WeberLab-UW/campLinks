
#look at scraping data
import pandas as pd
from pangram import Pangram
import numpy as np
import random
import re
from collections import Counter #for extracting labels and confidence majorities with pangram
import os
from dotenv import load_dotenv
import sqlite3

load_dotenv()
pangram_client = Pangram(api_key=os.getenv("PANGRAM_API_KEY"))

# query for the 5 sampled texts of state house candidates

#add in getting candidate name too 
#having a hard time adding candidate name in the correct data base
QUERY = """
    SELECT t.candidate_id, t.page_type, t.sampled_text, c.candidate_name
    FROM text t
    LEFT JOIN candidates c
    ON t.candidate_id = c.id;
"""
#save the sampled text to an array
DB_PATH = "/Users/agueorg/Desktop/WeberLab/anna-RA/candidate-scraping/media_db.db"
with sqlite3.connect(DB_PATH) as conn:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(QUERY).fetchall()

candidate_ids = [row[0] for row in rows]
page_type = [row[1] for row in rows]
sampled_texts = [row[2] for row in rows]
candidate_names = [row[3] for row in rows]




#pangrams AI detection code 
def check_AI(text):
    try:
        result = pangram_client.predict(text)
        valid_API_call = True
    except:
        fraction_ai = None
        fraction_ai_assisted = None
        fraction_human = None
        num_ai_segments = None
        label_final = None
        ai_assistance_final = None
        confidence_final = None
        
    
    if valid_API_call:
        # V3 analysis with AI-assistance detection
        fraction_ai = result['fraction_ai']
        fraction_ai_assisted = result['fraction_ai_assisted']
        fraction_human = result['fraction_human']
        num_ai_segments = result['num_ai_segments']
        # Access individual window classifications
        label_all = []
        ai_assistance_all = []
        confidence_all = []

        for window in result['windows']:
            label = window['label'] #this is just saving the final label in the window, NOT the final label ?? 
            label_all.append(label)

            ai_assistance_score = window['ai_assistance_score'] #same with these
            ai_assistance_all.append(ai_assistance_score)

            confidence = window['confidence']
            confidence_all.append(confidence)

        #take the majority of label for final label
        label_final = Counter(label_all).most_common(1)[0][0]

        #take the average of the final ai_assistance scores 
        ai_assistance_final = np.mean(ai_assistance_all)

        #take the majority of confidence for final confidence 
        confidence_final = Counter(confidence_all).most_common(1)[0][0]
        print(ai_assistance_final, confidence_final, num_ai_segments)

    return [label_final, ai_assistance_final, confidence_final, fraction_ai, fraction_ai_assisted, fraction_human, num_ai_segments]


