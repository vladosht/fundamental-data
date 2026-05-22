import json
import regex as re
from functools import cache

@cache  #Executed only once, because config_json never changes during a program run
def get_re_patterns(config_json):
    known_facts, unwanted_keys = json.loads(config_json)

    # Convert the lists of strings to binary strings of names, joined by the OR regex operator '|'
    known_facts =   b'|'.join([re.escape(a_fact.split('.')[1].encode()) for a_fact in known_facts])
    unwanted_keys = b'|'.join([re.escape(a_key.encode()) for a_key in unwanted_keys])
    
    # This is how a beginning of a wanted json slice looks like.
    # We are trying to match these for all known facts and remove the others.
    """
    "Assets":{"label":"Assets",
              "description":"Sum of the carrying amounts as of the balance sheet date of all assets that are recognized...",
              "units":{"USD":[{"end":"2024-08-31","val":2783300000,"accn":"0001410578-24-001617","fy":2025,"fp":"Q1","form":"10-Q","filed":"2024-09-24","frame":"CY2024Q3I"},
                              {"end":"2024-11-30","val":2849300000,"accn":"0001410578-25-000003","fy":2025,"fp":"Q2","form":"10-Q","filed":"2025-01-08","frame":"CY2024Q4I"},
                              {"end":"2025-02-28","val":2859100000,"accn":"0001410578-25-000519","fy":2025,"fp":"Q3","form":"10-Q","filed":"2025-03-28","frame":"CY2025Q1I"},
                              ...
    This corresponds to the 'us-gaap.Assets.usd' known fact.
    """

    # Pass 1: Keep wanted facts, remove others
    # (*SKIP)(*FAIL) prevents the regex engine from matching inside target blocks
    fact_body = rb'":\{"label":[^]]*\]\}\}'
    keep_part = b',"(?:' + known_facts + b')' + fact_body
    drop_part = rb',"\w+' + fact_body
    facts_regex = re.compile(keep_part + rb'(*SKIP)(*FAIL)|' + drop_part)
    
    # Pass 2: Mass-remove unwanted keys and their values (can be either strings or integers)
    values_of_keys = rb')":(?:"[^"]*"|[^,}]+)'
    keys_regex = re.compile(rb',"(' + unwanted_keys + values_of_keys)
    
    return facts_regex, keys_regex

def preprocess_json(jfile, config_json):
    facts_regex, keys_regex = get_re_patterns(config_json) # Get the cached regex patterns
    jfile = facts_regex.sub(b'', jfile) #Remove unwanted facts
    jfile = keys_regex.sub(b'', jfile)  #Remove unwanted data fields
    return jfile
