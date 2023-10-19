'''
File containing the handlers for each source and destinaion.
'''
import json
from typing import Dict, List

# MODIFY THESE VALUES
class Sources:
    '''
    A lookup for the source name.
    '''
    MySource = 'MY_SOURCE'


# MODIFY THESE VALUES
class Destinations:
    '''
    A lookup for the destination name.
    '''
    MyDestination = 'MY_DESTINATION'


def determine_source(bucket_name: str, file_name: str):
    ''' MODIFY THIS FUNCTION
    Determine which source it's coming from based on the file.
    :returns: Sources "enum"
    '''
    return Sources.MySource


# ----------- File Handlers. -----------#
'''
Takes a row of lines from a file and format them into a dict.
Rows are strings. Thus can be parsed as JSON or CSV or TSV or anything else.
'''
def get_parser(source: str) -> List[Dict]:
    parsers = {}
    parsers[Sources.MySource] = json_handler
    return parsers[source]


def json_handler(data: List[str])-> List[Dict]:
    return [json.loads(s) for s in data]


# ----------- Row Handlers. ----------- #
'''
Processes the row into a users/track payload
and writes it to the destination queue.

Create a handler for each destination.
'''
def determine_destination(row: Dict) -> str:
    ''' MODIFY THIS FUNCTION
    e.g. if row['venue_name'] == 'splendour': return Destinations.Splendour
    '''
    return Destinations.MyDestination


def get_row_handler(destination: str):
    handlers = {}
    handlers[Destinations.MyDestination] = my_destination_row_handler
    return handlers[destination]


def my_destination_row_handler(row: Dict) -> Dict:
    return


# ----------- Other Processors. -----------#
def chunk_grouper(payloads: Dict):
    '''
    Takes the users/track payloads and groups them
    into requests to optimise Braze's users/track API.
    '''
    grouped_paylaods = {}
    for destination_key, payloads in payloads.items():
        flat = {
            'attributes': [],
            'purchases': [],
            'events': [],
        }
        for payload in payloads:
            for users_track_key, value in payload:
                flat[users_track_key].append(value)
        
        max_len = max(
            len(flat['attributes']), 
            len(flat['purchases']), 
            len(flat['events'])
        )
        step = 75
        users_track_payloads = []
        for start_index in range(0, max_len, step):
            end_index = start_index + step
            users_track_payloads.append({
                'attributes': flat['attributes'][start_index:end_index],
                'purchases':  flat['purchases'][start_index:end_index],
                'events':     flat['events'][start_index:end_index],
            })
        grouped_paylaods[destination_key] = users_track_payloads
    return grouped_paylaods

