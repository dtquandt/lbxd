import os

import letterboxd
import requests
import pandas as pd
import base62

from concurrent.futures import ThreadPoolExecutor, as_completed

def api_request(path: str):
    """
    Wrapper for the Letterboxd API. Takes a path and returns the response from the API.
    
    Parameters
    ----------
    path : str
        The path to the API endpoint you want to call.
    
    Returns
    -------
    requests.models.Response
        The response from the API.
    """
    LBXD_KEY = os.getenv('LBXD_KEY')
    LBXD_SECRET = os.getenv('LBXD_SECRET')
    API_BASE = 'https://api.letterboxd.com/api/v0'
    
    return letterboxd.api.API(api_base=API_BASE, api_key=LBXD_KEY, api_secret=LBXD_SECRET).api_call(path)


def get_id_from_username(member_name):
    """
    Return a member's ID from their username. This is necessary because the Letterboxd API
    requires member IDs instead of usernames. Raises a ValueError if the API search request
    fails or if no results are found.
    
    Parameters
    ----------
    member_name : str
        The username of the member whose ID you want to pull.
        
    Returns
    -------
    str
        The API member ID of the member whose username you passed in.
    """
    
    head_request = requests.head(f'https://letterboxd.com/{member_name}/')
    status_code = head_request.status_code
    if status_code != 200:
        raise ValueError(f'Request failed when looking up member {member_name}.\
                           Status code: {status_code}')
    res_headers = head_request.headers

    if 'X-Letterboxd-Identifier' not in res_headers:
        raise KeyError(f'Page headers did not include Letterboxd Identifier. Possible change on their side?')
    member_id = res_headers['X-Letterboxd-Identifier']
    return member_id


def get_member_watchlist(member_id):
    """
    Return a member's watchlist from the Letterboxd API.
    
    Parameters
    ----------
    member_name : str, optional
        The username of the member whose watchlist you want to pull.
    member_id : str, optional
        The member ID of the member whose watchlist you want to pull.
        
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the member's watchlist.
    """
        
    full_results = []
    
    def paginate_watchlist(cursor='start=0'):    
        wl_response = api_request(f'member/{member_id}/watchlist?perPage=100&cursor={cursor}')
        wl_response_status = wl_response.status_code
        if wl_response_status != 200:
            raise ValueError(f'Request failed when pulling watchlist for member ID {member_id}.\
                               Status code: {wl_response_status}')
        wl_json = wl_response.json()
        full_results.extend(wl_json['items'])
        if 'next' in wl_json.keys():
            paginate_watchlist(cursor=wl_json['next'])
    
    paginate_watchlist(full_results)
    
    return pd.DataFrame(full_results)


def get_combined_watchlists(member_ids):
    """
    Return the combined watchlists of a list of members.
    
    Parameters
    ----------
    member_ids : list
        A list of member IDs whose watchlists you want to combine.
        
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the combined watchlists of the members you passed in.
    """
    
    watchlists = []
    
    for member_id in member_ids:
        watchlist = get_member_watchlist(member_id)
        watchlists.append(watchlist)
    
    combined_watchlist = pd.concat(watchlists)
    
    return combined_watchlist


def get_member_watches(member_id):
    """
    Return a member's watched films and their ratings, if any, from the Letterboxd API.
    
    Parameters
    ----------
    member_id : str
        The member ID of the member whose watches you want to pull.
        
    Returns
    -------
    pd.DataFrame
        A DataFrame containing the member's watched films and their ratings, if any.
    """
    
    cursor = 'start=0'
    results = {'next':None}
    all_ratings = []

    while True:
        response = api_request(
            f'films/?perPage=100&member={member_id}&memberRelationship=Watched&sort=MemberRatingHighToLow&cursor={cursor}')
        results = response.json()
        for item in results['items']:
            entry = {'member': member_id}
            entry['film'] = item.get('id')
            relationships = item.get('relationships')
            if relationships:
                relationship = relationships[0].get('relationship')
                if relationship:
                    entry['rating'] = relationship.get('rating')
            all_ratings.append(entry)
        if 'next' not in results:
            break
        else:
            cursor = results['next']
        
    return pd.DataFrame(all_ratings)


def threaded_api_request(url_list, max_retries=15, max_threads=50, print_every=1000):
    """
    Return the results of a list of API requests. This function is multithreaded and will
    retry failed requests up to max_retries times. It will also print a status update every
    print_every requests.
    
    Parameters
    ----------
    url_list : list
        A list of API endpoints you want to call.
    max_retries : int, optional
        The number of times to retry a failed request. Defaults to 15.
    max_threads : int, optional
        The maximum number of threads to use. Defaults to 50.
    
    Returns
    -------
    list
        A list of the results of the API requests you passed in.
    
    """

    all_results = []
    missing_urls = []
    failed_urls = []

    def error_handler(url):

        retry_count = 0

        while True:
            try:
                res = api_request(url)
                return res.json()
            except Exception as e:
                if str(e)[0:3] == '404':
                    missing_urls.append(url)
                    return None
                retry_count += 1
                if retry_count > max_retries:
                    failed_urls.append(url)
                    print('Url failed after 15 retries.')
                    return None

    def runner(url_list):
        count = 0
        threads = [] 
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            for url in url_list:
                threads.append(executor.submit(error_handler, url))
            for task in as_completed(threads):
                entry = task.result()
                if entry:
                    all_results.append(entry)
                count += 1
                if count % print_every == 0:
                    print(f'{count} URLs processed so far.')

    print('Running scraper...')
    runner(url_list)
    
    return all_results, missing_urls, failed_urls


def encode_id(internal_id, is_user=False):
    """
    Return a base62-encoded version of a Letterboxd internal ID. The external 
    ID is used by the Letterboxd API for both members and films.
    
    Parameters
    ----------
    internal_id : int
        The internal ID you want to encode.
    
    Returns
    -------
    str
        The base62-encoded version of the internal ID you passed in.
    """
    if is_user:
        return base62.encode((internal_id*10) + 7, charset=base62.CHARSET_INVERTED)
    else:
        return base62.encode(internal_id*10, charset=base62.CHARSET_INVERTED)


def decode_id(external_id):
    """
    Return a base62-decoded internal id from a Letterboxd external ID used
    by the Letterboxd API for both members and films.    
    
    Parameters
    ----------
    external_id : str
        The external ID you want to decode.
    
    Returns
    -------
    int
        The base62-decoded version of the external ID you passed in.
    """
    
    return int(base62.decode(external_id, charset=base62.CHARSET_INVERTED)/10)