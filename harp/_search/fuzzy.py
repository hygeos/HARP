import difflib
from pathlib import Path

import numpy as np
import pandas as pd


def _split_string(s: str):
    # s = s.replace("_", " ")
    # s = s.replace("-", " ")
    # s = s.replace(";", " ")
    # s = s.replace(",", " ")
    # s = s.strip()
    return s.split(" ")
    

def _fuzzy_score(search_terms: list, string: str, word_threshold = 0.71):
    """
    Args:
        search_terms: A list of terms to search for.
        string: The string to be checked against the search terms.
        word_threshold: The minimum similarity ratio required for a word match.
    
    Returns:
        A float representing the fuzzy score, adjusted for unmatched terms.
    """
    
    iscore = 0
    score  = 0
    
    # word_match = 0
    splitted_string = _split_string(string)
    nrefterms = len(splitted_string)
    for term in search_terms: #for each term
        for word in splitted_string: # for each word in the line
            res = difflib.SequenceMatcher(None, term, word).ratio() # check match
            if res >= word_threshold: 
                score += res # weight by word match -> allow to discriminate perfect matches with low accuracy matches
                iscore += 1
                # word_match += res
                break # search term has been found.

    # penalize string that have terms which have not been matched
    unmatched_term_penalty =  (1-word_threshold) * (1 - (iscore / nrefterms))
    
    return score - unmatched_term_penalty
    

def search(
    keywords: list[str], 
    df: pd.DataFrame, 
    source_column: str,
    sort_results: bool = False, 
    nmax: int = None,
    result_column: str = 'score'
) -> pd.DataFrame:
    """
    Args:
        keywords: A list of search terms.
        df: A pandas DataFrame containing the data to search through.
        source_column: Name of the column containing strings to search.
        sort_results: If True, results are sorted by score in descending order.
        nmax: Maximum number of results to return.
        result_column: Name of the column to store the results.
    
    Returns:
        A pandas DataFrame with the search results added as a new column.
        If nmax is specified, returns only the top nmax results.
    """

    assert nmax is None or nmax > 0
    assert source_column in df.columns, f"Column '{source_column}' not found in DataFrame"
    
    # Prepare keywords
    keywords = [k.lower().strip() for k in keywords]
    nterms = len(keywords) # used to normalize to a ratio
    
    # Calculate scores for each row
    df[result_column] = df[source_column].apply(
        lambda x:  np.clip(_fuzzy_score(keywords, x.lower().strip()) / nterms, 0, None)
    )
    
    # Sort if requested
    if sort_results:
        df = df.sort_values(by=result_column, ascending=False)
    
    # Apply nmax limit
    if nmax is not None:
        df = df.head(nmax)
    
    return df
