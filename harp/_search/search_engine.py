import difflib
from pathlib import Path
import string

import numpy as np
import pandas as pd

from harp._search import search_cfg

def _split_string(s: str):
    # s = s.replace("_", " ")
    # s = s.replace("-", " ")
    # s = s.replace(";", " ")
    # s = s.replace(",", " ")
    # s = s.strip()
    return s.split(" ")
    

def _fuzzy_score(search_terms: list, string: str):
    """
    Args:
        search_terms: A list of terms to search for.
        string: The string to be checked against the search terms.
        word_threshold: The minimum similarity ratio required for a word match.
    
    Returns:
        A float representing the fuzzy score, adjusted for unmatched terms.
    """
    
    word_threshold = search_cfg.word_threshold
    
    iscore = 0
    score  = 0
    
    # word_match = 0
    splitted_string = _split_string(string)
    nrefterms = len(splitted_string)
    for term in search_terms: #for each term
        for word in splitted_string: # for each word in the line
            res = difflib.SequenceMatcher(None, term.strip(), word.strip()).ratio() # check match
            if res >= word_threshold: 
                score += res # weight by word match -> allow to discriminate perfect matches with low accuracy matches
                iscore += 1
                # word_match += res
                break # search term has been found.

    # penalize string that have terms which have not been matched
    unmatched_term_penalty = (1-word_threshold) * (1 - (iscore / nrefterms))
    
    if search_cfg.match_exact: unmatched_term_penalty  *= 0.25
    if search_cfg.match_strict: unmatched_term_penalty *= 0.5
    
    
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




def filter_best(
    df: pd.DataFrame, 
):
    """
    Filters a DataFrame to retain the best matches from a fuzzy search
    Assumes a column 'fuzzy_score' is present

    Args:
        df: DataFrame containing a 'fuzzy_score' column for ranking.

    Returns:
        DataFrame with filtered rows: 
        - Removes entries with scores below 0.20
        - Retains entries within 20% of the best score
        - Fills with top 5 entries if there are fewer than 5 matches

    Raises:
        ValueError: If input DataFrame lacks 'fuzzy_score' column.
    """
    
    ntarget = 5
    
    # remove really bad matches
    trash_treshold = search_cfg.trash_treshold
    df = df[df['score'] > trash_treshold]
    
    # remove matches too far from best match
    # best = df['score'].max()
    # tolerance = 0.2  # Keep scores within 20% of the best score
    # bests = df[df['score'] >= (best - tolerance)]
    
    # if not enough fill with garbage until target
    # if len(bests) < ntarget:
        # bests = df.nlargest(ntarget, 'score')
    
    return df.copy()
    



def compile(
    results: list[pd.DataFrame],
    sources: str
):

    filtered = []
    
    for t in results:
        
        dataset_source = t.attrs["institution"] + "." + t.attrs["collection"] + "." + t.attrs["dataset"]
        t["dataset"] = dataset_source
        t["uscore"] = t["score"].apply(lambda x: round(x, 2))
        t["score"] = t["score"].apply(lambda x: min(round(x + 0.0499, 1), 1.0))
        t["match"] = t["score"].apply(lambda x: f"{x:.0%}".rjust(5))

        # Filtering from the sources provided by flag --from
        if sources is not None:
            for src in sources:
                # print(src.strip().lower(), " in ",  dataset_source, " is ", src.lower() in dataset_source)
                
                if src.strip().lower() in dataset_source.lower():
                    filtered.append(t)
        else:
            filtered.append(t)

    # concatenation
    results = pd.concat(filtered, ignore_index=True)
    results = results.sort_values("score", ascending=False)
    
    # minimum threshold
    minimum = search_cfg.match_threshold
    results = results[results['score'] >= (minimum / 100)]
    
    # reordering
    # results = results.drop(["search"], axis=1)
    results = results[["match", "units", "name", "dataset", "query_name", "short_name", "score", "uscore", "search"]]
    
    # togglable queryname display
    if search_cfg.display_query_name == False:
        
        results = results.drop(["query_name"], axis=1)
        
    if not search_cfg.debug:
        results = results.drop(["score", "uscore", "search"], axis=1)
             
    
    return results
    