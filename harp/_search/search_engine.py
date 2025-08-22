import difflib
from pathlib import Path
import string

import numpy as np
import pandas as pd

from core import log

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
        
        if " " in term: # trigger quote matching
            if term in string:
                score += 1 # weight by word match -> allow to discriminate perfect matches with low accuracy matches
                iscore += 1
                continue
        
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
    
    # Remove really bad matches
    trash_treshold = search_cfg.trash_treshold
    df = df[df['score'] > trash_treshold]
    
    # NOTE: below removed in favor of all results filters
    #       removes bias
    
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
        
        dataset_source = t.attrs["collection"] + "." + t.attrs["dataset"] # + " " # removed: t.attrs["institution"] + "." + 
        t["dataset"] = dataset_source
        t["timerange"] = t.attrs["timerange"]
        t["uscore"] = t["score"].apply(lambda x: round(x, 2))
        t["score"] = t["score"].apply(lambda x: min(round(x + 0.0499, 1), 1.0))
        t["match"] = t["score"].apply(lambda x: f"{x:.0%}".rjust(5))

        # Filtering from the sources provided by flag --from
        if sources is not None:
            matched_all = True
            for src in sources:
                # print(src.strip().lower(), " in ",  dataset_source, " is ", src.lower() in dataset_source)
                
                if not src.lower() in dataset_source.lower():
                    matched_all = False
                    break
                
            if matched_all: 
                filtered.append(t)
        else:
            filtered.append(t)
    
    if len(filtered) == 0:
        return []

    # concatenation
    table = pd.concat(filtered, ignore_index=True)
    table = table.sort_values("score", ascending=False)
    
    # minimum threshold
    minimum = search_cfg.match_threshold
    table = table[table['score'] >= (minimum / 100)]
    
    
    _apply_specific_format(table)
    
    # reordering
    # results = results.drop(["search"], axis=1)
    table = table.rename(columns={"short_name": "param", "spatial": "resolution"})
    table = table[["match",  "dims", "resolution", "units", "name", "param", "dataset", "timerange", "query_name", "score", "uscore", "search"]]
    
    table = table.drop(["match"], axis=1)
    
    # togglable queryname display
    if search_cfg.display_query_name == False:
        table = table.drop(["query_name"], axis=1)
        
    if not search_cfg.debug:
        table = table.drop(["score", "uscore", "search"], axis=1)
    
    n = len(table)
    log.info(f"{n} entries found.", flush=True)
    
    
    return table
    
    

def _apply_specific_format(t):
    
    # Format spatial column
    def _format_deg_zero_padded(s):
        x, y = s.split(" x ")
        x, y = x.strip(), y.strip()
        x = x + '0' * max((3 - len(x)), 0)
        y = y + '0' * max((3 - len(y)), 0)
        return f"{x}° x {y}°"
        
    def _format_deg_align(s):
        nchar = 5
        x, y = s.split(" x ")
        return f"{(x+'°').ljust(nchar)} x {y}°"
        
    def _format_deg_align_and_pad(s):
        nchar = 5
        x, y = s.split(" x ")
        
        x = x + '0' * max((5 - len(x)), 0)
        y = y + '0' * max((5 - len(y)), 0)
        
        return f"{(x+'°').ljust(nchar)} x {y}°"
        
    def _format_compact(s):
        nchar = 3
        x, y = s.split(" x ")
        
        x = x.replace("0.", ".")
        y = y.replace("0.", ".")
        
        x = x.strip()
        y = y.strip()
        
        x = x + '0' * max((nchar - len(x)), 0)
        y = y + '0' * max((nchar - len(y)), 0)
        
        return f"{(x+'').ljust(nchar)}° × {y}°"
    
    t["spatial"] = t["spatial"].apply(_format_compact)
    
    
    
    def _format_units(t):
    
        t["units_"] = t["units"]
        
        # Format and standardize Units column
        t["units"] = t["units"].apply(lambda x: str(x).replace(".", " ").replace("**", ""))
        t['units'] = t['units'].replace(['~', '1', 'dimensionless'], "~")
        # DEBUG:
        #  _datafram_cols_diff(t, "units_", "units", "short_name", output_file=".harp_unit_formatter_diagnosis.txt")
        
        # t.drop([["units_"]], axis=1)

    _format_units(t)

def _datafram_cols_diff(df, c1, c2, ref=None, output_file=None):
    """
    Tracks changes in a DataFrame column and outputs them in a text format.
    
    Parameters:
    - df: pandas DataFrame
    - column_name: str, name of the column to track
    - output_file: str (optional), path to save the output text file
    
    Returns:
    - changes_df: DataFrame containing only rows where changes occurred
    - output_text: formatted text showing the changes
    """
    
    # Find rows where the value changed (excluding first row which has no previous value)
    diff_rows = df[df[c1] != df[c2]]
    # diff_rows = diff_rows.iloc[1:]  # Skip first row which is NaN comparison
    
    # Prepare the output text
    output_lines = []
    output_lines.append(f"Diff between columns: {c1} and {c2}")
    output_lines.append(f"Rows with differences: {len(diff_rows)}")
    output_lines.append(f"Percent of rows: {int((len(diff_rows) / len(df)) * 100)}%")
    output_lines.append("Diffs:")
    
    for idx, row in diff_rows.iterrows():
        c1_val = row[c1]
        c2_val = row[c2]
        
        if ref is not None:
            idx = row[ref] + ": "
        else:
            idx = ""
        output_lines.append(f"{idx.ljust(20)}{str(c1_val).ljust(30)} >> {str(c2_val).rjust(30)}")
    
    # Combine all lines into a single text
    output_text = "\n".join(output_lines)
    
    # Save to file if output path is provided
    if output_file:
        with open(output_file, 'w') as f:
            f.write(output_text)
    else:
        print(output_text)
