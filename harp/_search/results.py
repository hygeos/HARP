
import numpy as np
import pandas as pd

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
    threshold = 0.25
    df = df[df['score'] > threshold]
    
    # remove matches too far from best match
    best = df['score'].max()
    tolerance = 0.2  # Keep scores within 20% of the best score

    bests = df[df['score'] >= (best - tolerance)]
    
    # if not enough fill with garbage until target
    if len(bests) < ntarget:
        bests = df.nlargest(ntarget, 'score')
        
    return bests
    



def compile(
    results: list[pd.DataFrame], 
):
    for r in results:
        r["dataset"] = r.attrs["dataset"]
        r["collection"] = r.attrs["collection"]
        
        
    results = pd.concat(results, ignore_index=True)
    results = results.drop(['search'], axis=1)
    results = results.sort_values("score", ascending=False)
    
    # results["revelency"] = np.round(results["score"] * 5) 
    
    return results
    
    
    