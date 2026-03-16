
from core import log
from textwrap import dedent


def code_sample(dataset: str, param: str):
    
    ds = dataset.split(".")[1]
    
    rgb = log.rgb
    
    template = dedent(f"""
    from pathlib import Path
    from harp.datasets import {dataset}
    
    #{log.rgb.gray("# create a dataset object with the parameter of interest, which can be renamed")}
    provider = {dataset}(variables = ["{param}"])
    
    #{log.rgb.gray("# query the dataset for the parameter of interest at a specific time")}
    ds = provider.get(time = datetime(2020, 1, 21))
    """)    
    
    template = template.strip()
    
    log.info(f"Code sample to query {param} from {dataset} dataset:\n")
    print(template)