
import string
from harp._backend.baseprovider import BaseDatasetProvider


def format_search_table(self: BaseDatasetProvider):
    """
    Formats and standardize the pandas table to:
    columns: "short_name", "units", "name", "query_name", "search"
    attrs: "dataset", "import_path", "collection", "institution"
    """
    table = self.nomenclature.table.copy()
    
    # table["short_name"] = table["query_name"]
    
    table['name'] = table['query_name'].str.replace(f"[{string.punctuation}]", " ", regex=True)
    table["search"] = table["name"] # + "   " + table["short_name"] # + "   " + self.institution + "   " + self.collection
    
    table.attrs["dataset"]      = str(self.__class__.__name__)
    table.attrs["import_path"]  = str(self.__class__).split("\'")[1]
    table.attrs["collection"] = self.collection
    table.attrs["institution"] = self.institution
    
    
    # reorder columns
    table = table[["short_name", "units", "name", "query_name", "search"]]
    
    return table
    