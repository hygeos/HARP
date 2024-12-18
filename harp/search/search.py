import difflib
from pathlib import Path

list_name = [
    "temperature at 10m",
    "u wind at 10m",
    "v wind at 10m",
    "surface wind speed",
    "planetary boundary layer height",
    "surface pressure",
    "sea level pressure",
    "total column ozone",
    "total column water_vapor",
    "total aerosol optical depth 469nm",
    "total aerosol optical depth 550nm",
    "total aerosol optical depth 670nm",
    "total aerosol angstrom coefficient 550nm",
    "dust aerosol optical depth 550nm",
    "organic carbon aerosol optical depth 550nm",
    "black carbon aerosol optical depth 550nm",
    "sea salt aerosol optical depth 550nm",
    "sulfate aerosol optical depth 550nm",
    "total cloud cover",
    "low cloud cover",
    "mid cloud cover",
    "high cloud cover",
    "total cloud optical depth",
    "low cloud optical depth",
    "mid cloud optical depth",
    "high cloud optical depth",
    "total column cloud ice water",
    "total column cloud liquid water",
    "ozone mass mixing ratio",
    "specific humidity",
    "temperature",
]


def fuzzy_search(list_search, list_name, threshold = 0.75) :
    results_key_index = {} #{key0 : 0, ..., keyn : n}
    index = 0
    list_tuples = [] #[[key0, cpt0] ... [keyn, cptn]]
    
    for term in list_search: #for each term
        for name in list_name: #for each name
            for word in name.split(): #for each word in the name
                res = difflib.SequenceMatcher(None, term, word).ratio() #check match
                if res >= threshold:
                    if name in results_key_index.keys():
                        list_tuples[results_key_index[name]][1] += 1
                    else:
                        results_key_index[name] = index
                        index += 1
                        list_tuples.append([name,1])
                    
    sorted_return_list = sorted(list_tuples, key=lambda x: x[1], reverse=True)
    for i in sorted_return_list:
        print(i)
    return sorted_return_list


def break_down_csv(path_file) :
    try:
        with open(path_file, "r") as file:
            content = file.read()
    except FileNotFoundError as f:
        raise FileNotFoundError("Could not find the file you are trying to read.")

    flag_first_after_data = True #to check first line after data

    lines = content.split("\n")
    index = 1
    for line in lines :
        if not line :
            continue
        
        if line[0] == "#": #config 
            if flag_first_after_data and line[1].isupper():
                print("Tittle")
            else:
                print("config", index)
                index += 1
            flag_first_after_data = False
        else : #data
            flag_first_after_data = True
            index = 1
            #data
            pass


def main():
    list_search = ["mid", "cloud", "carbon"]
    #fuzzy_search(list_search, list_name)
    break_down_csv(Path("./ressources/merra_full.csv"))
    pass


if __name__ == "__main__":
    main()

