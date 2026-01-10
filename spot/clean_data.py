import pandas as pd
#df['track_name'] = df['track_name'].astype(str).str.strip().str.lower()
#df['artist_name']= df['artist_name'].astype(str).str.strip().str.lower()
#df['album_name']= df['album_name'].astype(str).str.strip().str.lower()



def many_to_many(uri_info : pd.DataFrame, *,column :str, key: str | None = None) -> pd.DataFrame:
    #create new many to many df 

    #create df with the two relevant columns
    mmdf = uri_info[['id', column]].copy()

    #if it is a 
    if key != None:
        mmdf[column]= mmdf[column].apply(
            lambda x: [y[key] for y in x])
    
    mmdf= mmdf.explode(column)

    return mmdf

def dictcolumn(column : str, key: str) -> str:
    
    new_column = column.apply(
    lambda d: d.get(key) 
    if isinstance(d,dict) and key in d 
    else None)


    return new_column

