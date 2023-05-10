def drop_columns_meta(df):
    columns_drop = ['address','price', 'MISC','state', 'relative_results', 'url']
    df = df.drop(columns=columns_drop, axis=1)
    return df

#drop_columns_meta(df_meta)

def drop_columns_reviews(df):
    columns_drop = ['user_id','name', 'pics','resp', 'url']
    df = df.drop(columns=columns_drop, axis=1)
    return df

#drop_columns_reviews(dfc_fullr)
#/content/drive/MyDrive/pruebaQuind/data/processed/california_full.csv