import pandas as pd
import numpy as np

def timeDay_meta (bigList: list, day: str) :
    
    for smallList in bigList :
            if smallList[0] == day :
                return smallList[1]
            
    return bigList

def tableHours (df) :

    df_hours = df[['gmap_id', 'hours']]
    
    df_hours['ID_hours'] = np.arange(df_hours.shape[0])
    

    df_hours['Monday'] = df_hours['hours'].apply(lambda hour : timeDay_meta(hour, 'Monday') if isinstance(hour, list) else hour)
    df_hours['Tuesday'] = df_hours['hours'].apply(lambda hour : timeDay_meta(hour, 'Tuesday') if isinstance(hour, list) else hour)
    df_hours['Wednesday'] = df_hours['hours'].apply(lambda hour : timeDay_meta(hour, 'Wednesday') if isinstance(hour, list) else hour)
    df_hours['Thursday'] = df_hours['hours'].apply(lambda hour : timeDay_meta(hour, 'Thursday') if isinstance(hour, list) else hour)
    df_hours['Friday'] = df_hours['hours'].apply(lambda hour : timeDay_meta(hour, 'Friday') if isinstance(hour, list) else hour)
    df_hours['Saturday'] = df_hours['hours'].apply(lambda hour : timeDay_meta(hour, 'Saturday') if isinstance(hour, list) else hour)
    df_hours['Sunday'] = df_hours['hours'].apply(lambda hour : timeDay_meta(hour, 'Sunday') if isinstance(hour, list) else hour)
    
    df_hours.drop(['hours'], axis=1, inplace=True)
    df_hours.to_csv('/content/drive/MyDrive/pruebaQuind/data/processed/hours.csv', sep=';', index = False, mode='a')

    df.drop(['hours'], axis=1, inplace=True)

    return df

#tableHours(df_meta)