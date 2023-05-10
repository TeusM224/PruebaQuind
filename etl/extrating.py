import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from csv import QUOTE_NONE

#Archivo meta
df_j1 = pd.read_json('/content/drive/MyDrive/Google Maps/metadata-sitios/1.json', lines = 'True')
df_j2 = pd.read_json('/content/drive/MyDrive/Google Maps/metadata-sitios/2.json', lines = 'True')
df_j3 = pd.read_json('/content/drive/MyDrive/Google Maps/metadata-sitios/3.json', lines = 'True')
df_j4 = pd.read_json('/content/drive/MyDrive/Google Maps/metadata-sitios/4.json', lines = 'True')
df_j5 = pd.read_json('/content/drive/MyDrive/Google Maps/metadata-sitios/5.json', lines = 'True')
df_j6 = pd.read_json('/content/drive/MyDrive/Google Maps/metadata-sitios/6.json', lines = 'True')
df_j7 = pd.read_json('/content/drive/MyDrive/Google Maps/metadata-sitios/7.json', lines = 'True')
df_j8 = pd.read_json('/content/drive/MyDrive/Google Maps/metadata-sitios/8.json', lines = 'True')
df_j9 = pd.read_json('/content/drive/MyDrive/Google Maps/metadata-sitios/9.json', lines = 'True')
df_j10 = pd.read_json('/content/drive/MyDrive/Google Maps/metadata-sitios/10.json', lines = 'True')
df_j11 = pd.read_json('/content/drive/MyDrive/Google Maps/metadata-sitios/11.json', lines = 'True')

df_full = pd.concat([df_j1, df_j2, df_j3, df_j4, df_j5, df_j6, df_j7, df_j8, df_j9, df_j10, df_j11])
df_full.index = range(df_full.shape[0])
df_full.to_csv('/content/drive/MyDrive/prueba_quind/data/processed/meta_full.csv', sep=';', index=False,mode='a',encoding='utf-8')

#df_meta = pd.read_csv('/content/drive/MyDrive/pruebaQuind/data/processed/meta_full.csv', sep=';')

#Archivo reviews
dfc_r1 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/1.json', lines = 'True')
dfc_r2 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/2.json', lines = 'True')
dfc_r3 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/3.json', lines = 'True')
dfc_r4 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/4.json', lines = 'True')
dfc_r5 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/5.json', lines = 'True')
dfc_r6 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/6.json', lines = 'True')
dfc_r7 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/7.json', lines = 'True')
dfc_r8 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/8.json', lines = 'True')
dfc_r9 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/9.json', lines = 'True')
dfc_r10 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/10.json', lines = 'True')
dfc_r11 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/11.json', lines = 'True')
dfc_r12 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/12.json', lines = 'True')
dfc_r13 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/13.json', lines = 'True')
dfc_r14 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/14.json', lines = 'True')
dfc_r15 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/15.json', lines = 'True')
dfc_r16 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/16.json', lines = 'True')
dfc_r17 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/17.json', lines = 'True')
dfc_r18 = pd.read_json('/content/drive/MyDrive/pruebaQuind/data/raw/review-California/18.json', lines = 'True')

dfc_fullr = pd.concat([dfc_r1,dfc_r2,dfc_r3,dfc_r4,dfc_r5,dfc_r6,dfc_r7,dfc_r8,dfc_r9,dfc_r10,dfc_r11,dfc_r12,dfc_r13,dfc_r14,dfc_r15,dfc_r16,dfc_r17,dfc_r18])
dfc_fullr.index = range(dfc_fullr.shape[0])
dfc_fullr.to_csv('/content/drive/MyDrive/pruebaQuind/data/processed/california_full.csv', sep=';',escapechar='\\', index=False, mode='a',encoding='utf-8') #Error: need to escape, but no escapechar set pandas

#df_california = pd.read_csv('/content/drive/MyDrive/pruebaQuind/data/processed/california_full.csv', sep=';')