import pandas as pd 
import requests 
import re
import regex
import os
import time
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from dateutil import parser
from datetime import datetime


hoy = datetime.date.today()
hoy = hoy.strftime(f'%m/%d/%Y')
hoy_nstr = datetime.strptime(hoy, "%m/%d/%Y")

columnas = ['id_oferta','nombre_oportunidad', 'desc_convocatoria','link_oportunidad','donante_o_inv','fecha_publicacion','fecha_cierre','tipo_oportunidad','idioma','tematica_oportunidad','tipo_contribucion','monto_contribucion_original','financiamiento','contrapartida','duracion_proyecto','alcance_territorial','organizaciones_elegibles','capacidad_financiera','beneficiarios','requisitos_aplicacion','requisitos_elegibilidad','perfil_general','forma_envio_propuesta','palabras_clave','objetivos_desarrollo_sostenible']

df_grants = pd.DataFrame(columns=columnas)

links = ['https://www.grants.gov/view-opportunity.html?oppId=347744']
"""
'https://www.grants.gov/view-opportunity.html?oppId=347324',
         'https://www.grants.gov/view-opportunity.html?oppId=347325',
         'https://www.grants.gov/view-opportunity.html?oppId=347326',
         'https://www.grants.gov/view-opportunity.html?oppId=347328'
"""

for link in links:
    options = webdriver.ChromeOptions()
    driver_path = "C:\Program Files (x86)\chromedriver.exe"
    driver = webdriver.Chrome(executable_path = driver_path, chrome_options=options)
 
    driver.get(link)

    iframe = driver.find_element(By.TAG_NAME, "iframe")
    driver.switch_to.frame(iframe)

    element_id = driver.find_element(By.ID, "oppNumHeading").text

    element_name = driver.find_element(By.ID, "oppTitleHeading").text

    element_desc = driver.find_element(By.XPATH,"//body[1]/table[1]/tbody[1]/tr[1]/td[1]/div[1]/div[1]/div[1]/fieldset[3]/div[1]/table[1]/tbody[1]/tr[2]/td[1]").text.replace('\n','')

    element_donante = driver.find_element(By.ID, "agencyNameHeading").text

    try:
        element_close_date = driver.find_element(By.XPATH, "//body[1]/table[1]/tbody[1]/tr[1]/td[1]/div[1]/div[1]/div[1]/fieldset[1]/table[1]/tbody[1]/tr[1]/td[2]/div[1]/table[1]/tbody[1]/tr[4]/td[1]/span[1]").text.strip()
        if element_close_date:
            element_close_date_regex = re.search(r"[A-Z][a-z]{2}\s\d{2},\s\d{4}", element_close_date)
            close_date = element_close_date_regex.group(0)

            date_obj = parser.parse(close_date) #Lo vuelve objeto parser pa que soporte otros funciones
            close_date = date_obj.strftime('%m/%d/%Y') #se pasa al formato requerido

            close_date_nstr = datetime.strptime(close_date, "%m/%d/%Y") # se vuelve objeto datetime
    except:
        pass

    element_opportunity = driver.find_element(By.XPATH,"//body[1]/table[1]/tbody[1]/tr[1]/td[1]/div[1]/div[1]/div[1]/fieldset[1]/table[1]/tbody[1]/tr[1]/td[1]/div[1]/table[1]/tbody[1]/tr[4]/td[1]/span[1]").text

    element_opportunity_type = driver.find_element(By.XPATH,"//body[1]/table[1]/tbody[1]/tr[1]/td[1]/div[1]/div[1]/div[1]/fieldset[1]/table[1]/tbody[1]/tr[1]/td[1]/div[1]/table[1]/tbody[1]/tr[7]/td[1]").text.replace('\n','')

    total_days = (close_date_nstr - hoy_nstr).days

    

    driver.quit()

    df_grants = df_grants.append({'id_oferta':element_id,
                                'nombre_oportunidad':element_name,
                                'desc_convocatoria':element_desc,
                                'link_oportunidad':link,
                                'donante_o_inv':element_donante,
                                'fecha_publicacion':hoy,
                                'fecha_cierre':close_date,
                                'tipo_oportunidad':element_opportunity,
                                'idioma':'Inglés',
                                'tematica_oportunidad':element_opportunity_type,
                                'tipo_contribucion':[],
                                'monto_contribucion_original':[],
                                'financiamiento':[],
                                'contrapartida':[],
                                'duracion_proyecto':total_days,
                                'alcance_territorial':[],
                                'organizaciones_elegibles':[],
                                'capacidad_financiera':[],
                                'beneficiarios':[],
                                'requisitos_aplicacion':[],
                                'requisitos_elegibilidad':[],
                                'perfil_general':[],
                                'forma_envio_propuesta':[],
                                'palabras_clave':[],
                                'objetivos_desarrollo_sostenible':[]},
                                
                                ignore_index=True)
    #lista_texto = []