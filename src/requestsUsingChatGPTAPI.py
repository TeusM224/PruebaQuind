import openai
import re

openai.api_key = # Reemplaza YOUR_API_KEY por tu clave API

ods = ["Fin de la pobreza", 
       "Hambre cero", 
       "Salud y bienestar", 
       "Educación de calidad", 
       "Igualdad de género", 
       "Agua limpia y saneamiento", 
       "Energía asequible y no contaminante", 
       "Trabajo decente y crecimiento económico", 
       "Industria, innovación e infraestructura", 
       "Reducción de las desigualdades", 
       "Ciudades y comunidades sostenibles", 
       "Producción y consumo responsables", 
       "Acción por el clima", 
       "Vida submarina", 
       "Vida de ecosistemas terrestres", 
       "Paz, justicia e instituciones sólidas", 
       "Alianzas para lograr los objetivos"]

ods_dict = {}

for obj in ods:
    ods_dict[obj] = []

for word in example_words:
    prompt = f"Clasifica las palabras '{word}' en uno de los ODS:\n"
    prompt += "\n".join([f"{i+1}. {obj}" for i, obj in enumerate(ods)]) + "\n\n"
    prompt += "ODS: "
    completions = openai.Completion.create(engine="text-davinci-003", prompt=prompt, max_tokens=1, temperature=0.4)
    result = completions.choices[0].text.strip()
    if re.match(r"^\d+$", result) and int(result) in range(1, len(ods) + 1):
        ods_dict[ods[int(result) - 1]].append(word)

#print(ods_dict)

def key_with_most_values(dictionary):
    """
    Función que recibe un diccionario y devuelve la clave con más valores.
    """
    most_values_key = None
    most_values_count = 0

    for key, values in dictionary.items():
        if len(values) > most_values_count:
            most_values_key = key
            most_values_count = len(values)

    return most_values_key

key_with_most_values(ods_dict)