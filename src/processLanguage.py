import nltk
#nltk.download('punkt')
#nltk.download('stopwords')
#nltk.download('averaged_perception_tagger')
#nltk.download('wordnet')

from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.probability import FreqDist
from nltk.corpus import stopwords
import string

ejemplo = df_grants.loc[0,'desc_convocatoria']
#ejemplo

stop_words = stopwords.words('english')
summary = [x for x in word_tokenize(ejemplo.lower()) if x not in stop_words + list(string.punctuation)]
#summary

lista_example = FreqDist(summary).most_common(20)
#lista_example

example_words = [palabra[0] for palabra in lista_example ]
#example_words