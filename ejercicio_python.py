#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

import io
import json
import os
import re
import sys
import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import tweepy
from IPython.core.display import clear_output
from bs4 import BeautifulSoup
from textblob import TextBlob
from tweepy import API
from tweepy.streaming import StreamListener

# Variables Globales
# Configuración acceso api y variables globales
consumer_key = ''
consumer_secret = ''
access_token_key = ''
access_token_secret = ''
# File
file_name = 'tweets.json'
stream_language = ['en']
query_list = ['Curie', 'Planck', 'Einstein', 'Bohr', 'Fleming', 'Higgs']
dir_json = './jsons'
dir_images = './images'


class MyListener(StreamListener):

    def __init__(self, output_file, count_max=50, api=None):
        self.api = api or API()
        self.output_file = output_file
        self.counter = 1
        self.counter_tweet = 0
        self.count_max = count_max
        self.start_time = time.time()
        self.tweet_data = []
        self.status_list = []

    def on_status(self, status):
        while self.counter <= self.count_max:
            clear_output(False)
            print('Nº Tweets recuperados: ' + str(self.counter)
                  + ' - ' + self.get_time()
                  , end=' ')
            try:
                self.status_list.append(status)
                json_string = json.dumps(status._json, ensure_ascii=False)
                self.tweet_data.append(json_string)
                self.counter += 1
                return True
            except BaseException as ex:
                sys.stderr.write("Error on_data:{}\n".format(ex))
                return True
        with io.open(self.output_file, 'w', encoding='utf-8') as f:
            f.write(u'{"tweets":[')
            if len(self.tweet_data) > 1:
                f.write(','.join(self.tweet_data))
            else:
                f.write(self.tweet_data[0])
            f.write(u']}')
        return False

    def on_error(self, status):
        if status == 420:
            print(status)
            return False

    def get_time(self):
        dif = time.strftime("%H:%M:%S",
                            time.gmtime(time.time() - self.start_time))
        return str(dif)


class MyTokenizerClass:
    """"
    Creamos una clase propia para reunir los métodos encargados de porcesado,
    búsqueda, conteo, etc de palabras o caracteres en el texto del tweet.
    Vamos a usar una serie de regex para que la  partición del texto sea más
    acertada que usando simplemente el word_tokenizer de la librería nltk.
    Estas regex provienen en parte del blog de Marco Bonzanini, de la web
    regex101.com y otras son propias
    https://marcobonzanini.com/2015/03/09/mining-twitter-data-with-python-part-2/
    """
    emoticons_str = r"""
        (?:
            [:=;] # Eyes
            [oO\-]? # Nose (optional)
            [D\)\]\(\]/\\OpP] # Mouth
        )"""

    regex_str = [
        emoticons_str,
        r'<[^>]+>',  # HTML tags
        r'@[\w_]+',  # @-mentions (regex101.com)
        r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)",  # hash-tags (marcobonzanini.com)
        r'http[s]?://[^\s<>"]+|www\.[^\s<>"]+',  # URLs (regex101.com)
        r'(?:(?:\d+,?)+(?:\.?\d+)?)',  # numeros  (regex101.com)
        r"(?:[a-z][a-z'\-_]+[a-z])",  # palabras con - y ' (marcobonzanini.com)
        r'(?:[\w_]+)',  # otras palabras  (regex101.com)
        r'(?:[^[:punct:]])'  # cualquier otra cosa excepto signos de puntuación
    ]

    tokens_re = re.compile(r'(' + '|'.join(regex_str) + ')',
                           re.VERBOSE | re.IGNORECASE)
    emoticon_re = re.compile(r'^' + emoticons_str + '$',
                             re.VERBOSE | re.IGNORECASE)

    def num_palabras_caracteres(self, s):
        """
        Método que cuenta las palabras y caracteres de un texto
        Importante: No se consideran palabras los signos de puntuación,
        Serán excluídos con la regex (?:[^[:punct:]]) aplicada
        :param s: Cadena de texto a evaluar
        :return: Diccionario con ambos valores: número de palabras y número
        de caracteres
        """
        num = {}
        tokens = self.tokens_re.findall(s)
        num['palabras'] = len(tokens)
        num['caracteres'] = len([char for token in tokens for char in token])
        return num


class MyStatisticsClass:
    """"
     Creamos una clase propia para reunir los métodos encargados de
     analizar los tweets
    """

    def __init__(self, df_statistic):
        self.df = df_statistic

    def get_save_picture_path(self, file_name):
        """
        Método que devuelve la ruta de grabación de las imágenes
        :param file_name: String con el nombre del archivo de imagen
        :return: Ruta de grabación.
        """
        return os.path.join(dir_images, file_name)

    def get_tweets_per_hour(self):
        """
        Método que muestra por pantalla el número de tweets agregado por hora,
        y crea un gráfico barplot.
        :return: Lista de las horas con los tweets creados y gráfico barplot
        """
        # Frecuencia por horas
        frecuencia_list = self.df.groupby('hora')['id'].count()

        # Creamos un df a partir de la serie y renombramos las columnas
        df_hour = pd.DataFrame([frecuencia_list]).T
        df_hour.rename(columns={'id': 'value'}, inplace=True)

        print('Las distribución horaria de los tweets es:\n')
        for index, row in df_hour.iterrows():
            print('Hora {0} - {1} tweets'.format(index, row['value']))

        # Mostramos gráfico
        sns.set(color_codes=True)
        palette = sns.color_palette('Reds_d', 24)
        fig, ax = plt.subplots(figsize=(14, 6))
        ax = sns.barplot(df_hour.index, df_hour['value'], alpha=.6,
                         palette=palette)
        for p in ax.patches:
            if p.get_height > 0:
                ax.annotate("%d" % p.get_height(),
                            (p.get_x() + p.get_width() / 2.,
                             p.get_height()),
                            ha='center', va='center', fontsize=10, color='gray',
                            fontweight='bold', xytext=(0, 5),
                            textcoords='offset points')
        ax.set(ylabel='Frecuencia', xlabel='Horas')
        fig.suptitle(u'Distribución Horaria',
                     horizontalalignment='center', y=0.95)
        plt.savefig(self.get_save_picture_path('Hours.png'),
                    bbox_inches="tight")
        plt.show()

    def get_count_word(self, s, word_to_find):
        """
        Método para contar el número de ocurrencias de una palabra en una cadena
        :param s: cadena en la que buscar
        :param word_to_find: palabra a buscar
        :return: número de ocurrencias
        """
        word_to_find = word_to_find.lower()
        s = s.lower()
        word_token = re.compile(r'(\W' + word_to_find + '\W)+')

        tokens = word_token.findall(s)
        return len(tokens)

    def get_count_of_query_words(self):
        """
        Método que calcula y totaliza la ocurrencia en el texto de los
        tweets, de cada uno de los elementos que se usan en la consulta
        de filtrado
        :return: grafico con las frecuencias
        """
        num_cat = len(query_list)
        count_list = [0] * num_cat

        vect_num = np.vectorize(self.get_count_word)
        for idx, val in enumerate(query_list):
            count_list[idx] = vect_num(self.df['text'], val.lower()).sum()

        df_count = pd.DataFrame({'value': count_list}, index=query_list)
        df_count = df_count.sort_values('value', ascending=False)
        # Mostramos en pantalla los resultados
        print("Los valores obtenidos son:\n")
        for index, row in df_count.iterrows():
            print('{0} - {1} ocurrencias'.format(index, row['value']))

        # Mostramos gráfico
        sns.set(color_codes=True)
        palette = sns.color_palette('Oranges_d', num_cat)
        fig, ax = plt.subplots(figsize=(14, 6))
        ax = sns.barplot(df_count.index, df_count['value'], alpha=.6,
                         palette=palette)
        ax.set(ylabel='Frecuencia', xlabel=u'Términos de búsqueda')
        for p in ax.patches:
            if p.get_height > 0:
                ax.annotate("%d" % p.get_height(),
                            (p.get_x() + p.get_width() / 2.,
                             p.get_height()),
                            ha='center', va='center', fontsize=10, color='gray',
                            fontweight='bold', xytext=(0, 5),
                            textcoords='offset points')
        fig.suptitle(u'Frecuencia de Aparición',
                     horizontalalignment='center', y=0.95)
        plt.savefig(self.get_save_picture_path('Frequency.png'),
                    bbox_inches="tight")
        plt.show()

    def get_time_zone_distribution(self):
        """
        Método para obtener la 10 distribuciones horarias más frecuentes de los
        tweets creados
        :return: barplot
        """
        df_time = self.df[self.df['time_zone'].notnull()]
        grupo = df_time.groupby('time_zone')['id'].count().nlargest(10)
        # Creamos un df a partir de la serie y renombramos las columnas
        df_time = pd.DataFrame([grupo]).T
        df_time.rename(columns={'id': 'value'}, inplace=True)
        num_cat = df_time.shape[0]

        # Mostramos en pantalla los resultados
        print("Las 10 Zonas Horarias con mayor número de tweets son:\n")
        for index, row in df_time.iterrows():
            print('{0} - {1} tweets'.format(index, row['value']))

        # Mostramos gráfico
        sns.set(color_codes=True)
        palette = sns.color_palette('Greens_d', num_cat)
        fig, ax = plt.subplots(figsize=(14, 10))
        ax = sns.barplot(df_time.index, df_time['value'], alpha=.6,
                         palette=palette)
        ax.set(ylabel='Frecuencia', xlabel=u'Zonas')
        for p in ax.patches:
            if p.get_height > 0:
                ax.annotate("%d" % p.get_height(),
                            (p.get_x() + p.get_width() / 2.,
                             p.get_height()),
                            ha='center', va='center', fontsize=10, color='gray',
                            fontweight='bold', xytext=(0, 5),
                            textcoords='offset points')
        fig.suptitle(u'Distribución 10 Zonas Horarias más frecuentes',
                     horizontalalignment='center', y=0.95)
        plt.xticks(rotation=90)
        plt.savefig(self.get_save_picture_path('TimeZone.png'),
                    bbox_inches="tight")
        plt.show()

    def get_porcentaje_fuente_tweet(self):
        """
         Método para obtener los porcentajes de los dispositivos en los que
         se crearon los tweets
         :return: porcentaje de df['source'] y gráfico
         """
        # Calculamos el porcentaje de cada origen con respecto al total
        grupo = self.df.groupby('source')['id'].count()
        num_total_registros = self.df.shape[0]
        grupo = (grupo * 100) / num_total_registros

        # Cogemos los índices que suponen los 5 mayores porcentajese
        #  el resto los agrupamos en 'otros'.
        top_index = grupo.nlargest(5).index
        others_index = [i for i in grupo.index if i not in top_index]

        # Creamos un df a partir de la serie y renombramos las columnas
        df_percent = pd.DataFrame([grupo]).T.reset_index()
        df_percent.rename(columns={'id': 'value'}, inplace=True)

        # Si el agregado por source devuelve más de 5 categorías,
        # reemplazamos los valores que no pertenezcan a los 5 mayores por Otros
        if len(others_index) > 0:
            df_percent = df_percent.replace(others_index, 'Otros')

        # Vemos cuales son los porcentajes de los orígenes
        percent = df_percent.groupby('source').sum().reset_index()
        percent = percent.sort_values('value', ascending=False)

        # Mostramos en pantalla los porcentajes obtenidos
        print("Los porcentajes por origen son:\n")
        for index, row in percent.iterrows():
            print('{} - {:,.2f}% '.format(row['source'], row['value']))

        # Mostramos el gráfico
        fig, ax = plt.subplots(figsize=(14, 6))
        palette = sns.color_palette('Pastel1')
        ax.pie(percent['value'], labels=percent['source'],
               autopct='%1.1f%%',
               startangle=90, colors=palette)
        ax.axis('equal')
        fig.suptitle(u'Distribución por Origen',
                     horizontalalignment='center', y=0.95,
                     fontsize=14)
        plt.legend(bbox_to_anchor=(1.1, 1))
        plt.savefig(self.get_save_picture_path('Sources.png'),
                    bbox_inches="tight")
        plt.show()

    def get_polarity_classification(self, s):
        """
        Método para clasificar la polaridad de un texto usando textblob
        :param s: cadena de texto
        :return: Polaridad que puede ser: Positiva, Neutra o Negativa
        """
        # Primero limpiamos el texto eliminando caracteres especiales, links,..
        s = ' '.join(
            re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)",
                   " ", s).split())

        analysis = TextBlob(s)
        if analysis.sentiment.polarity > 0:
            return 'Positiva'
        elif analysis.sentiment.polarity == 0:
            return 'Neutra'
        else:
            return 'Negativa'

    def get_sentimental_analysis(self):
        """
        Método que devuelve los resultados obtenidos y un gráfico tras aplicar
        el análisis sentimental a los tweets
        :param: columna text del df
        :return: porcentaje polaridad y gráfico
        """
        grupo = self.df.groupby('sa')['id'].count().sort_index(
            ascending=False)

        num_total_registros = self.df.shape[0]
        grupo = (grupo * 100) / num_total_registros
        # Creamos un df a partir de la serie y renombramos las columnas
        df_sent = pd.DataFrame([grupo], ).T.reset_index()
        df_sent.columns = ['sa', 'value']
        df_sent['value'] = pd.to_numeric(df_sent['value'])

        # Mostramos en pantalla los porcentajes obtenidos
        print("Los porcentajes por Polaridad son:\n")
        for index, row in df_sent.iterrows():
            print('{} - {:,.2f}% '.format(row['sa'], row['value']))

        # Mostramos el gráfico
        fig, ax = plt.subplots(figsize=(14, 6))
        palette = sns.color_palette('Pastel1')
        ax.pie(df_sent['value'], labels=df_sent['sa'], autopct='%1.1f%%',
               startangle=90, colors=palette)
        ax.axis('equal')
        fig.suptitle(u'Sentimental Analysis',
                     horizontalalignment='center', y=0.95,
                     fontsize=14)
        plt.legend(bbox_to_anchor=(1.1, 1))
        plt.savefig(self.get_save_picture_path('Sentimental.png'),
                    bbox_inches="tight")
        plt.show()

    def get_media_longitud(self):
        """
        Método para obtener la media de la longitud de los tweets
        :return: valor medio de df['num_caracteres']
        """
        media = np.mean(self.df['num_caracteres'])
        print('La longitud media de los tweets es: {:.0f} caracteres'
              .format(media))

    def get_custom_max_min(self, name_col, max_min='min'):
        """
        Método para devolver el índice de la fila que posee el mayor valor
        de la columna  que se pasa como parámetro
        :param max_min: cadena que indica si se busca el máximo o el mínimo
        :param name_col: columna del df
        :return: Diccionario que contiene el valor máximo hallado y el índice
        respectivo
        """
        result = {}
        if max_min == 'max':
            valor = np.max(self.df[name_col])
        else:
            valor = np.min(self.df[name_col])
        result['valor'] = valor
        result['index'] = self.df[self.df[name_col] == valor].index[0]
        return result


def get_connection_api():
    """
    Método que devuelve la conexión al api de twitter
    :return: auth
    """
    # Conectamos con el api haciendo uso de las credenciales proporcionadas
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token_key, access_token_secret)
    return auth


def search_with_stream():
    """
    Método que invoca el comando de escucha de twitter y devuelve una lista
    con los tweets obtenidos. El número de tweets a recopilar es solicitado
    en el momento de la ejecución.
    :return: status_list
    """
    while True:
        # Usamos el método raw_input porque estamos en la versión 2.7.
        # En la 3.6 no existe
        user_input = raw_input(
            'Cuantos tweets desea recuperar con el stream?\n')
        try:
            num_tweets = int(user_input)
            break
        except ValueError:
            print("El valor introducido no es un número entero.\n")
    print('Comienza a ejecutarse el stream .....')
    auth = get_connection_api()
    listener = MyListener(file_name, num_tweets)
    api = tweepy.streaming.Stream(auth, listener, tweet_mode='extended')
    api.filter(languages=stream_language,
               track=query_list, async=False)
    return listener.status_list


def create_dataframe_from_list(tweet_list):
    columns = ['id', 'created_at', 'user', 'location', 'text',
               'full_text_flag', 'source', 'time_zone', 'from_file']
    index = pd.Series(tweet.id for tweet in tweet_list)
    rows_list = []
    for tweet in tweet_list:
        truncated = tweet.truncated
        if truncated:
            text = tweet.extended_tweet['full_text']
            full_text_flag = 'S'
        else:
            text = tweet.text
            full_text_flag = 'N'
        data = {'id': tweet.id,
                'created_at': tweet.created_at,
                'user': tweet.user.name,
                'location': tweet.user.location,
                'text': text.encode('ascii', 'ignore').lower(),
                'full_text_flag': full_text_flag,
                'source': tweet.source,
                'time_zone': tweet.user.time_zone,
                'from_file': 'direct_from_list'}
        rows_list.append(data)
    df_list = pd.DataFrame(rows_list, columns=columns, index=index)
    df_list.index.name = 'id'
    # Cambiamos el datatype de la columna created_at a datetime
    df_list['created_at'] = pd.to_datetime(df_list['created_at'])
    # Creamos la nueva columna con la hora
    df_list['hora'] = df_list['created_at'].dt.hour
    return df_list


def create_dataframe_from_json():
    """
    Método para obtener un pandas dataframe a partir de un archivo json que
    contiene tweets almacenados
    :return: dataframe con las columnas 'created_at', 'user', 'location',
    'text', 'full_text_flag, 'hora', 'source', 'time_zone', 'from_file'
     y como índices el id del tweet
    """
    columns = ['id', 'created_at', 'user', 'location', 'text',
               'full_text_flag', 'source', 'time_zone', 'from_file', 'hora']
    df_json = pd.DataFrame(columns=columns)
    for root, dirs, filenames in os.walk(dir_json):
        for f in filenames:
            print('Cargando archivo ' + f)
            file_path = os.path.join(dir_json, f)
            df_json = df_json.append(create_partial_df(file_path))
    return df_json


def create_partial_df(file_path):
    try:
        with open(file_path, 'r') as f:
            file_name_aux = os.path.basename(os.path.normpath(file_path))
            tweets = json.loads(f.read())
            index = pd.Series(x['id'] for x in tweets['tweets'])
            columns = ['id', 'created_at', 'user', 'location', 'text',
                       'full_text_flag', 'source', 'time_zone', 'from_file']
            rows_list = []
            for x in tweets['tweets']:
                soup = BeautifulSoup(x['source'], 'html5lib')
                source = soup.a.get_text()
                truncated = x['truncated']
                if truncated:
                    text = x['extended_tweet']['full_text']
                    full_text_flag = 'S'
                else:
                    text = x['text']
                    full_text_flag = 'N'

                data = {'id': x['id'],
                        'created_at': x['created_at'],
                        'user': x['user']['name'],
                        'location': x['user']['location'],
                        'text': text.encode('ascii', 'ignore').lower(),
                        'full_text_flag': full_text_flag,
                        'source': source.encode('ascii', 'ignore'),
                        'time_zone': x['user']['time_zone'],
                        'from_file': file_name_aux}
                rows_list.append(data)
        df_aux = pd.DataFrame(rows_list, columns=columns, index=index)
        df_aux.index.name = 'id'
        # Cambiamos el datatype de la columna created_at a datetime
        df_aux['created_at'] = pd.to_datetime(df_aux['created_at'])
        # Creamos la nueva columna con la hora
        df_aux['hora'] = df_aux['created_at'].dt.hour
        return df_aux
    except BaseException as ex:
        sys.stderr.write("Error on_data:{}\n".format(ex))
        time.sleep(5)
        return False


def create_menu_principal():
    print('Escoja entre una de la siguientes opciones')
    print('1- Búsqueda Con Stream')
    print('2- Estadísticas desde los json adjuntos (dir:./json)')
    print('3- Salir')
    option = int(input('Que opción desea?'))
    return option


def main():
    global df
    if os.path.isfile(file_name):
        os.remove(file_name)

    option = create_menu_principal()
    if option == 1:
        tweets_list = search_with_stream()
        df = create_dataframe_from_list(tweets_list)
    elif option == 2:
        df = create_dataframe_from_json()
    else:
        exit(0)

    # Instanciamos la clase MyTokenizerClass para poder trabajar con ella
    mtk = MyTokenizerClass()

    # Número de palabras y caracteres
    vect_num = np.vectorize(mtk.num_palabras_caracteres)
    df['num_palabras'] = [d['palabras'] for d in vect_num(df['text'])]
    df['num_caracteres'] = [d['caracteres'] for d in vect_num(df['text'])]

    print('\n')
    # Instanciamos la clase MyStatisticsClass para poder trabajar con ella
    msc = MyStatisticsClass(df)

    # Distribución de tweets a lo largo del día
    print('\n')
    msc.get_tweets_per_hour()

    # Distribución de los elementos de la consulta de filtrado
    print('\n')
    msc.get_count_of_query_words()

    # Distribución de las zonas horarias
    print('\n')
    msc.get_time_zone_distribution()

    # Distribución Fuentes de los tweets
    print('\n')
    msc.get_porcentaje_fuente_tweet()

    # Sentimental analysis de los tweets
    vect_pol = np.vectorize(msc.get_polarity_classification)
    df['sa'] = vect_pol(df['text'])
    print('\n')
    msc.get_sentimental_analysis()

    # Longitud media de los tweets
    print('\n')
    msc.get_media_longitud()

    # Tweets con mayor número de caracteres
    max_carac = msc.get_custom_max_min('num_caracteres', 'max')
    print('\n')
    print("El tweet más largo es: \n{}"
          .format((df['text'][max_carac['index']])))
    print("Nº de caracteres: {}".format(max_carac['valor']))

    # Tweets con menor número de caracteres
    min_carac = msc.get_custom_max_min('num_caracteres', 'min')
    print('\n')
    print("El tweet más corto es: \n{}"
          .format((df['text'][min_carac['index']])))
    print("Nº de caracteres: {}".format(min_carac['valor']))

    # Tweets totales
    print('\n')
    print("Total de tweets recogidos: {}".format(df.shape[0]))


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt as e:
        print(e.message)
