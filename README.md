
AUTHOR: Pedro Burgo Vázquez

DATE: 22/12/2017


#### Descripción de la Práctica

Como Tarea del Módulo 4 del Master de Big Data de Telefónica, se realiza un *streaming* de 24 horas usando la api
de *tweepy*. Como parámetros de filtrado se han usado:
 
	` track = ['Curie', 'Planck', 'Einstein', 'Bohr', 'Fleming', 'Higgs']`

	`languages = ['en']`

En cada cambio de hora, se han exportado los *tweets* recogidos a un archivo *json* y guardado en un directorio separado (`'./jsons'`).
Las imágenes también se han guardado en su propio directorio (`'./images'`).

La clase `MyListener(StreamListener)` que se presenta en el archivo *ejercicio_python.py*, recupera el número de tweets que solicita el usuario a través de un input:
``` 
    while True:
        user_input = raw_input(
            'Cuantos tweets desea recuperar con el stream?\n')
        try:
            num_tweets = int(user_input)
            break
        except ValueError:
            print("El valor introducido no es un número entero.\n")
```            
            
En el archivo adjunto *my_listener.py* se muestra la clase tal y como se usó para recuperar los tweets y crear los jsons cada cambio de hora.

Total de tweets recogidos: 11.385 

Se muestra el resultado de las estadísticas en archivo M4_Tarea_Estadisticas.pdf