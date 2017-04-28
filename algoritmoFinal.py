# -*- coding: utf-8 -*-

###############################################################################
# Autores: Asier Aranda, Marta Cela, Marcos López García, Alberto Pueyo, Candela Vidal
# Proyecto: Bid Delfos para DiezPuntoCero como parte del curso de Telefónica
# Empleo Digital
# Fecha: 27 de abril del 2017
# Versión de Python 2.7.13. Spark 1.6
# Descripción: El programa se encarga de limpiar y analizar un texto para
# proporcionar un conteo de las palabra significativas y almacenarlas en una
# base de datos 
###############################################################################

# Librerías importadas

from nltk.corpus import stopwords
from string import punctuation
from pyspark import SparkContext, SparkConf
from pymongo import MongoClient
from Tkinter import *
from tkFileDialog import askopenfilename
import ntpath
from datetime import *


# VARIABLES Y PARARMETROS INICIALES

# Variables iniciales

diccionario = []
compuestas = []
lista = []
client = MongoClient()
conf = SparkConf()
sc = SparkContext(conf=conf)
sp_stopwords = []

# GUI que permite seleccioinar el archivo a tratar

Tk().withdraw() 
rutafichero = askopenfilename() 

# Implementación de el diccionario de palabras vacías de significado


sp_stopwords = stopwords.words('spanish')
entrada = open('stopwords.txt', 'r')
for palabra in entrada:
    sp_stopwords.append(palabra.decode('utf8'))

sp_stopwords.extend(['a'.decode('utf8'),'b'.decode('utf8'),'c'.decode('utf8'),'d'.decode('utf8'),'e'.decode('utf8'),'f'.decode('utf8'),'g'.decode('utf8'),'h'.decode('utf8'),'i'.decode('utf8'),'j'.decode('utf8'),'k'.decode('utf8'),'l'.decode('utf8'),'m'.decode('utf8'),'n'.decode('utf8'),'o'.decode('utf8'),'p'.decode('utf8'),'q'.decode('utf8'),'r'.decode('utf8'),'s'.decode('utf8'),'t'.decode('utf8'),'u'.decode('utf8'),'v'.decode('utf8'),'w'.decode('utf8'),'x'.decode('utf8'),'y'.decode('utf8'),'z'.decode('utf8'),'vídeo'.decode('utf8'),'así'.decode('utf8')])
sp_stopwords.extend(['qué'.decode('utf8')])






# Implementación de los diccionarios de signos de puntuación a tratar

puntuacion_si = [32, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 193, 201, 205, 199, 209, 211, 218, 220, 225, 231, 233, 237, 241, 243, 250, 252]

#puntuacion = list(punctuation)
#puntuacion.extend(map(str,range(10)))

#puntuacion2 = [ ord('¡'.decode('utf8')), ord('¿'.decode('utf8')),ord('º'.decode('utf8')),ord('ª'.decode('utf8')),ord('¥'.decode('utf8')),ord('÷'.decode('utf8') ),ord('±'.decode('utf8')),ord('©'.decode('utf8')),ord('®'.decode('utf8')) ]


# Creacción del RDD con el texto a tratar
mifich = sc.textFile("file:"+ rutafichero)


# Obtención de nombre del fichero para obtener los datos de categoría y fecha del mismo
nombrefichero = ntpath.basename(rutafichero)
datos = nombrefichero.replace(".txt","").split("_")

fecha = date(int(datos[1]), int(datos[2]), int(datos[3]))
fecha = fecha.isocalendar()

# METODOS IMPLEMENTADOS PARA EL USO DEL PROGRAMA


###############################################################################################
# Función: buscarPalabrasCompuestas
# Parámetros de entrada:
#                       texto:  contiene el texto en el que se buscan
#                       las palabras
# Parámetros de salida:
#                       compuestas:  contiene las palabras candidatas
#                       a ser palabras compuestas
# Descripción: el método es la rutina de un map() que se encarga de buscar palabras
# compuestas y nombres propios basándose en si hay palabras que empiezan por mayúscula juntas 
###############################################################################################


def buscarPalabrasCompuestas(texto):
    mayusculas = [ 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'Ñ', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z','Á','É','Í','Ó','Ú']
    anterior = False
    palabraaux = ""

    listabpc = texto.split(".")


    for i in listabpc:
        listabpc2 =i.split(',')


        for j in listabpc2:
            listabpc3 = j.split(" ")

            for ind,k in enumerate(listabpc3):
                
                if k != "":
                    if k[0] in list(map(lambda x: x.decode('utf8'), mayusculas)):
                        if anterior == False:
                            
                            palabraaux += k
                        else:
                            palabraaux += " "
                            palabraaux += k
                            
                        anterior = True
                        if ind == (len(listabpc3)-1):
                            compuestas.append(palabraaux)
                            palabraaux = ""
                            anterior = False
                            
                    if k[0] not in list(map(lambda x: x.decode('utf8'), mayusculas)):
                        if anterior == True  :
                            compuestas.append(palabraaux)
                            palabraaux = ""
                        anterior = False
                    

    return compuestas

#################################################################################
# Función: palabrasCompuestas
# Parámetros de entrada:
#                       texto: contiene el texto en el que se buscan
#                       las palabras
# Parámetros de salida:
#                       lista: contiene las palabras candidatas
#                       a ser palabras compuestas
# Descripción: el método devuelve una lista de las palabras compuestas presentes
# en el texto
#################################################################################

def palabrasCompuestas(texto):

        
        for i in diccionario:  
            a = texto.count(i)

            for x in range(a):
                lista.append(i)

        return lista

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
##################################################################################
# Función: limpiarTexto
# Parámetros de entrada:
#                       texto: tipo str que contiene el texto en el que se buscan
#                       las palabras
# Parámetros de salida:
#                       texto: contiene el texto sin las palabras compuestas
# Descripción: el método elimina del texto aquellas palabras compuestas que están
# presentes en el texto
##################################################################################

def limpiarTexto(texto):

	for i in addCompuestas:
		texto = texto.replace(i[0],"")

	return texto
##################################################################################
# Función: mapaPalabras
# Parámetros de entrada:
#                       texto:  contiene el texto en el que se buscan
#                       las palabras
# Parámetros de salida:
#                       salida: contiene una lista con las palabras y la distancia
#                       media entre la palabra y la principal, así como el número
#                       de veces con el que se calcula la media
# Descripción: el método elimina del texto aquellas palabras compuestas que están
# presentes en el texto
##################################################################################
def mapaPalabras(texto):
    principal = False
    c=0

    posicion1 = 0
    posicion2 = 0
    valor = 0.0
    salida = []
    texto = texto.split(" ")
    for auxiliar in palabra2:

        c=0
        posicion1 = 0
        posicion2 = 0
        valor = 0.0
        principal = False

        for ind,i in enumerate(texto):

            i = i.replace(",","").replace(".","")
                

            if i == palabra.decode('utf8'):

                posicion1= ind
                if principal == False and posicion2 !=0:
                    c+=1
                    valor = valor+(posicion1-posicion2)
                principal = True
                    

            if i == auxiliar.decode('utf8') :

                posicion2 = ind
                if principal == True:
                    c+=1
                    valor = valor+(posicion2-posicion1)
                principal = False

        if c !=0:
            valor = valor/c
            
            salida.append((palabra, auxiliar, valor,c))
            
    return salida

# RUTINA PRINCIPAL DEL PROGRAMA

# Se buscan candidatas a palabras compuestas en el texto

pcompuestas = mifich.map(buscarPalabrasCompuestas)

candidatasCompuestas = pcompuestas.collect()
candidatas = candidatasCompuestas[-1]

candidatasFinal= []

for i in candidatas:
    i = ''.join([char for char in i if ord(char) in puntuacion_si])

    candidatasFinal.append(i)


# Se determina si las candidatas a palabras compuestas cumplen o no los criterios para ser añadidas al diccionario

dicFichero = open('diccionario.txt','r')


for palabra in dicFichero:
    
    diccionario.append(palabra.replace('\n',"").decode('utf8'))


dicFichero.close()
dicFichero = open('diccionario.txt','w')

cadidatas = sc.parallelize(candidatasFinal).map(lambda x : (x,1)).reduceByKey(lambda x,y : x+y).collect()


for palabra in cadidatas:
    if palabra[1] >= 2 and palabra[0] not in diccionario and palabra[0].lower().encode('utf8') not in list(map(lambda x: x.encode('utf8'), sp_stopwords)):
     
        diccionario.append(palabra[0])

for palabra in diccionario:
    dicFichero.write(palabra.encode('utf8')+'\n')



# Se cuentan las palabras compuestas y propias del diccionario en el texto

lista2 = mifich.map(palabrasCompuestas)


lista3 = lista2.collect()


contadorCompuestas = sc.parallelize(lista3[-1]).map(lambda x: (x,1)).reduceByKey(lambda x,y : x+y)


addCompuestas =contadorCompuestas.collect()



# Se limpia el texto eliminando de él las palabras compuestas


previo = mifich.map(limpiarTexto)


# Tratamiento de palabras simples

# Se elimina la puntuación del texto

tran2 = previo.map(lambda reg : ''.join([char for char in reg if ord(char) in puntuacion_si]))


#tran1 = previo.map(lambda reg :''.join([char for char in reg if char not in puntuacion]))

#tran2 = tran1.map(lambda reg : ''.join([char for char in reg if ord(char) not in puntuacion2]))

# Se eliminan las palabras que están en la lista de palabras vacías de significado


tran3 = tran2.flatMap(lambda reg : reg.split(' ')).map(lambda x : x.lower())



tran4 = tran3.filter(lambda palabra : palabra not in sp_stopwords)


# se realiza el conteo de palabras 

tran5 = tran4.map(lambda x : (x,1))

salida = tran5.reduceByKey(lambda x,y : x+y)

salida1 = salida.collect()




# se configura el acceso a mongo DB
db = client.test_database
collection = db.test_collection2

palabra = ""


# se añaden a la base de datos las palabras compuestas y nombres propios
       
for i in addCompuestas:
        post = {'palabra': i[0], 'cantidad':i[1], 'categoria': datos[0], 'dia' : datos[3], 'semana': fecha[1], 'año' : datos[1]}
        collection.insert(post)

# se añaden a la base de datos las palabras simples
for i in salida1:
        post = {'palabra': i[0], 'cantidad':i[1], 'categoria': datos[0], 'dia' : datos[3], 'semana': fecha[1], 'año' : datos[1]}
        collection.insert(post)

#Mapa de aparición
#Creacion de la interfaz
tk = Tk()
#Título de la interfaz
tk.title("Parámetros Mapa Aparición")
#Dimensiones de la interfaz



# palabra principal respecto a la que hacer el mapa de calor
palabra = "zapatos"


# lista de palabras con las que se quiere comparar la principal
palabra2= ["Madrid", "León", "Granada", "Ponferrada", "Valencia", "Barcelona", "Sevilla"]


salidaMapa2 = mifich.map(mapaPalabras)
salidaMapa3 = salidaMapa2.collect()

sal = salidaMapa3 


ficheroMapa = open(palabra+".txt","w")

listaMapaAux = []
minimo = 100000
maximo = 0

for i in palabra2:
    valor=0
    cantidad = 0

    for ele in sal:
        if ele !=[]:
            for tupla in ele:
                if tupla[1] ==i:
                    valor += tupla[2]*tupla[3]
                    cantidad += tupla[3]
    #ficheroMapa.write(i + ";"+str((valor/cantidad))+'\n')
    if cantidad !=0:
        
        listaMapaAux.append((i,valor/cantidad))

for i in listaMapaAux:
    if i[1] < minimo:
        minimo = i[1]
    if i[1] > maximo:
        maximo = i[1]

ficheroMapa.write("Clave;Palabra;Distancia;Relación\n")
for i in listaMapaAux:
    
    if (maximo-minimo) != 0:

        ficheroMapa.write(palabra +';'+i[0] + ";"+str(int(round(i[1])))+";"+str(int(round(100-((i[1]-minimo)/(maximo-minimo))*100)))+'\n')
    else:
        ficheroMapa.write(palabra +';'+i[0] + ";"+str(int(round(i[1])))+";"+str(int(round(100-((i[1]-minimo)/(1))*100)))+'\n')
    


