<img  align="left" width="150" style="float: left;" src="https://www.upm.es/sfs/Rectorado/Gabinete%20del%20Rector/Logos/UPM/CEI/LOGOTIPO%20leyenda%20color%20JPG%20p.png">
<img  align="right" width="60" style="float: right;" src="https://www.dit.upm.es/images/dit08.gif">


<br/><br/>


# Práctica Spark Scala - Procesado Batch

## 1. Objetivo

- Afianzar los conceptos sobre Spark
- Desplegar en local un programa Spark Scala

## 2. Dependencias

Para realizar la práctica el alumno deberá tener instalado en su ordenador:
- Herramienta GIT para gestión de repositorios [Github](https://git-scm.com/downloads)
- Scala versión 2.12
- Spark versión 3.1.2
- SBT
- Java 8
- Máquina virtual con sistema operativo linux y distribución Ubuntu 22.04 (Disponible en el enlace compartido en moodle) 


## 3. Descripción de la práctica

La práctica plantea una serie de ejercicios para afianzar los conocimiento sobre el desarrollo y desplieuge de aplicacines en entornos de Big Data, en este caso la herramienta que se utiliza es el motor de Big Data Apache Spark.

La práctica se desarrolla en tres bloques en donde se pretende ejemplificar como desplegar un job en Spark y desarrollar una apliación con Spark Scala.


## 4. Inicializar el entorno.

Para la realización de la práctica se ha provisionado una máquina virtual con sistema operativo linux y distribución Ubuntu. En los recursos de Moodle de la asignatura se puede acceder al enlace de descarga.

Descarge el fichero de VM con extensión .ova e importelo en virtualbox. 

Dentro de la máquina virtual abra un terminal y realice los siguientes pasos.

Instalar con sdkman la versión de Scala 2.12:
```
sdk install scala 2.12.15
```
Y configurarla como la versión por defecto:
```
sdk default scala 2.12.15
```

Instalar con sdkman Spark 3.1.2:

```
sdk install spark 3.1.2
```

Instalar con sdkman Java 8:

```
sdk install java 8.0.302-open
```

Descargar el repositorio:
```
git clone https://github.com/Big-Data-ETSIT/P5_spark_batch.git
```

Descargar y descoprimir en la carpeta `P5_spark_batch/books` los libros sobre los que se contarán las palabras (https://drive.upm.es/s/4QS9qau4qa819Q1)


## 5. Tareas a realizar.

### Bloque 1: Ejecutar el contador de palabras con sbt.

Usando como directorio de trabajo `P5_spark_batch/wordcount`

**Pregunta 1: Explicar qué hace el código WordCountBatch.scala**

Arrancar el servidor sbt:
```
sbt
```

Ejecutar dentro del servidor sbt el código del programa:
```
runMain es.upm.dit.WordCountBatchJob
```

Comprobar que se ha generado en la carpeta `P5_spark_batch/result` el .csv con el resultado de la ejecución. Esta ejecución únicamente utiliza las librerías de spark en el programa Scala.

### Bloque 2: Ejecutar el código con spark-submit.


En el apartado anterior ejecutamos el código con sbt, es decir, sin emplear el motor de Big Data Apache Spark. En este apartado se va a desplegar en local, utilizando el comando spark-submit. 

Para ello primero debemos compilar el código y empaquetarlo (generar el .jar). Utilizaremos sbt:

```
sbt compile
sbt package
```
Comprobar que se genera el .jar en la carpeta `P5_spark_batch/wordcount/target/scala-2.12`

Lanzar el programa con el comando spark-submit

```
spark-submit --class "es.upm.dit.WordCountBatchJob" target/scala-2.12/word-count_2.12-1.0.jar
```

**Pregunta 2: Explicar la diferencia entre ejecutar el job con spark-submit y con sbt.**

**Pregunta 3: ¿El entorno en el que estamos ejecutando la práctica (una máquina virtual dentro de nuestro ordenador personal) es un entrono válido para contar las palabras de un millón de libros? Justifique la respuesta.**



### Bloque 3: Desarrollar la aplicación LongCountBatchJob.scala.

En este apartado se pide crear un nuevo programa llamado `LongCountBatchJob.scala` que cuente las palabras largas (i.e., palabras con longitud mayor de 5 caracteres) y las guarde en un .csv en la carpeta `P5_spark_batch/result2`. 

Pista: utilizar la función `filter`. 

Además, debe ejecutarse con sbt y spark-submit.


**Captura 1: código del object LongCountBatchJob.scala**

**Captura 2: primeras líneas del .csv generado tras la ejecución con spark-submit.**


## 7. Instrucciones para la Entrega y Evaluación.
El alumno debe subir un fichero pdf a moodle en donde se incluyan las respuestas a las preguntas planteadas a lo largo de la práctica y las dos capturas solicitadas.



