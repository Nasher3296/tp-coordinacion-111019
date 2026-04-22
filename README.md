# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.

## Informe de implementación

Tomé la implementación del middleware del TP-MOM como base, pero sumando una corrección importante: la eliminación de la lógica de retry con exponential backoff. La decisión viene a raíz dos motivos. Por un lado, este TP no requiere manejar errores de conexión intermitente; para que ese mecanismo tuviera sentido sería necesario que una queue o exchange desaparezca en plena publicación, lo cual no es un escenario considerado en este contexto. Por otro lado, mi implementación original tenía un error grave: el `Channel` de amqp091-go no es seguro para uso concurrente, y estaba realizando publicaciones desde dos goroutines simultáneamente —desde `Send` y desde `sendRetry`. En el momento no me dí cuenta sinceramente. Como solución, mantengo la escucha del canal `return` pero sin buffer acotado, limitando su rol al logueo.

### Alternativa descartada: sincronización por prefetch

Durante la cursada se planteó en clase la posibilidad de sincronizar los Sums limitando el prefetch a 1. La idea es que con un solo mensaje en vuelo a la vez, al recibir el EOF se tiene la certeza de que todos los mensajes anteriores ya fueron procesados y reconocidos, eliminando la necesidad de un protocolo de coordinación explícito. El costo es un overhead importante: cada mensaje requiere un round-trip de ACK antes de que el broker entregue el siguiente, lo que penaliza fuertemente el throughput. A cambio, la sincronización es trivial y la cantidad de mensajes de coordinación se reduce a un simple `Confirm` hacia los demás Sums al finalizar.

Opté por no tomar ese camino. La penalización en throughput de procesar un mensaje a la vez en la etapa de ingesta de datos es demasiado alta. En cambio, mantener el prefetch por defecto permite consumir en ráfaga y aprovechar el paralelismo del broker. El costo es un protocolo de coordinación más elaborado: por cada EOF se intercambian una `CountQuery` y una `CountReply` por cada Sum participante, más el `Confirm` final, con posibilidad de repetir la ronda si los conteos aún no coinciden. Es más mensajes, pero el trabajo de coordinación ocurre solo una vez al finalizar la ingesta, mientras que la ganancia en velocidad de procesamiento aplica a cada mensaje de datos recibido.

### Coordinación entre instancias de Sum

Para la coordinación entre instancias de Sum opté por un esquema de coordinador. No existe un coordinador designado, sino que la instancia de SUM que reciba el EOF de la queue común va a actuar de coordinador para ese cliente específicamente. Esto implica que varios SUMS pueden ser coordinadores incluso en simultáneo, pero de distintos clientes. También un SUM podría ser coordinador en simultáneo de más de un cliente. 

Cada instancia de Sum consume de dos fuentes: una cola compartida de la que leen todos los Sums (donde llegan los registros del gateway), y una cola dedicada por instancia que se utiliza exclusivamente para los mensajes de coordinación. Cada Sum conoce el nombre de la cola dedicada de todos los demás, lo que permite publicarles directamente sin necesidad de un exchange intermediario. Esto implica dos goroutines por instancia: una para la cola compartida y otra para la cola de coordinación. Dado que ambas goroutines acceden al estado por cliente (en particular al conteo de mensajes recibidos), el acceso está protegido con un mutex.

Para que esto sea posible, modifiqué el `message_handler` del Gateway en dos aspectos: por un lado, asigna un ID único a cada cliente que se conecta; por otro, lleva un conteo de la cantidad de registros serializados hacia la cola interna por cada cliente. Ambos datos viajan en el payload del EOF que el Gateway emite al finalizar la ingesta de ese cliente.

Cuando una instancia de Sum recibe ese EOF en la cola compartida, conoce el total de registros que envió el cliente. En ese momento asume el rol de coordinador para dicho cliente y publica una `CountQuery` en la cola dedicada de cada uno de los demás Sums. La query incluye el ID del cliente, el ID del coordinador y un número de ronda.

Cada Sum no-coordinador que recibe la query responde con su conteo de mensajes procesados para ese cliente, escribiendo un `CountReply` en la cola dedicada del coordinador.

El coordinador acumula las respuestas. Una vez que recibió la de todos los demás Sums (es decir, N-1 respuestas siendo N el total de instancias), suma los conteos recibidos al propio y lo compara contra el total esperado. Hay dos casos:

- **Los conteos coinciden**: el coordinador envía un `Confirm` a todos los Sums y procede a enviar su data al Aggregator.
- **Los conteos no coinciden**: necesariamente se recibió menos de lo esperado, lo que indica que aún hay mensajes en tránsito o encolados en algún Sum. Se incrementa el número de ronda y se repite la `CountQuery`.

Al recibir el `Confirm`, cada Sum no-coordinador también envía su data acumulada al Aggregator.

### Distribución hacia los Aggregators

Cada instancia de Sum conoce la cola dedicada de cada Aggregator. Para determinar a cuál enviar cada fruta, se aplica un hash sobre el par `(clientID, nombre de fruta)` y se toma el módulo por la cantidad de Aggregators. Incluir el `clientID` en el hash mejora la distribución: con varios clientes concurrentes, la misma fruta de distintos clientes puede ir a Aggregators distintos. La correctitud se mantiene porque todos los Sums aplican la misma función de hash, garantizando que todas las sumas parciales de un mismo par `(cliente, fruta)` lleguen siempre al mismo Aggregator.

Una vez enviadas todas las frutas del cliente, cada Sum envía un EOF a **todos** los Aggregators, independientemente de si procesó datos de ese cliente o no. Esto asegura que el total de EOFs que recibe cada Aggregator por cliente sea siempre N (uno por cada instancia de Sum), sin depender de la distribución de los datos.

### Coordinación entre Aggregators y el Joiner

Cada Aggregator espera recibir N EOFs por cliente (uno por cada Sum) antes de procesar. Al completarlos, calcula un top parcial y envía **un único mensaje** al Joiner con ese resultado. No hay un EOF separado: el propio mensaje del top parcial actúa como señal de finalización.

El Joiner, a su vez, espera recibir M mensajes por cliente, siendo M la cantidad de Aggregators. Una vez recibidos todos, construye el top final y lo envía al Gateway. La cantidad de mensajes que el Joiner espera es fija e independiente de la cantidad de frutas o de registros originales del cliente.

