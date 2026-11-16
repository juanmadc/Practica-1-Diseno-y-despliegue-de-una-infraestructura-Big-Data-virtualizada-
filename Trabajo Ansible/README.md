cat > README.md << 'EOF'
# Práctica 1: Infraestructura Big Data virtualizada (HDFS + Spark)

**Autores:** JUAN MANUEL DELFA CASADO Y RAUL DEL FRESNO PEREZ
**Repositorio git:** https://github.com/juanmadc/Practica-1-Diseno-y-despliegue-de-una-infraestructura-Big-Data-virtualizada-.git

## Arquitectura

| VM | IP | Contenedores Docker |
|---|---|---|
| master | 192.168.56.20 | namenode, spark-master |
| worker1 | 192.168.56.21 | datanode, spark-worker |
| worker2 | 192.168.56.22 | datanode, spark-worker |

- HDFS con factor de replicación 2 (`dfs.replication=2`).
- Spark en modo standalone (master en `spark://master:7077`).
- Todo el software HDFS/Spark corre en contenedores Docker, desplegados con roles de Ansible.

## Estructura del proyecto

- `Vagrantfile`: define las 3 VMs y lanza Ansible.
- `site.yml`: playbook principal.
- `requirements.yml`: colecciones de Ansible (community.docker).
- `roles/`: common, namenode, datanode, spark_master, spark_worker.
- `jobs/wordcount.py`: job Spark de ejemplo.

## Requisitos previos
VirtualBox, Vagrant y Ansible instalados.

## Despliegue
```bash
vagrant up
```
Si falla el aprovisionamiento: `vagrant provision`.

## Interfaces web
- NameNode: http://192.168.56.20:9870
- Spark Master: http://192.168.56.20:8080
- Spark Workers: http://192.168.56.21:8081 y http://192.168.56.22:8081

## Prueba 1. Subir un fichero a HDFS y comprobar la replicación (desde master todo)
```bash
vagrant ssh master
docker exec hdfs-namenode bash -c 'seq 1 20000 | awk "{print \"Linea \" \$1 \": hola mundo spark hdfs docker ansible vagrant\"}" > /tmp/prueba.txt'
docker exec hdfs-namenode wc -l /tmp/prueba.txt
docker exec hdfs-namenode hdfs dfs -mkdir -p /data
docker exec hdfs-namenode hdfs dfs -D dfs.replication=2 -put -f /tmp/prueba.txt /data/prueba.txt

# La 2ª columna debe ser 2
docker exec hdfs-namenode hdfs dfs -ls /data
# Cada bloque debe mostrar repl=2 y dos DataNodes
docker exec hdfs-namenode hdfs fsck /data/prueba.txt -files -blocks -locations
```

## Prueba 2. Lanzar el job de Spark con salida en HDFS (desde master todo)
```bash
# Con este comando vemos cuantas tareas realiza cada worker, deberian tener el mismo numero
docker exec spark-master /opt/spark/bin/spark-submit /jobs/wordcount.py hdfs:///data/prueba.txt hdfs:///data/salida
# Con este comando comprobamos si se ha ejecutado bien el comando anterior, _SUCCESS=TABIEN
docker exec hdfs-namenode hdfs dfs -ls /data/salida
# Este comando muestra el numero de palabras repetidas que ha contado
docker exec hdfs-namenode hdfs dfs -cat /data/salida/part-* | head
```

## Prueba 3. Parar un worker y leer el fichero
```bash
# Primero, comprobamos que los dos workers, aparecen vivos; si no, refrescamos la pagina de ambos (desde master).
docker exec hdfs-namenode hdfs dfsadmin -report | grep -i "live datanodes"
# Paramos el worker 1
exit
vagrant ssh worker1
docker stop hdfs-datanode spark-worker
# Leemos el fichero, pero con un datanode caido, no debe dar fallos porque el cliente lee la replica que sigue viva en el worker2
docker exec hdfs-namenode hdfs dfs -cat /data/prueba.txt | head -3
docker exec hdfs-namenode hdfs dfs -cat /data/salida/part-00000 | head -3
# Esperamos la deteccion del fallo, debe salir un datonode unicamamente (porque uno de los workers esta caido) y los bloques replicados deben estar sanos (HEALTHY)
docker exec hdfs-namenode hdfs dfsadmin -report | grep -i "live datanodes"
docker exec hdfs-namenode hdfs fsck /data/prueba.txt | grep -E "Under|Status|Live"
# Lanzamos el job con un solo worker esta vez y añadimos un grep para que filtre la salida y nos muestre lo que queremos
docker exec spark-master /opt/spark/bin/spark-submit /jobs/wordcount.py hdfs:///data/prueba.txt hdfs:///data/salida_1worker 2>&1 | grep -E "Tareas por nodo|Palabras distintas"
# Levantamos el worker1
exit
vagrant ssh worker1
docker start hdfs-datanode spark-worker
```
Los datos siguen disponibles gracias a la réplica en worker2 (fsck puede mostrar bloques "under-replicated", pero ninguno missing ni corrupt). Para restaurar: `vagrant up worker1`.
EOF


## Experimento A. Reparto de carga en Spark (2 workers vs 1 worker)

# Generamos un fichero de prueba en HDFS para medir tiempos y picos de CPU en Grafana
vagrant ssh master -c 'sudo docker exec hdfs-namenode bash -c "seq 1 8000000 > /tmp/datos_a.txt && hdfs dfs -mkdir -p /data && hdfs dfs -put -f /tmp/datos_a.txt /data/datos_a.txt && hdfs dfs -rm -r -f /data/salida_2workers /data/salida_1worker"'

# 1. Lanzamos el job wordcount.py con los 2 workers activos y medimos el tiempo de ejecucion
vagrant ssh master -c "time sudo docker exec spark-master /opt/spark/bin/spark-submit /jobs/wordcount.py hdfs:///data/datos_a.txt hdfs:///data/salida_2workers 2>&1 | grep -E 'Tareas por nodo|Palabras distintas|real'"

# 2. Paramos el contenedor spark-worker en worker2 y repetimos el job con 1 solo worker activo
vagrant ssh worker2 -c "sudo docker stop spark-worker"
vagrant ssh master -c "time sudo docker exec spark-master /opt/spark/bin/spark-submit /jobs/wordcount.py hdfs:///data/datos_a.txt hdfs:///data/salida_1worker 2>&1 | grep -E 'Tareas por nodo|Palabras distintas|real'"

# Comprobamos en Grafana ([http://192.168.56.20:3000/d/cluster-big-data](http://192.168.56.20:3000/d/cluster-big-data)) la diferencia de tiempo y como en la primera ejecucion el pico de CPU se reparte entre .21 y .22 y en la segunda recae solo sobre .21

# Volvemos a levantar spark-worker en worker2
vagrant ssh worker2 -c "sudo docker start spark-worker"




## Experimento B: Caida de un worker


1. **Preparación de datos replicados en HDFS:**
   Subir un fichero de prueba con factor de replicación `2` para garantizar una copia en `worker1` y otra en `worker2`:
   ```bash
   vagrant ssh master -c 'NN=$(sudo docker ps -q -f "name=namenode") && sudo docker exec $NN bash -c "echo Datos_Experimento_B > /tmp/prueba_b.txt && hdfs dfs -put -f /tmp/prueba_b.txt /prueba_b.txt && hdfs dfs -setrep -w 2 /prueba_b.txt"'
   ```

2. **Simulación de caída del nodo `worker2`:**
   Apagar la máquina virtual `worker2` (`192.168.56.22`):
   ```bash
   vagrant halt worker2
   ```

3. **Verificación de alerta temprana y disponibilidad de lectura (minuto 1):**
   - Comprobar en Prometheus (`http://192.168.56.20:9090/alerts`) y en el grupo de Telegram el disparo de la alerta **`NodoOServicioCaido`** tras cumplirse `for: 1m`.
   - Verificar que el fichero sigue siendo accesible desde la réplica almacenada en `worker1`:
     ```bash
     vagrant ssh master -c 'sudo docker exec $(sudo docker ps -q -f "name=namenode") hdfs dfs -cat /prueba_b.txt'
     ```

4. **Detección interna de DataNode muerto en HDFS (~10 minutos y 30 segundos):**
   - Observar en el panel **«Clúster Big Data»** de Grafana (`http://192.168.56.20:3000/d/cluster-big-data`) que `fs_namesystem_num_live_data_nodes` baja a `1`, `fs_namesystem_num_dead_data_nodes` sube a `1` y `fs_namesystem_under_replicated_blocks` pasa a ser mayor que `0` una vez expirado el *heartbeat* de Hadoop (630 s).
   - Confirmar en Telegram la llegada de las alertas **`HDFSDataNodeCaido`** y **`HDFSBloquesSubReplicados`**.

5. **Recuperación del nodo y resolución de alertas:**
   Volver a arrancar `worker2` y comprobar en Grafana y Telegram que todos los objetivos regresan a `UP`, los bloques sub-replicados vuelven a `0` y se reciben los avisos `[ALERTA RESUELTA]`:
   ```bash
   vagrant up worker2
   ```



## Experimento C. Línea base del clúster en reposo (30 minutos) y dimensionamiento
bash
# Mantenemos el cluster encendido y sin jobs activos durante 30 minutos
vagrant status

# En Grafana ([http://192.168.56.20:3000/d/cluster-big-data](http://192.168.56.20:3000/d/cluster-big-data)) seleccionamos el rango "Last 30 minutes" y revisamos la fila "2. ¿Van justos los recursos?":
# - CPU y Steal por VM (%): < 6% en reposo en los 3 nodos (steal ~0%)
# - Memoria RAM por VM (%): ~46% en master (justifica ampliar el master a 4096 MB en el Vagrantfile) y ~26% en worker1 y worker2
# - Disco Raiz por VM (%): ~35% estable gracias al limite de retencion de Prometheus (7d / 1GB)