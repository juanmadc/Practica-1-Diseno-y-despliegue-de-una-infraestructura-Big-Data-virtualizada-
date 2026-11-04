cat > README.md << 'EOF'
# Práctica 1: Infraestructura Big Data virtualizada (HDFS + Spark)

**Autores:** JUAN MANUEL DELFA CASADO Y RAUL DEL FRESNO PEREZ
**Repositorio git:** https://github.com/USUARIO/NOMBRE-REPO

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

## Prueba 1. Subir un fichero a HDFS y comprobar la replicación
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

## Prueba 2. Lanzar el job de Spark con salida en HDFS
```bash
# Con este comando vemos cuantas tareas realiza cada worker, deberian tener el mismo numero
docker exec spark-master /opt/spark/bin/spark-submit /jobs/wordcount.py hdfs:///data/prueba.txt hdfs:///data/salida
# Con este comando comprobamos si se ha ejecutado bien el comando anterior, _SUCCESS=TABIEN
docker exec hdfs-namenode hdfs dfs -ls /data/salida
# Este comando muestra el numero de palabras repetidas que ha contado
docker exec hdfs-namenode hdfs dfs -cat /data/salida/part-* | head

## Prueba de tolerancia a fallos
```bash
vagrant halt worker1
vagrant ssh master
docker exec namenode hdfs dfs -cat /data/big.txt | head -3
docker exec namenode hdfs fsck /data/big.txt
```
Los datos siguen disponibles gracias a la réplica en worker2 (fsck puede mostrar bloques "under-replicated", pero ninguno missing ni corrupt). Para restaurar: `vagrant up worker1`.
EOF

