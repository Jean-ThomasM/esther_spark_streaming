# spark-streaming

Projet Simplon Data Engineer : Analyse de flux de données en temps réel avec Spark Structured Streaming
Décembre 2025
Jean-Thomas Miquelot

Principe : utiliser PyPark Streaming pour récupérer des données comme un flux, d'abord depuis un dossier (récupération de fichiers json), puis avec un flux simulé avec Kafka.

### Flux simple : 
Lecture du dossier out/sensor_data, traitement des fichiers json fichier par fichier, écriture dans delta_bronze.

### Flux Kafka : 
Récupération des fichiers json, envoie ligne par ligne des fichier dans Kafka, puis récupération de ces données via PySpark Streaming, conversion en json, puis écriture dans delta_bronze.


Configuration technique :
- l'environnement virtuel est géré avec UV
- les notebooks jupyter sont gérés directement dans VScode
  

Etapes pour lancer le programme :

Se placer à la racine du projet.

Utiliser la commande "uv sync" pour créer l'environnement virtuel :

```sh
uv sync
```

# PIPELINE SIMPLE

Cette partie se compose des éléments suivants :
- data/sensor_data : les données json
- pipeline_files_pyspark.ipynb : notebook contenant toutes les étapes de l'import des données json jusqu'à l'enregistrement de la table delta_silver




## Instructions d'exécution :
- Ouvrir le notebook scripts/pipeline_files_pyspark.ipyn
- Lancer les cellules une par une OU cliquer sur "Run All"
-> Cela va lancer l'extraction des données à partir des json dans le dossier data/sensor_data
-> Cela va créer la table delta_bronze

## Vérifier le contenu de delta_bronze :
- Ouvrir le notebook scripts/data_exploration.ipynb
- lancer la première cellule
- lancer les cellules de la partie "Exploration Table Delta Pipeline Classique"
- la dernière cellule permets de modifier une requête SQL simplement




# PIPELINE KAFKA
Cette partie se compose des éléments suivants :
- data/sensor_data : les données json (les mêmes que le pipeline fichiers)
- docker-compose.yml : lancement des conteneurs zookeeper & kafka
- json_to_kafka.py : script python pour envoyer les fichiers json à kafka
- kafka_to_delta.py : script python qui récupère le flux kafka pour écrire dans la table delta_bronze_kafka
- kafka_bronze_to_silver : script python qui récupère la table delta_bronze_kafka comme un flux, nettoie les données, et écrit dans la table delta_silver_kafka



## Instructions d'exécution :
- Se mettre à la racine du projet, lancer docker compose :
```sh
docker compose -d
```
- attendre 30 secondes et vérifier que les conteneurs sont bien "UP" (Kafka peut crasher silencieusement au bout de plusieurs secondes) :
```sh
docker ps
```
- lancer le script python json_to_kafka
```sh
uv run scripts/json_to_kafka.py
```
- dans une autre fenêtre de terminal lancer le script python kafka_to_delta
```sh
uv run scripts/kafka_to_delta.py
```
- dans une troisième fenêtre de terminal lancer le script python kafka_bronze_to_silver
```sh
uv run scripts/kafka_bronze_to_silver.py
```
- interrompre manuellement les terminaux des scripts kafka_to_delta et kafka_bronze_to_silver

## Vérifier le contenu de delta_bronze :
- Ouvrir le notebook scripts/data_exploration.ipynb
- lancer la première cellule
- lancer les cellules de la partie "Exploration Table Delta Pipeline avec Kafka"
- données affichées : aperçu des 20 premières lignes, aperçu avec Pandas, requêtes possibles avec SQL
- la dernière cellule permets de modifier une requête SQL simplement

# Erreurs possibles :
- l'ordre d'exécution des scripts (json_to_kafka // kafka_to_delta // kafka_bronze_to_silver) doit être respecté, sinon erreur
- si erreurs avec les notebooks : 
  - restart
  - relancer les cellules