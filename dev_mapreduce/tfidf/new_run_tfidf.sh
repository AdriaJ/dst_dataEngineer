#!/usr/bin/env bash

# Make sure we have all the arguments
if [ $# -ne 2 ] && [ $# -ne 3 ]; then
   printf "./run_tfidf.sh <input_dir> <output_dir> [conf]\n"
   printf " Répertoire HDFS où est stocké le corpus\n"
   printf " output_dir = Répértoire HDFS où seront stockés les résultats\n"
   printf " conf = Options de configuration Hadoop\n"
   exit -1
fi

# Compile Java  files
javac -cp $(hadoop classpath) W*.java

#create jar file
jar cvf tfidf.jar W*.class

# Suppression des répertoires de sortie : inutile car dans le code des drivers
#${HADOOP_HOME}/bin/hadoop fs -rmr $OUTPUT/tfidf1 >& /dev/null
#${HADOOP_HOME}/bin/hadoop fs -rmr $OUTPUT/tfidf2 >& /dev/null

# Récupération des paramètres declare
declare INPUT=$1;
declare OUTPUT=$2;
declare CONFIG=$3;

#${HADOOP_HOME}/bin/hadoop fs -rmr $OUTPUT/tfidf3 >& /dev/null

# Execution des jobs
printf "\nJob 1 en cours d'exécution : Calcul de la fréquence des mots par document\n" 
${HADOOP_HOME}/bin/hadoop jar tfidf.jar WordFrequenceInDoc $CONFIG $INPUT $OUTPUT/tfidf1
printf "\nJob 2 en cours d'exécution : Comptage du nombre de mots par document\n" 
${HADOOP_HOME}/bin/hadoop jar tfidf.jar WordCountsForDocs $CONFIG $OUTPUT/tfidf1 $OUTPUT/tfidf2 
printf "\nJob 3 en cours d'exécution : Calcul du nombre de documents dans le corpus et des TF-IDF\n"
${HADOOP_HOME}/bin/hadoop jar tfidf.jar WordsInCorpusTFIDF $CONFIG $INPUT $OUTPUT/tfidf2 $OUTPUT/tfidf3

printf "Repertoire de sortie"
${HADOOP_HOME}/bin/hadoop fs -ls $OUTPUT/tfidf3

printf "Queue du résultat"
${HADOOP_HOME}/bin/hadoop fs -tail $OUTPUT/tfidf3/part-r-00000
