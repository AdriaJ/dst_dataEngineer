
#!/usr/bin/env bash

###############################################################################
# Ce script est utilisé pour calculer le TF-IDF à l'aide 3 jobs MapReduce
#
# Usage:
#    ./run_tfidf.sh <input_dir> <output_dir> [conf]
#  
#  where:
#     input_dir  = Répertoire HDFS où est stocké le corpus
#     output_dir = Répértoire HDFS où seront stockés les résultats
#     conf       = Options de configuration Hadoop (optionnel)
#
# Ce script va créer trois répertoires dans output_dir (tfidf1,tfidf2 et tfidf3)
# qui contiendront les sorties de chaque job
#
# Exemples:
#    ./run_tfidf.sh ~/input ~/output "-conf=myconf.xml"
#    ./run_tfidf.sh ~/input ~/output "-Dmapred.reduce.tasks=2"
#
##############################################################################


# Make sure we have all the arguments
if [ $# -ne 2 ] && [ $# -ne 3 ]; then
   printf "./run_tfidf.sh <input_dir> <output_dir> [conf]\n"
   printf "   Répertoire HDFS où est stocké le corpus\n"
   printf "   output_dir = Répértoire HDFS où seront stockés les résultats\n"
   printf "   conf       = Options de configuration Hadoop\n"
   exit -1
fi

# Récupération des paramètres
declare INPUT=$1;
declare OUTPUT=$2;
declare CONFIG=$3;

# Suppression des répertoires de sortie
${HADOOP_HOME}/bin/hadoop fs -rmr $OUTPUT/tfidf1 >& /dev/null
${HADOOP_HOME}/bin/hadoop fs -rmr $OUTPUT/tfidf2 >& /dev/null
${HADOOP_HOME}/bin/hadoop fs -rmr $OUTPUT/tfidf3 >& /dev/null

# Execution des jobs
printf "\nJob 1 en cours d'exécution : Calcul de la fréquence des mots par document\n"
${HADOOP_HOME}/bin/hadoop jar tfidf.jar WordFrequenceInDoc $CONFIG $INPUT $OUTPUT/tfidf1

printf "\nJob 2 en cours d'exécution :  Comptage du nombre de mots par document\n"
${HADOOP_HOME}/bin/hadoop jar tfidf.jar WordCountsForDocs $CONFIG $OUTPUT/tfidf1 $OUTPUT/tfidf2

printf "\nJob 3 en cours d'exécution: Calcul du nombre de documents dans le corpus et des TF-IDF\n"
${HADOOP_HOME}/bin/hadoop jar tfidf.jar WordsInCorpusTFIDF $CONFIG $INPUT $OUTPUT/tfidf2 $OUTPUT/tfidf3
#${HADOOP_HOME}/bin/hadoop jar tfidf.jar tf-idf-3 $CONFIG $INPUT $OUTPUT/tfidf2 $OUTPUT/tfidf3

