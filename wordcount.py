#On importe SparkConf et SparkContext dans l'environnement 
from pyspark import SparkConf,SparkContext

#Instantiation du client Spark sous Python
if __name__ == "__main__":
    conf = SparkConf().setAppName('word_count').setMaster("local")
    sc = SparkContext(conf=conf)
#Lecture du fichier txt
    text=sc.textFile("sample.txt")
#On transforme le texte en un vecteur de mots, un mot étant deux élements séparés par un blanc
    mots=text.flatMap(lambda line: line.split(" "))
# On crée un  couple(mot,1) qui compte individuellement l'occurence du mot
    nombre=mots.map(lambda word: (word,1))
# On aggrège les unités des mots
    nombre_mots=nombre.reduceByKey(lambda count1, count2: count1 + count2)

#On affiche le résultat
nombre_mots.collect()

#on exporte le resultat
nombre_mots.coalesce(1).saveAsTextFile("Resultat")