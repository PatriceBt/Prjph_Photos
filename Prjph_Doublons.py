#!/usr/bin/env python
# coding: utf-8

# # Projet PHOTOS
# 
# # Gestion des doublons dans la base de données
# 
# Gestion des doublons : 
# 
# 		créer une fonction 'Gestion des doublons' qui détecte les doublons dans la base 
#         
#         - récupérer la liste des documents qui ont des doublons
#           -> liste A-avec-doublons
#          
#         - récupérer la liste des documents qui n'ont pas de doublons
#           -> liste B-sans-doublons
#        
# 		- pour chaque document ('filename') de la liste A-avec-doublons, (appelé ici 'master') :
#         
#         Rechercher la liste des documents en doublon (avec même 'filename')
#         si liste vide -> insérer 'doublon' de type 'est_en_double=False'
#         si non vide -> recherche d'un document ('filename') en doublon topé 'master' dans la liste (cas de nouveau document ou de 'première fois')
#                         Si pas trouvé : insérer 'doublon' type 'est_en_double=True/Master' pour le master,
#                                         et pour les fichiers trouvés : 
#                                                mettre à jour 'doublon' type : 'est_en_double=True/path+filename du master'
#                         Si trouvé     : insérer 'doublon' type 'est_en_double=True/path+filename du doublon master trouvé,
#                                         et pour le 'doublon master' trouvé : mettre à jour nb_of_same_files
#         
#         
#         - pour chaque document ('filename') de la liste B-sans-doublons :
#             
#             insérer 'doulon'type 'est_en_double=False'
#             
#      ------------------------------------------------------------------------
#         
#         
#         A REVOIR  pour ajouter le cas d'un doublon master déjà présent (cf. algo ci-dessus) :
#         
# 			- une variable "doublon" est ajoutée (niveau filename) :
#        
# 				{"doublon": {"est_en_double"   :True/False,      <-- toujours rsgné : True si d. trouvés, False sinon
# 							 "est_doublon_de"  :xxxx,            <-- 'master' si des doublons 'non master' sont trouvés
#                                                                      nom complet du doublon 'master' si trouvé
# 							 "nb_of_same_files":nnnn             <-- pas rsgné si pas de doublons
# 							 "id de traitement":global_unique_id,<-- toujours rsgné
# 							 "date"            :d.now() }        <-- toujours rsgné
#                             "}
# 				- Remarque: "est_doublon_de" est valorisée à "master" (c'est le fichier 'maître', celui qui sera copié)
#         
# 		- pour chaque fichier trouvé identique, 
# 				{"doublon": {"est_en_double"   :True             
# 							 "est_doublon_de"  :xxxx,            <-- nom complet (path+filename) du fichier master
# 							 "id de traitement":global_unique_id,<-- toujours rsgné
# 							 "date"            :d.now() }        <-- toujours rsgné
#                             "}
#                 - Remarque: nb_of_same_files n'est pas valorisée
#         - Une variable doublon_histo est ajoutée (niveau filename) reprend l'ensemble de ces éléments pour historisation :
#           elle est ajoutée pour master et le fichier trouvé identique
# 				{"doublon_histo": {=valeurs de "doublon"}    }
# 
#     my_doublon_histo = [{ A PASSER EN VARIABLE - car variable
#                 "doublon_h_est_en_double"    : full_name,
#                 "doublon_h_est_doublon_de    : "master"
#                 "doublon_h_nb_of_same_files" : x,
#                 "doublon_h_id"               : global_unique_id.replace('.','_'),
#                 "doublon_h_date"             : strdnow,
# 
#                         }]       
# 
#         
#     result = mycollection.update_one( 
#                    {"_id":f["_id"]   <--   nb : ds le cas de recherche des doublons -> filename=master.filename}, 
#                    {'$set' :{ DICO A PASSER EN VARIABLE - car variable - :
#                                 "doublon.est_doublon" : status,
#                                 "doublon.id"     : global_unique_id.replace('.','_'),
#                                 "doublon.date"   : strdnow,
#                                 "doublon.comment": comment,
#                                 "doublon." : source_full_name,
#                                 "doublon."   : dest_full_name      
#                             },
#                     '$push':{   "copy_events_fn_histo":{"$each":my_copy_e_histo}}
#                     }
#                                      )

# In[1]:


#
"""
Algo

#-----------------------------------------------------------------
#Comptage des "filename" en doublon dans la base avec récupération  
#---> cf. requête aggrégation
#---> le résultat est mis dans un dataframe
#-----------------------------------------------------------------

#-----------------------------------------------------------------
#Pour chaque "filename" de la liste, on va :
# compter le nombre d'éléments (pour mise à jour de "nb_of_same_files")
# chercher un "master"
# s'il existe -> mise à jour de tous les autres 'est_doublon_de'=master  (cas où un contrôle de doublon a déjà été fait)
# sinon -> création d'un master (mise à jour du premier )                (cas où ce filename n'a jamais été topé doublon)
#          mise à jour de tous les autres 'est_doublon_de'=master
#-----------------------------------------------------------------


"""


# ### Imports

# In[2]:


import pandas as pd
import pymongo
from pymongo import MongoClient
import datetime
from datetime import datetime


# ### Fonctions

# In[ ]:





# ### traitement principal
# 

# In[20]:


#-----------------
#paramètres
#-----------------
#------ COLLECTION -------------------*      #2<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
mycollection_name = "test_doublons"          #TEST
#mycollection_name = "images_videos"         #PREPROD

#----------------------
#Connexion à la base
#----------------------
client = pymongo.MongoClient('localhost',27017)
mydb = client["prjph_catalogue"]
mycollection = mydb[mycollection_name]

#---------------------------------------------------------------
#initialisations
#---------------------------------------------------------------
strtimestamp = str(datetime.timestamp(datetime.now()))
global_unique_id = strtimestamp
global_unique_id_str = global_unique_id.replace('.','_')

#---------------------------------------------------------------
#affichage début
#---------------------------------------------------------------

#---------------------------------------------------------------
#recherche des documents "filename" en double et création de la liste (dataframe)
#par agrégation : voir possibilité de compter le nombre 'est_doublon_de'<>absent
# => cela permettrait de filtrer ensuite sur ceux pour lesquels une mise à jour doit être effectuée (cf. refait/nouveau)
#---------------------------------------------------------------

var_group = {"$group": { "_id"     : {"my_filename_grouped": "$filename"},"my_listeIds":{"$addToSet": "$_id"},"my_size_files":{"$addToSet":"$stats.st_size"},"my_count":{"$sum":1}}}
#pour filtrer le résultat du regroupement sur les groupes avec plus d'un éléments :
var_match = {"$match": { "my_count": {"$gt": 1} } }
#et pour trier le résultat avec :
var_sort  = {"$sort" : { "my_count": -1 } }
#pipeline on choosed collection
cursor = mycollection.aggregate( [var_group, var_match, var_sort] )


my_list_cursor = list(cursor)

df = pd.DataFrame(my_list_cursor)


#---------------------------------------------------------------
# Filtre (voir aussi ci-dessus)
# NB : pour limiter le temps de traitement, on peut filtrer la liste
# la mise à jour (cas de refait), parmi ces "filename",
# on a besoin de ceux pour lesquels, dans la liste des _id en double :
# - il n'y a au moins un "_id" avec "doublon" non renseigné
# (s'ils sont tous renseignés, c'est que la recherche a déjà été faite)
#Remarque : ce filtre se fait implicitement dans le traitement ci-dessous
#---------------------------------------------------------------

#---------------------------------------------------------------
#pour chaque filename (boucle sur la liste) (filtrée ou non)
# A VOIR : boucle dans pd.DataFrame (df) ou dans cursor
#---------------------------------------------------------------
#for i in df.shape[0]:
i=0
df._id[i]['my_filename_grouped']

#---------------------------------------------------------------
#  Comptage du nombre d'éléments de même filename
#---------------------------------------------------------------
nb_same_files = int(df.my_count[i])   #conversion de numpy.int64 à int

#---------------------------------------------------------------
#  Recherche du 'master' et récupération de '_id' + mise à jour de "nb_of_same_files"
#---------------------------------------------------------------
#-----------------------------------------------------------
#version actuelle :  on ne gère pas la présence du master
# => tout est refait à chaque fois
#-----------------------------------------------------------



#---------------------------------------------------------------
#  S'il n'existe pas, sélection ('_id') d'un 'master' (le plus petit <-> le plus ancien) et mise à jour de "doublon"
#---------------------------------------------------------------
#Version actuelle  : Dans la liste des _id, choix du plus grand :
mylistsorted = sorted(df.my_listeIds[i])
my_masterId = mylistsorted[0]
strdnow = datetime.now()

#Valorisation des variables à insérer
    
my_doublon = { "est_en_double"      : True,
                "est_doublon_de"     : "master",
                "traitment_id"       : global_unique_id_str,
                "traitment_date_now" : strdnow,
                "comment"            : "master",
                "nb_of_same_files"   : nb_same_files

              }

my_doublon_histo = [{ "doublon_h_est_en_double"    : True,
                      "doublon_h_est_doublon_de"   : "master",
                      "doublon_h_nb_of_same_files" : nb_same_files,
                      "doublon_h_id"               : global_unique_id_str,
                      "doublon_h_date"             : strdnow,
                   }]   
  

#Mise à jour dans db
result = mycollection.update_one( 
                   {"_id":my_masterId}, 
                   {'$set' :{ "doublon" : my_doublon } ,
                    '$push':{ "doublon_histo":{"$each":my_doublon_histo}}
                    }
                                     )

#---------------------------------------------------------------------------------------
#  Mise à jour de tous les autres "id_" = non encore renseignés "est_doublon_de"=master
#---------------------------------------------------------------------------------------


# In[15]:


df


# In[5]:


my_masterId


# In[6]:


i=0
df._id[i]['my_filename_grouped']


# In[7]:


df.my_count[i]


# In[8]:


i=0
mylistsorted = sorted(df.my_listeIds[i])
my_masterId = mylistsorted[-1]


# In[9]:


listsort = list.sort(df.my_listeIds[i])
print(listsort)


# In[ ]:





# In[ ]:





# # TESTS

# In[10]:


var_group = {"$group": { "_id"     : {"my_filename_grouped": "$filename"},"my_listeIds":{"$addToSet": "$_id"},"my_size_files":{"$addToSet":"$stats.st_size"},"my_count":{"$sum":1}}}
#pour filtrer le résultat du regroupement sur les groupes avec plus d'un éléments :
var_match = {"$match": { "my_count": {"$gt": 1} } }
#et pour trier le résultat avec :
var_sort  = {"$sort" : { "my_count": -1 } }
#pipeline on choosed collection
cursor = mycollection.aggregate( [var_group, var_match, var_sort] )
df=pd.DataFrame(list(cursor))
df


# In[11]:


var_group = {"$group": { "_id"     : {"my_filename_grouped": "$filename"},"my_listeIds":{"$addToSet": "$_id"},"my_count":{"$sum":1}}}
#pour filtrer le résultat du regroupement sur les groupes avec plus d'un éléments :
var_match = {"$match": { "my_count": {"$gt": 1} } }
#et pour trier le résultat avec :
var_sort  = {"$sort" : { "my_count": -1 } }
#pipeline on choosed collection
#cursor = mycollection.aggregate( [var_group, var_match, var_sort] )


pipeline = str(var_group)+str(var_match)+str(var_sort)



# In[ ]:





# In[12]:


df = spark.read.format("mongo").option("pipeline", pipeline).load()
df.show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




