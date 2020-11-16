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
# 							 "est_doublon_de"  :xxxx,            <-- 'master' si aucun des doublons est déjà topé 'master'
#                                                                       nom complet du doublon 'master' s'il existe déjà
#                                                                       pas renseigné si pas de doublons
# 							 "nb_of_same_files":nnnn             <-- pas rsgné si pas de doublons
# 							 "id de traitement":global_unique_id,<-- toujours rsgné
# 							 "date"            :d.now() }        <-- toujours rsgné
#                             "}
# 				- Remarque: "est_doublon_de" est valorisée à "master" (c'est le fichier 'maître', celui qui sera copié)
#         
#         - Une variable doublon_histo est ajoutée (niveau filename) reprend l'ensemble de ces éléments pour historisation :
#           elle est ajoutée pour master et le fichier trouvé identique
# 				{"doublon_histo": {=valeurs de "doublon"}    }
# 
# 

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
#pour inconcat:
import functools
import operator


# ### Fonctions

# In[ ]:





# ### traitement principal
# 

# In[14]:


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

var_group = {"$group": { "_id"              : {"my_filename_grouped": "$filename"},
                         "my_listeIds"      : {"$addToSet": "$_id"},
                         "my_doublon_master":  {"$addToSet": "$doublon.est_doublon_de"},
                         "my_count"         : {"$sum":1}}}

#my_doublon_master = [id du master, 'master'] si un 'master' a été trouvé précédemment.

#pour filtrer le résultat du regroupement sur les groupes avec plus d'un éléments :
var_match = {"$match": { "my_count": {"$gt": 1} } }
#et pour trier le résultat avec :
var_sort  = {"$sort" : { "my_count": -1 } }
#pipeline on choosed collection
cursor = mycollection.aggregate( [var_group, var_match, var_sort] )

#mise en liste
my_list_cursor = list(cursor)

#mise en dataframe
df = pd.DataFrame(my_list_cursor)

#extraction de la liste des filenames qui sont en doublon (pour filter en fusionnant si nécessaire -cf. etapé annulée)
df_temp = pd.DataFrame([x for x in df['_id']]) #cf. https://stackoverflow.com/questions/35711059/extract-dictionary-value-from-column-in-data-frame
my_list_of_filename_double = list(df_temp.my_filename_grouped)

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
# CETTE ETAPE EST ANNULEE (tout est retesté à chaque fois) - A ajouter selon le temps pris par le traitement en l'état
# Recherche des nouveau documents
# = ceux qui n'ont pas de champ 'doublon' (c'est un champ inséré uniquement par la fonction 'doublon')
# on récupère les filenames associés (où le critère de doublon complet - voir ajout de size à venir)
# et on va boucler sur cette liste de filenames pour la mise à jour des documents (les autres n'ont pas changés)
#---------------------------------------------------------------
#Récupération des 'nouveaux' documents
var_match3      = {"$match"  :{"doublon"      :{"$exists":False}}}
var_project3    = {"$project":{"my_filename"  :"$filename"}}
var_group3      = {"$group"  :{"_id":{"my_filename_grouped": "$my_filename"}}}
cursor3 = mycollection.aggregate([var_match3,var_project3, var_group3])

#mise en dataframe
my_list_cursor3 = list(cursor3)
df_temp = pd.DataFrame([x for x in df3['_id']]) #cf. https://stackoverflow.com/questions/35711059/extract-dictionary-value-from-column-in-data-frame
my_list_of_new_filenames = list(df_temp.my_filename_grouped)

my_list_of_new_filenames

#Et fusion des deux listes my_list_of_new_filenames et my_list_of_filename_double pour réduire la boucle
# non effectué dans cette version 

#---------------------------------------------------------------
#pour chaque filename (boucle sur la liste) (filtrée ou non)
#---------------------------------------------------------------
for i in range(df.shape[0]):

    #---------------------------------------------------------------
    #  Comptage du nombre d'éléments de même filename
    #---------------------------------------------------------------
    nb_same_files = int(df.my_count[i])   #conversion de numpy.int64 à int


    
    
    #---------------------------------------------------------------------------------------
    #  Mise à jour du niveau master
    #  = Sélection ('_id') d'un 'master' (le plus petit <-> le plus ancien) et mise à jour de "doublon"
    #  Remarque :  on ne gère pas la présence du master => tout est refait à chaque fois
    #-----------------------------------------------------------
    mylistsorted = sorted(df.my_listeIds[i])  #liste des documents en doublon sur le ième filename
    my_masterId  = mylistsorted[0]            #c'est le premier document de la liste (le plus ancien) qui est topé 'master'.
    strdnow      = datetime.now()             #date-heure du moment
    
    #Valorisation des variables à insérer pour master
    var_est_en_double      = True
    var_est_doublon_de     = "master"
    var_traitment_id       = global_unique_id_str
    var_traitment_date_now = strdnow
    #var_comment           = "no comment"
    var_nb_of_same_files   = nb_same_files
    var_list_of_ids_doubled= df.my_listeIds[i]    
    
    #Mise à jour dans db, niveau master
    result = mycollection.update_one( 
                       {"_id": my_masterId}, 
                       {'$set' :{ "doublon" : {
                                            "est_en_double"      : var_est_en_double,
                                            "est_doublon_de"     : var_est_doublon_de,
                                            "traitment_id"       : var_traitment_id,
                                            "traitment_date_now" : var_traitment_date_now,
                                            "nb_of_same_files"   : var_nb_of_same_files,
                                            "list_of_ids_doubled": var_list_of_ids_doubled
                                                 }
                                }
                        ,
                         '$push':{ "doublon_histo":{"$each": [{
                                            "h_est_en_double"      : var_est_en_double,
                                            "h_est_doublon_de"     : var_est_doublon_de,
                                            "h_traitment_id"       : var_traitment_id,
                                            "h_traitment_date_now" : var_traitment_date_now,
                                            "h_nb_of_same_files"   : var_nb_of_same_files,
                                            "h_list_of_ids_doubled": var_list_of_ids_doubled
                                                               }]
                                                  }
                                }
                        }
                                         )    

    #---------------------------------------------------------------------------------------
    #  Mise à jour de tous les autres "id_" = non encore renseignés
    #---------------------------------------------------------------------------------------
    #mylistsorted        = voir ci-dessus            #liste des documents en doublon sur le ième filename
    my_listeIds_to_update= mylistsorted[1:]          #tous les éléments sauf le 0 qui a été pris pour master 
    
    #Valorisation des variables à insérer pour master
    var_est_en_double      = True
    var_est_doublon_de     = my_masterId  #A VOIR COMMENT RECUPERER ORGNL_DIRNAME+FILENAME 
    var_traitment_id       = global_unique_id_str
    var_traitment_date_now = strdnow
    #var_comment           = "master"
    var_nb_of_same_files   = nb_same_files
    var_list_of_ids_doubled= df.my_listeIds[i]     
    
    #Mise à jour dans db, niveau master
    result = mycollection.update_many( 
                       {"_id"  :{"$in":my_listeIds_to_update}}, 
                       {'$set' :{ "doublon" : {
                                            "est_en_double"      : var_est_en_double,
                                            "est_doublon_de"     : var_est_doublon_de,
                                            "traitment_id"       : var_traitment_id,
                                            "traitment_date_now" : var_traitment_date_now,
                                            "nb_of_same_files"   : var_nb_of_same_files,
                                            "list_of_ids_doubled": var_list_of_ids_doubled
                                                }
                                }
                         ,
                         '$push':{ "doublon_histo":{"$each": [{
                                            "h_est_en_double"      : var_est_en_double,
                                            "h_est_doublon_de"     : var_est_doublon_de,
                                            "h_traitment_id"       : var_traitment_id,
                                            "h_traitment_date_now" : var_traitment_date_now,
                                            "h_nb_of_same_files"   : var_nb_of_same_files,
                                            "h_list_of_ids_doubled": var_list_of_ids_doubled
                                                                 }]
                                                   }
                                }
                        }
                                         )    
    

    #fin de boucle ---------------------------------------
    
cursor.close

#---------------------------------------------------------------
#recherche des documents "filename" qui ne sont pas en double
#---------------------------------------------------------------
var_group2   = {"$group": { "_id"               : {"my_filename_grouped": "$filename"},
                           "my_ids_sans_doublon": {"$addToSet": "$_id"},
                           "my_count"           : {"$sum":1}}}
#pour filtrer le résultat du regroupement sur les groupes avec plus d'un éléments :
var_match2   = {"$match": { "my_count": {"$eq": 1} } }
#et pour trier le résultat avec :
var_sort2    = {"$sort" : { "my_count": -1 } }
#pipeline on choosed collection
cursor2 = mycollection.aggregate( [var_group2, var_match2, var_sort2] )
#mise en liste
my_list_cursor2 = list(cursor2)
#mise en dataframe
df2 = pd.DataFrame(my_list_cursor2)
#colonne my_ids_sans_doublon mise en liste (de listes)
my_list = list(df2.my_ids_sans_doublon)          #les documents sans doublons et à mettre à jour 

#conversion de liste depuis une liste de listes - cf. https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-list-of-lists
#import functools
#import operator
my_listeIds_sans_doublon_to_update = functools.reduce(operator.iconcat, my_list, [])

#---------------------------------------------------------------------------------------
# fusion des deux listes my_list_of_new_filenames et my_listeIds_sans_doublon_to_update pour réduire le traitement
# non effectué dans cette version 
#---------------------------------------------------------------------------------------    


#---------------------------------------------------------------------------------------
#  Mise à jour en base
#---------------------------------------------------------------------------------------    


#Mise à jour dans db, niveau master
result = mycollection.update_many( 
                   {"_id"  :{"$in":my_listeIds_sans_doublon_to_update}}, 
                   {'$set' :{ "doublon" : {
                                        "est_en_double"      : False,
                                        "traitment_id"       : global_unique_id_str,
                                        "traitment_date_now" : strdnow,
                                            }
                            }
                     ,
                     '$push':{ "doublon_histo":{"$each": [{
                                        "h_est_en_double"      : False,
                                        "h_traitment_id"       : global_unique_id_str,
                                        "h_traitment_date_now" : strdnow,
                                                             }]
                                               }
                            }
                    }
                                     )   
    
cursor2.close    
    
print('done '+str(datetime.now()))


# ###  pour tests

# In[69]:


#cursor3 = mycollection.find({"doublon":{"$exists":False}}) #,{"_id":1,"doublon.est_en_double":1, "filename":1})
var_match3      = {"$match"  :{"doublon"      :{"$exists":False}}}
var_project3    = {"$project":{"my_filename"  :"$filename"}}
var_group3      = {"$group"  :{"_id":{"my_filename_grouped": "$my_filename"}}}

cursor3 = mycollection.aggregate([var_match3,var_project3, var_group3])

#from pprint import pprint
#for document in cursor3: 
#    pprint(document)

my_list_cursor3 = list(cursor3)


#mise en dataframe
df_temp = pd.DataFrame([x for x in df3['_id']]) #cf. https://stackoverflow.com/questions/35711059/extract-dictionary-value-from-column-in-data-frame
my_list_of_new_filenames = list(df_temp.my_filename_grouped)
my_list_of_new_filenames


# In[66]:


testdf = pd.DataFrame([x for x in df3['_id']]) #cf. https://stackoverflow.com/questions/35711059/extract-dictionary-value-from-column-in-data-frame
list(testdf.my_filename_grouped)


# In[13]:


var_group2   = {"$group": { "_id"               : {"my_filename_grouped": "$filename"},
                           "my_ids_sans_doublon": {"$addToSet": "$_id"},
                           "my_count"           : {"$sum":1}}}
#pour filtrer le résultat du regroupement sur les groupes avec plus d'un éléments :
var_match2   = {"$match": { "my_count": {"$eq": 1} } }
#et pour trier le résultat avec :
var_sort2    = {"$sort" : { "my_count": -1 } }
#projection pour avoir facilement les _id en colonne et les récupérer sous forme de liste :
#var_project2 = {"$project" : { "my_ids_sans_doublon":"$_id.my_filename_grouped"} }
#pipeline on choosed collection
cursor2 = mycollection.aggregate( [var_group2, var_match2, var_sort2] )
#mise en liste
my_list_cursor2 = list(cursor2)
#mise en dataframe
df2 = pd.DataFrame(my_list_cursor2)
#colonne my_ids_sans_doublon mise en liste (de listes)
my_list = list(df2.my_ids_sans_doublon)          #les documents sans doublons et à mettre à jour 

#conversion de liste depuis une liste de listes - cf. https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-list-of-lists
#import functools
#import operator
my_listeIds_sans_doublon_to_update = functools.reduce(operator.iconcat, my_list, [])
df2


# In[5]:


df2


# In[ ]:


var_group = {"$group": { "_id"     :     {"my_filename_grouped": "$filename"},
                         "my_listeIds":  {"$addToSet": "$_id"},
                         "my_count":     {"$sum":1}}}

#pour filtrer le résultat du regroupement sur les groupes avec plus d'un éléments :
var_match = {"$match": { "my_count": {"$eq": 1} } }
#et pour trier le résultat avec :
var_sort  = {"$sort" : { "my_count": -1 } }
#pipeline on choosed collection
cursor2 = mycollection.aggregate( [var_group, var_match, var_sort] )

#mise en liste
#mise en dataframe
df = pd.DataFrame(list(cursor2))

my_listeIds_sans_doublon_to_update = list(df.my_listeIds) 
df.my_listeIds.values.tolist()


# In[ ]:


#https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-list-of-lists
a=[['r'],['t']]
a=my_listeIds_sans_doublon_to_update
import functools
import operator
functools.reduce(operator.iconcat, a, [])


# In[70]:


#1er cursor
var_group = {"$group": { "_id"          : {"my_filename_grouped": "$filename"},
                         "my_listeIds"  : {"$addToSet": "$_id"},
                         "my_doublon"  :  {"$addToSet": "$doublon.est_doublon_de"},
                         "my_count"     : {"$sum":1}}}
#pour filtrer le résultat du regroupement sur les groupes avec plus d'un éléments :
var_match = {"$match": { "my_count": {"$gt": 1} } }
#et pour trier le résultat avec :
var_sort  = {"$sort" : { "my_count": -1 } }
#pipeline on choosed collection
cursor = mycollection.aggregate( [var_group, var_match, var_sort] )

#mise en liste
my_list_cursor = list(cursor)

#mise en dataframe
df = pd.DataFrame(my_list_cursor)

df


# In[72]:


df_temp = pd.DataFrame([x for x in df['_id']]) #cf. https://stackoverflow.com/questions/35711059/extract-dictionary-value-from-column-in-data-frame
my_list_of_filename_double = list(df_temp.my_filename_grouped)


# In[ ]:


my_list_cursor2


# In[ ]:


#1er cursor
var_group = {"$group": { "_id"     :     {"my_filename_grouped": "$filename"},
                         "my_listeIds":  {"$addToSet": "$_id"},
                         "my_size_files":{"$addToSet":"$stats.st_size"},
                         "my_count":     {"$sum":1}}}

#pour filtrer le résultat du regroupement sur les groupes avec plus d'un éléments :
var_match = {"$match": { "my_count": {"$gt": 1} } }
#et pour trier le résultat avec :
var_sort  = {"$sort" : { "my_count": -1 } }
#pipeline on choosed collection
cursor = mycollection.aggregate( [var_group, var_match, var_sort] )

#mise en liste
my_list_cursor = list(cursor)

#mise en dataframe
df = pd.DataFrame(my_list_cursor)
df


# In[ ]:


#les identifiants sont récupérés de la colonne _id et après transformations successives :
my_list_of_ids_sans_doublons = list(pd.DataFrame(list(df._id)).my_filename_grouped)

my_list_of_ids_sans_doublons


# In[ ]:


#2ème cursor
var_group = {"$group": { "_id"          : {"my_filename_grouped": "$filename"},
                         "my_count"     : {"$sum":1}}}

#pour filtrer le résultat du regroupement sur les groupes avec plus d'un éléments :
var_match = {"$match": { "my_count": {"$eq": 1} } }
#et pour trier le résultat avec :
var_sort  = {"$sort" : { "my_count": -1 } }
#projection pour avoir facilement les _id en colonne et les récupérer sous forme de liste :
var_project = {"$project" : { "my_filenames":"$_id.my_filename_grouped"} }
#pipeline on choosed collection
cursor = mycollection.aggregate( [var_group, var_match, var_sort, var_project] )
#mise en liste
my_list_cursor = list(cursor)
#mise en dataframe
df = pd.DataFrame(my_list_cursor)

df, list(df.my_filenames)


# In[ ]:


list(df.my_filenames)


# In[ ]:


#les identifiants sont récupérés de la colonne _id et après transformations successives :
my_list_of_ids_sans_doublons = list(pd.DataFrame(list(df._id)).my_filename_grouped)

my_list_of_ids_sans_doublons


# In[ ]:


df._id[0]['my_filename_grouped']


# In[ ]:


df.my_listeIds[i]


# In[ ]:


df.my_listeIds


# In[ ]:


my_masterId


# In[ ]:


i=0
df._id[i]['my_filename_grouped']


# In[ ]:


df.my_count[i]


# In[ ]:


i=0
mylistsorted = sorted(df.my_listeIds[i])
my_masterId = mylistsorted[-1]


# In[ ]:


listsort = list.sort(df.my_listeIds[i])
print(listsort)


# In[ ]:





# In[ ]:





# # TESTS

# In[ ]:


var_group = {"$group": { "_id"     : {"my_filename_grouped": "$filename"},"my_listeIds":{"$addToSet": "$_id"},"my_size_files":{"$addToSet":"$stats.st_size"},"my_count":{"$sum":1}}}
#pour filtrer le résultat du regroupement sur les groupes avec plus d'un éléments :
var_match = {"$match": { "my_count": {"$gt": 1} } }
#et pour trier le résultat avec :
var_sort  = {"$sort" : { "my_count": -1 } }
#pipeline on choosed collection
cursor = mycollection.aggregate( [var_group, var_match, var_sort] )
df=pd.DataFrame(list(cursor))
df


# In[ ]:


var_group = {"$group": { "_id"     : {"my_filename_grouped": "$filename"},"my_listeIds":{"$addToSet": "$_id"},"my_count":{"$sum":1}}}
#pour filtrer le résultat du regroupement sur les groupes avec plus d'un éléments :
var_match = {"$match": { "my_count": {"$gt": 1} } }
#et pour trier le résultat avec :
var_sort  = {"$sort" : { "my_count": -1 } }
#pipeline on choosed collection
#cursor = mycollection.aggregate( [var_group, var_match, var_sort] )


pipeline = str(var_group)+str(var_match)+str(var_sort)



# In[ ]:





# In[ ]:


df = spark.read.format("mongo").option("pipeline", pipeline).load()
df.show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




