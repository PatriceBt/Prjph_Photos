#!/usr/bin/env python
# coding: utf-8

# # Projet Photos - Création des statistiques

# Récupération des données de la base mongodb
# 
# - Mise sous dataframe
# 
# - Statistiques
# 
# - Visualisations
# 

# ## Avancement
# 
# ### Réalisé
# 
# - affichage données début
# 
# - récupération des données mongodb dans un dataframe (par découpage possible)
# 
# - Calcul et présentation des statistiques 
# 
#     - Par nombre de fichiers + et par volume (cumul des tailles) par : 
#     
#         .année
#         
#         .mois
#         
#         .jour
#         
#         .catégorie de fichier (image, vidéo)
#         
#         .type de fichier
#         
#         .répertoire initial
# 
# 
# ### A venir
# 
#     Ajout de la prise en compte des doublons dans le calcul des statistiques.     
#         
#     NB : il faut pouvoir croiser les résultats
#     -> utiliser Bokeh ? pour les graphes ?
#     
#     Gérer les dates non renseignées (il y en a)
#     
#         
#         
# ### A corriger
# 
#     . Graphique par dossier : indiquer uniquement les dossiers 'volumineux' (à définir)
#         
#     . Date : 
#         - récupérer la donnée exif 36867 pour avoir la date de prise de vue (notamment des jpg : elle peut
#     être différente de st_mtime si le fichier a été modifié sur l'iphone par exemple)
#         
#         - pour les videos (.MOV), date st_mtime ok (pas d'autre)
#    
#         - pour les .PNG, st_mtime ok (pas d'autres) - rmq : correspondent à des screenshot sur iphone
#         
#         
#     

# # Imports

# In[1]:


import os
import datetime as dt
from datetime import datetime
import pandas as pd

#Pour mongodb
import pymongo
from pymongo import MongoClient
from pandas import json_normalize

from matplotlib import pyplot as plt

import time




# # Fonctions

# ### A mettre en module ?

# In[2]:


#Fonction de calcul du nombre et de la taille totale des fichiers catalgués dans la base

# ! CETTE FONCTION EST UNE COPIE DE CELLE PRESENTE DANS CATALOGUE -

def fnph_clc_nb_st_size_tot(parm_collection):
    """
    Fonction de calcul du nombre et de la taille totale des fichiers catalgués dans la base
    retourne une liste de deux éléments : taille totale, nombre de fichiers
    """
    #Requête aggrégation
    totalSize = parm_collection.aggregate(
       [
         {
           "$group":
             {
               "_id"        : "",
               "totalSize"  : { "$sum": "$stats.st_size" },
               "count"      : { "$sum": 1 }
             }
         }
       ]
    )

    
    reslist=list(totalSize)[0]
    result=[reslist['totalSize'], reslist['count']]
    return(result)


# ### Nouvelles fonctions

# In[3]:


#import pandas as pd
#from pymongo import MongoClient

def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)


    return conn[db]


# In[4]:


def fnph_read_mongodb(
           parm_db_name, 
           parm_collection, 
           parm_query_match     ={},
           parm_query_projection={},
           parm_host            ='localhost', 
           parm_port            =27017, 
           parm_username        =None, 
           parm_password        =None,
           parm_chunksize       =100, 
           parm_no_id           =True,
           parm_json_normalized =True):
    """ Read from Mongo and Store into DataFrame 
    découpage en chunk - chunksize à définir, pour l'instant fixée en début de traitement
    En sortie : les données récupérées (tous les chunks concaténés) 
                en format json ou en DataFrame (selon parm_json_normalized)
    """

    #df_result                 = pd.DataFrame()  non ! car test sur local()
    #df_result_aux             = pd.DataFrame()
    
    # Connect to MongoDB
    db = _connect_mongo(host='localhost', port=27017, username=None, password=None, db=parm_db_name)
    client = MongoClient(host=parm_host, port=parm_port)
    db_aux = client[parm_db_name]

    # Variables to create the chunks
    nb_enr_tot = db_aux[parm_collection].find(parm_query_match, parm_query_projection).count()
    skips_variable = range(0, nb_enr_tot, int(parm_chunksize))
    
    if len(skips_variable)<=1: #en fait il ne peut être qu'égal à 1 au minimum ou à 0 si la base est vide
        skips_variable = [0,nb_enr_tot]

    # Iteration to create the dataframe in chunks
    for i in range(1,len(skips_variable)):
        
        # Expand the cursor and construct the DataFrame                                   # <--Requête2
        # création du curseur (NB!!! le cursor est détruit dès la première utilisation - quelle qu'elle soit)
        cursor_result = db_aux[parm_collection].find(parm_query_match, parm_query_projection)[skips_variable[i-1]:skips_variable[i]]
        if parm_json_normalized:
            df_result_aux = json_normalize(cursor_result)
        else:
            df_result_aux = pd.DataFrame(list(cursor_result))

        if parm_no_id: #selon que l'on souhaite récupérer l'_id ou non (paramètrage de l'appel à la fonction)
            del df_result_aux['_id']

        # Concatenate the chunks into a unique df
        if 'df_result' not in locals():
            df_result = df_result_aux
        else:
            df_result = pd.concat([df_result, df_result_aux]            , ignore_index=True)
          
    # Récupération des derniers enregistrements (après le dernier chunk traité)    
    enr_deb = (len(skips_variable)-1)*parm_chunksize
    if nb_enr_tot - enr_deb > 0 : #Si chunksize est > au nb total d'enregs, il n'y a plus d'enregs à traiter et 
                                  #      enr_deb > nb_enr_tot, il ne reste donc plus d'enregistrements à traiter.
        enr_fin = nb_enr_tot
        print("debug-->(enr_deb,enr_fin):",(enr_deb,enr_fin))

        #                                                                                     # <--Requête3
        cursor_result = db_aux[parm_collection].find(parm_query_match, parm_query_projection)[enr_deb:enr_fin]
        df_result_aux =pd.DataFrame(list(cursor_result)) #<--Requête3

        if 'df_result' not in locals():
            df_result             = df_result_aux
        else:
            df_result             = pd.concat([df_result, df_result_aux], ignore_index=True)
    else:
        print("(warning) chunksize supérieur au nombre d''enregistrements récupérés.")

    print('debug-->nb_enr_tot=', nb_enr_tot)
    return df_result


# In[5]:


get_ipython().run_cell_magic('time', '', '#Test fnph_read_mongodb()\n\n\nimport datetime\nfrom datetime import datetime\n\n#client = pymongo.MongoClient(\'localhost\',27017)\n#mydb = client["prjph_catalogue"]\ntest1mydb_name = "prjph_catalogue"\ntest1mycollection_name = "test_documents"          #TEST\n#mycollection_name = "images_videos"           #PREPROD\n#------ ---------- -------------------*\n#mycollection = mydb[mycollection_name]\n\n\ntest1mysize  = 1000\n\ntest1myquery_match      =  {"extfile":".mov"} #le null ne convient pas à Python\n#test1myquery_match      =  {} #le null ne convient pas à Python\n#   projection-->\ntest1myquery_projection =  {    "_id"              :1,\n                                "filename"         :1, \n                                "exif.36865"       :1, \n                                "exif.36866"       :1, \n                                "stats.st_mtime"   :1, \n                                "stats.st_size"    :1, \n                                "orgnl_dirname"    :1, \n                                "doctype"          :1, \n                                "extfile"          :1, \n                                "vid_ppty.duration":1, \n                                "exif.271"         :1, \n                                "exif.272"         :1  \n                           }\n#testmyquery_match={}\n#testmyquery_projection={}\n\n#Traitement\nmydate=datetime.now()\nmychunksize=test1mysize\nstart=datetime.now()\ndf_test1    = pd.DataFrame()\ndf_test1    = fnph_read_mongodb(test1mydb_name, \n                             test1mycollection_name, \n                             parm_query_match       = test1myquery_match, \n                             parm_query_projection  = test1myquery_projection,\n                             parm_chunksize=test1mysize, \n                             parm_no_id=False,\n                             parm_json_normalized = True)\nend  = datetime.now()\ntimelaps=end-start\nprint(\'durée :\', timelaps)\nprint(\'df_test1.shape =\', df_test1.shape)')


# ## Fonction graphiques

# In[6]:


df_stot =  pd.DataFrame(df_stats.groupby('doctype').agg({'filename':['count'], 'stats.st_size':['sum']}).sum(), 
              columns=['sums'])
df_stot
df_stot.iloc[1:2]
#df_stot.iloc[:1]


# In[7]:


def fn_graph_quantite_volume_global():
    
    from matplotlib.ticker import FormatStrFormatter, FuncFormatter
    
    mytitle1 = 'Quantité de fichiers'
    mytitle2 = 'Volume (Go) des fichiers'

    df_stot =  pd.DataFrame(df_stats.groupby('doctype').agg({'filename':['count'], 'stats.st_size':['sum']}).sum(), 
              columns=['sums'])
    
    #fig = df_stot.plot(kind='bar',subplots=True,sharex=False,sharey=False,title=mytitle, figsize=(6,6))
    fig1 = df_stot.iloc[:1,0].plot(kind='bar',subplots=False,sharex=False,sharey=False,title=mytitle1, figsize=(3,3))
    plt.show()
    fig2 = df_stot.iloc[1:2].plot(kind='bar',subplots=False,sharex=False,sharey=False,title=mytitle2, figsize=(3,3))
    
    #formatage des graduations
    ax=plt.gca()
    ax.yaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000000))) #pour exprimer en Go
    
    plt.show()
    


# In[8]:


fn_graph_quantite_volume_global()


# In[9]:


#Fonction graphiques
def fn_graph_quantite_volume_par_an():
    from matplotlib.ticker import FormatStrFormatter, FuncFormatter
    
    
    mytitle = 'Par année - quantité et volume (Go) des fichiers'
    #df_sq_year = df_stats.groupby(df_stats['date_creation_y']).size()
    df_sq_year = df_stats.groupby('date_creation_y').agg({'filename':['count'], 'stats.st_size':['sum']})
    #df_sq_year.plot.bar()
    
    fig, ax = df_sq_year.plot(kind='bar',subplots=True,sharex=False,sharey=False,title=mytitle, figsize=(10,6))
    
    #Graduations
    #ax.yaxis.set_major_formatter(FormatStrFormatter('%d'))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000000))) #pour exprimer en Go
    #ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))  #pour un séparteur de milliers
    
    #tight (marges) pour l'espace entre les subplots
    plt.tight_layout(pad=1, w_pad=1, h_pad=1)
    
    #titres des sublots (sous-parcelles)
    #ax[0,1].set_title("t1") ???
    #ax[0,2].set_title("t2") ???
    
    plt.legend(loc = 'upper left')
    
    plt.show()
    


# In[10]:


fn_graph_quantite_volume_par_an()


# In[11]:


def fn_graph_quantite_volume_par_mois():
    
    from matplotlib.ticker import FormatStrFormatter, FuncFormatter
    import matplotlib.ticker as ticker
    
    mytitle = 'Par mois - quantité et volume (Go) des fichiers'
    
    df_sq_month = df_stats.groupby(['date_creation_y','date_creation_m']).agg({'filename':['count'], 'stats.st_size':['sum']})
    

    fig, ax = df_sq_month.plot(kind='bar',subplots=True,sharex=True,sharey=False,title=mytitle, figsize=(10,6))
    
    #Graduations
    ax.xaxis.set_major_locator(ticker.MultipleLocator(12)) #les ticks majeurs tous les 10 pour l'axe y (toutes les 10 (u)
    ax.xaxis.set_minor_locator(ticker.MultipleLocator(1))  #les ticks mineurs tous les 2 pour l'axe y (toutes les 2 unités)
    
    
    #ax.yaxis.set_major_formatter(FormatStrFormatter('%d'))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000000))) #pour exprimer en Go
    #ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))  #pour un séparteur de milliers
    
    #tight (marges) pour l'espace entre les subplots
    plt.tight_layout(pad=2, w_pad=1, h_pad=1)
    
    #titres des sublots (sous-parcelles)
    #ax[0,1].set_title("t1") ???
    #ax[0,2].set_title("t2") ???
    
    plt.legend(loc = 'upper left')
    
    plt.show()


# In[12]:


fn_graph_quantite_volume_par_mois()


# In[13]:


def fn_graph_quantite_volume_par_type_de_fichier():
    
    from matplotlib.ticker import FormatStrFormatter, FuncFormatter
    
    mytitle = 'Par type de fichier (catégorie) - quantité et volume (Go) des fichiers'

    df_sc = df_stats.groupby('doctype').agg({'filename':['count'], 'stats.st_size':['sum']})
    
    fig, ax = df_sc.plot(kind='bar',subplots=True,sharex=True,sharey=False,title=mytitle, figsize=(6,6), legend=False)

    #ax.yaxis.set_major_formatter(FormatStrFormatter('%d'))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000000))) #pour exprimer en Go
    #ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))  #pour un séparteur de milliers
    
    #tight (marges) pour l'espace entre les subplots
    plt.tight_layout(pad=2, w_pad=1, h_pad=1)
    
    plt.legend(loc = 'lower left')
    
    plt.show()


# In[14]:


fn_graph_quantite_volume_par_type_de_fichier()


# In[15]:


def fn_graph_quantite_volume_par_extension():
    
    from matplotlib.ticker import FuncFormatter

    mytitle = 'Par extension de fichier (type) - quantité et volume (Go) des fichiers'
    df_st = df_stats.groupby('extfile').agg({'filename':['count'], 'stats.st_size':['sum']})
    
    fig, ax = df_st.plot(kind='bar',subplots=True,sharex=True,sharey=False,title=mytitle,figsize=(15,5))
    #graduation
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000000))) #pour exprimer en Go

    plt.show()


# In[243]:


fn_graph_quantite_volume_par_extension()


# In[16]:


def fn_graph_quantite_volume_par_dossier():
    
    from matplotlib.ticker import FuncFormatter

    mytitle = 'Par dossier [à modifier pour avoir le dossier initial de recherche] - quantité et volume (Go) par répertoire'
    plt.figure(figsize=(15,5))
    df_st = df_stats.groupby('orgnl_dirname_str').agg({'filename':['count'], 'stats.st_size':['sum']})
    
    fig, ax = df_st.plot(kind='bar',subplots=True,sharex=True,sharey=False,title=mytitle,figsize=(15,5))
    
    #graduation
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000000))) #pour exprimer en Go    
    plt.show()


# In[258]:


fn_graph_quantite_volume_par_dossier()


# In[17]:


# Croisements
    
def fn_graph_quantite_volume_par_an_et_catégorie():
    
    mytitle = 'Par année et catégorie - quantité et volume des fichiers'
    df_syc = df_stats.groupby(['date_creation_y','doctype']).agg({'filename':['count'], 'stats.st_size':['sum']})
    df_syc.plot(kind='bar',subplots=True,sharex=True,sharey=False,title=mytitle,figsize=(15,5))
    plt.show()


# In[260]:


fn_graph_quantite_volume_par_an_et_catégorie()


# In[117]:


#dataframe des doublons

def fnph_df_doublons(parm_collection):
    """
    Fonction de calcul de statistiques sur les doublons et graphiques
    """
    #-------------------
    #Requête aggrégation
    var_group = {"$group": { "_id"    : {"filename_grpd": "$filename", "extfile_grpd":"$extfile", "doctype_grpd":"$doctype"},
                       "listeIds_size": {"$addToSet": {"id":"$_id","size":"$stats.st_size"}},
                       "list_sizes"   : {"$addToSet": "$stats.st_size"},                   
                       "my_count"     : {"$sum":1}                               
                     } }          
    cursor_doublons = parm_collection.aggregate( [ var_group ], allowDiskUse=True  )
    #-------------------
    #mise en liste
    my_list_cursor = list(cursor_doublons)
    #mise en dataframe
    df_result = pd.DataFrame(my_list_cursor)

    #----------------------------------------------------------------------------
    #Suppression de la ligne '_id'={} qui correspond au document "eventgnls"
    #----------------------------------------------------------------------------
    df_result = df.drop(df_result[df_result['_id']=={}].index)
    
    #----------------------------------------------------------------------------
    #Calcul du nombre de sizes distinctes (y.c. non renseignées)
    #----------------------------------------------------------------------------
    print('--- Calcul du nombre de sizes distinctes à refaire --- ')
    df_result['nb_distinct_sizes']=df_result.apply(lambda x: len(x['list_sizes']), axis=1)
    #A REFAIRE !!!
    #A REFAIRE !!! : list_sizes est déduit de la liste des sizes récupérées. Hors, si size n'est pas renseignée, elle
    #A REFAIRE !!!   n'apparait pas dans la liste. Il faut donc reconstituer le nombre à partir des sizes dans liste_ids_size
    #A REFAIRE !!!
    #--------------------------------
    # dégroupement de _id en trois colonnes (pour avoir _id, ext, type)
    #--------------------------------
    df_result['filename_grpd']=df_result.apply(lambda x: x['_id']['filename_grpd'], axis=1)
    df_result['extfile_grpd'] =df_result.apply(lambda x: x['_id']['extfile_grpd'], axis=1)
    df_result['doctype_grpd'] =df_result.apply(lambda x: x['_id']['doctype_grpd'], axis=1)

    
    return(df_result)


# In[ ]:


#REMARQUE : A VOIR !!!!!
# https://stackoverflow.com/questions/7571635/fastest-way-to-check-if-a-value-exists-in-a-list


# In[118]:


#def fnph_graph_doublons(parm_collection):
# EN CONSTRUTION ......................
# EN CONSTRUTION ......................
# EN CONSTRUTION ......................

#pour test
client = pymongo.MongoClient('localhost',27017)
mydb = client["prjph_catalogue"]
mydb_name = "prjph_catalogue"
mycollection_name = "test_documents"          #TEST
#mycollection_name = "images_videos"           #PREPROD
mycollection = mydb[mycollection_name]

#appel dataframe des doublons
df_test = fnph_df_doublons(mycollection).copy()

#split ici car un peu long à refaire
print('done')


# In[125]:


pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
df_test[df_test.my_count>1]


# In[75]:


pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)


# In[27]:


#test pour récupérer la liste des sizes

df=df_test.copy()
#pour chaque ligne, on va lire la liste ids, size et récupérer les sizes pour les stocker dans la colone list_sizes
for i in range(df.shape[0]):
    
    
    #lecture de la liste ids_size et mise en liste
    mylist=df.listeIds_size[i]

    #récupération des sizes sous forme de liste
    #try:
    #    df['list_sizes'].iloc[i] = [mylist[j]['size'] for j in range(len(mylist))]
    #except:
    #    print(i, 'erreur')
        
    #récupération des sizes sous forme de liste - méthode 2
    if 'size' in mylist[j]: #teste l'existence de la clé 'size' dans le dico
        df['list_sizes'].iloc[i] = [mylist[j]['size'] for j in range(len(mylist))]
    else:
        print(i, 'erreur')        
df


# In[29]:


#split ici car un peu long à refaire
print("il y a 'AU MOINS'", df_test.shape[0], "filename uniques")
print("Peut-être plus si l'on compte les 'faux doublons'.")
print("(nb total de documents ", df_test.my_count.sum(),"- à vérifier avec nb_tot...)\n")
#docs avec doublons
mask=df_test.my_count!=1
df_avec_doublons=df_test[mask]
#df_avec_doublons.describe()

print('!!! Calcul du nombre de sizes distinctes à refaire - df_test.shape (filenames uniques)=', df_test.shape)

print("\ndont : filenames avec doublons (ou 'faux doublons'):",df_avec_doublons.shape)

df_avec_doublons.head()


# In[23]:



print(df_avec_doublons.shape[0], "filenames avec doublons")
print("Le cumul de my_count donne", df_avec_doublons.my_count.sum(), "documents concernés")


# In[24]:


#filenames en doublons mais avec des tailles différentes.
mask1=df_avec_doublons.nb_distinct_sizes!=1
print(df_avec_doublons[mask1].shape)

print("il y a donc au moins 12261 faux doublons ")


# In[25]:


#filenames en doublons : 2 fichiers exactements de taille différentes.
mask4a=df_avec_doublons.nb_distinct_sizes==df_avec_doublons.my_count
#mask4b=df_avec_doublons.my_count!=2
mask4=mask4a #& mask4b
print(df_avec_doublons[mask4].shape)

print("Ces 2014 éléments parmi les 12261 sont des faux doublons")
print("il reste donc au moins", 12261-2014, "faux doublons")


# In[26]:


print("12261/67492=", 100*12261/67492, "=> c'est le pourcentage de 'doublons' qui sont en fait des faux doublons")
print("=> voir la nécessité d'inclure la taille dans la recherche de doublons")


# # Traitement principal

# In[261]:


get_ipython().run_cell_magic('time', '', '#Programme principal\n\n#----------------------------------------------------------------------------------------------------------\n#description :\n\n# Préparation\n# - Accès à la bd mongdodb\n# - Récupération des données dans un df (par découpage pour gérer vitesse vs utilisation mémoire)\n\n# Obtention des statistiques\n\n# Présentation des statistiques\n#----------------------------------------------------------------------------------------------------------\n\n\n#Paramètres utilisateurs***********************************************************************************\n#Libellé traitement 1<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\nmontitre_traitement="Stats - Premier traitement"\n#Répertoire de sauvegarde de l\'historique du traitement\nparm_df_log_rep = r\'C:\\Users\\LENOVO\\Documents\\Projets\\Prj_photos\\Prjph_log\'\n#Paramètres de connexion\n#------ Base  -------------------*\n#connection au serveur mongodb 27017, base test\nclient = pymongo.MongoClient(\'localhost\',27017)\nmydb = client["prjph_catalogue"]\nmydb_name = "prjph_catalogue"\nmychunksize = 5000                #<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n#Collection                        2<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n#mycollection_name = "test_documents"          #TEST\nmycollection_name = "images_videos"           #PREPROD\n#------ ---------- -------------------*\nmycollection = mydb[mycollection_name]\n#Paramètres utilisateurs***********************************************************************************\n\n#----------------\n#DEBUT TRAITEMENT\n#----------------\n\n#----------------\n#initialisations\n#----------------\n\n#Compteurs\n\n#init du df historique des répertoires traités\ndf_log=pd.DataFrame(columns=[\'unique_id\',\'time\',\'rep_explored\',\'nb_sous_rep\', \'nb_of_files\', \'nb_of_files_cumul\', \'comment\'])          #df contenant la liste des répertoires traités\n\n#init du df historique des répertoires traités\ndf_stats=pd.DataFrame()          #NB : les colonnes du df seront définies par l\'appel à fnph_read_mongodb\n\n#global_unique_id : Variable de valeur unique pour rsgner unique_id lors des différents appels\nstrtimestamp = str(datetime.timestamp(datetime.now()))\nglobal_unique_id = strtimestamp\nprint(\'\\n           global_unique_id:\', global_unique_id,\'\\n\')\n\n#------------\n# TRAITEMENT\n#------------\n\n#==========*Entête*============\nstart_time=datetime.now()\nprint(\'start..................... : \', start_time, \'\\n\')\n\nprint("\\n**** Collection            :", mycollection_name, "****\\n")\nprint("****")\nprint("****")\nprint("****")\nprint("****")\nprint("****")\n\n#==========*Récupération des données*============\n\n#----Volume de la base (nombre de documents et cumul de la taille): \nvol_base = fnph_clc_nb_st_size_tot(mycollection)\n\n#----Stockage en dataframe des données utiles\n\n#"filename"         :1, #nom du fichier\n#"exif.36865"       :1, #exif date de création \n#"exif.36866"       :1, #exif date de mise en data par l\'appareil (~date de création?)\n#"stats.st_mtime"   :1, #date de dernière modification (~date de création)\n#"stats.st_size"    :1, #taille en octets   \n#"orgnl_dirname"    :1, #nom du répertoire de stockage (au moment de la création du catalogue)\n#"doctype"          :1, #catégorie de fichier\n#"extfile"          :1, #extension du fichier (type)\n#"vid_ppty.duration":1, #durée (si vidéo)\n#"exif.271"         :1, #nom du fabriquant\n#"exif.272"         :1  #nom du modèle\n\n#myquery = { [ QueryMatch, QueryProject ] }\n\n#   match--> tous les documents (hors eventsgnls)\n#myquery_match      =  {"eventgnls":{"$eq":null} } }, #le null ne convient pas à Python\n#myquery_match      =  {"eventgnls":""} #le null ne convient pas à Python\n#myquery_match      =  {"extfile":".jpg"}\nmyquery_match      =  {}\n\n#   projection-->\nmyquery_projection =  { "_id"              :0,\n                        "filename"         :1, \n                        "stats.st_size"    :1, \n                        "stats.st_mtime"   :1, \n                        "exif.36865"       :1, \n                        "exif.36866"       :1, \n                        "extfile"          :1, \n                        "doctype"          :1, \n                        "orgnl_dirname"    :1, \n                        "vid_ppty.duration":1, \n                        "exif.271"         :1, \n                        "exif.272"         :1  \n                      }\n\ndeb=datetime.now()\n#print(\'debug-->avant read_mongodb :\', deb)\n\n#=======Requête de lecture de mongodb et récupération des données au format json normalized si souhaité================\ndf_stats = fnph_read_mongodb(mydb_name, \n                             mycollection_name, \n                             myquery_match, \n                             myquery_projection, \n                             parm_chunksize = mychunksize, \n                             parm_no_id = False,\n                             parm_json_normalized = True)\n#=======Requête de lecture de mongodb==================================================================================\n\n\n#--------------------------------------------------------\n# CALCUL ET REPRESENTATION GRAPHIQUE DES STATISTIQUES\n#--------------------------------------------------------\n\n#Préparation du df (df_stats) -------------\n#Ajout de colonnes\n\n#Conversion de st_mtime (timestamp) en date claire\ndf_stats[\'date_creation\'    ] = df_stats[\'stats.st_mtime\'].apply(lambda x: time.strftime(\'%Y%m%d\', time.localtime(x))  if pd.notnull(x) else \'\')\ndf_stats[\'date_creation_y\'  ] = df_stats[\'stats.st_mtime\'].apply(lambda x: time.strftime(\'%Y\'    , time.localtime(x))  if pd.notnull(x) else \'\')\ndf_stats[\'date_creation_m\'  ] = df_stats[\'stats.st_mtime\'].apply(lambda x: time.strftime(\'%m\'    , time.localtime(x))  if pd.notnull(x) else \'\')\ndf_stats[\'date_creation_d\'  ] = df_stats[\'stats.st_mtime\'].apply(lambda x: time.strftime(\'%d\'    , time.localtime(x))  if pd.notnull(x) else \'\')\ndf_stats[\'date_creation_hms\'] = df_stats[\'stats.st_mtime\'].apply(lambda x: time.strftime(\'%H%M%S\', time.localtime(x))  if pd.notnull(x) else \'\')\n\n#Conversion de orgnl_dirname (liste) en string (la liste ne contient qu\'un seul élément)\ndf_stats[\'orgnl_dirname_str\'] = df_stats[\'orgnl_dirname\'].apply(lambda x: x[0])\n\n\n#--------------------\n# GRAPHIQUES\n#--------------------\n\n# - Quantités et volumes\nfn_graph_quantite_volume_par_an()\n\nfn_graph_quantite_volume_par_mois()\n\nfn_graph_quantite_volume_par_type_de_fichier()\n\nfn_graph_quantite_volume_par_extension()\n\n#fn_graph_quantite_volume_par_dossier()\n\n# Croisements\nfn_graph_quantite_volume_par_an_et_catégorie()\n\n\n\n\n# --------------------\n# FIN DE TRAITEMENT\n# --------------------\nfin=datetime.now()\n#print(\'debug-->après read_mongodb :\', fin, "durée :", fin-deb)\n    \n#ajout de la ligne start dans df_log \n#rmq : nb_sous_rep et nb_of_files sont les documents à la racine du répertoire, pas le total\ndf_log=df_log.append({\'unique_id\'   :global_unique_id,\n                      \'time\'        :str(datetime.now()),\n                      \'rep_explored\':" <--- début de traitement - <"+montitre_traitement+">-->",\n                      \'comment\'     :"start "}, ignore_index=True)\n\n\n# ==============STATS DU TRAITEMENT===================\nmsg    = [\'\']*12\nend_time=datetime.now()\nmsg[0] = \'\\ndone----------------------------------------\'\nmsg[1] = \'df_stats.shape=\' + str(df_stats.shape)\nmsg[2] = \'\'\nmsg[3] = \'\'\nmsg[4] = \'\'\n#msg[5] = \'taille ttle des fichiers en base  : \' + str(var_tailletot)\n#msg[6] = \'nombre ttl de  documents en base  : \' + str(var_nbtot)\nmsg[7] = \'Durée du traitement               : \' + str(end_time - start_time)\nmsg[8] = \'\'\nmsg[9] = \'\'\nmsg[10] = \'done----------------------------------------\'\nfor i in range(len(msg)):\n    if msg[i]!=\'\':\n        print(msg[i])\n\n        \n#Stockage de la fin de traitement dans l\'historique df_log.\nfor i in range(len(msg)):\n    if msg[i]!="":\n        df_log=df_log.append({\'unique_id\'   : global_unique_id,\n                              \'time\'        : str(end_time),\n                              \'comment\'     : msg[i]}, ignore_index=True)\n\n#Sauvegarde du df_log - nom complété de la collection et nom du répertoire exploré\ndnow = datetime.now()\nstrtimestamp = str(datetime.timestamp(dnow)).replace(\'.\',\'_\')\nmypathlog=r\'C:\\Users\\LENOVO\\Documents\\Projets\\Prj_photos\\Prjph_log\'\n\ndf_log_filename= \'prjph_df_log_stats__\' +                                   \\\n                    global_unique_id.replace(\'.\',\'_\') + "__" +        \\\n                    mycollection_name + "__" +                        \\\n                 \'.csv\' #référencement avec global_unique_id utilisé pour référencer les documents dans la base.\ndf_log.to_csv(os.path.join(mypathlog,df_log_filename), sep=\'\\t\')\nprint(df_log_filename, \'saved into\', mypathlog)\n\nprint(\'\\nended..................... : \', end_time, \'\\n\')\n\n#Programme principal fin\n')


# # Tests

# In[ ]:


get_ipython().run_cell_magic('time', '', '# TEST DE REFORMATAGE PAR BOUCLE AVEC ILOC - TRES LONG !\n# ATTENTION : NE FONCTIONNE PAS SI LA VALEUR EST NaN or il y en a (...cf. gérer les dates non renseignées)\n\nimport time\nimport sys\nimport tqdm\n#df_stats[\'newdate\']=time.strft(\'%A, %Y-%m-%d %H:%M:%S\', time. localtime(df_stats[\'stats.st_mtime\'][3]))\ndf_stats[\'date_creation\']=""\ndf_stats[\'date_creation_y\']=""\ndf_stats[\'date_creation_m\']=""\ndf_stats[\'date_creation_d\']=""\ndf_stats[\'date_creation_hms\'] = ""\n\n\nmydate=time.strftime(\'%A, %Y-%m-%d %H:%M:%S\', time. localtime(df_stats[\'stats.st_mtime\'][3]))\n#mydate_year=time.strftime(\'%Y\', time. localtime(df_stats[\'stats.st_mtime\'][3]))\n#mydate_month=time.strftime(\'%m\', time. localtime(df_stats[\'stats.st_mtime\'][3]))\n#mydate_ymd=time.strftime(\'%y%m%d\', time. localtime(df_stats[\'stats.st_mtime\'][3]))\n\n#for i in tqdm.tqdm(range(df_stats.shape[0])):\n#for i in range(df_stats.shape[0]):\n    #df_stats[\'date_creation\']=time.strftime(\'%Y%m%d\', time.localtime(df_stats[\'stats.st_mtime\']))\n#    datetoconvert = time.localtime(df_stats[\'stats.st_mtime\'].iloc[i])\n#    df_stats[\'date_creation\'].iloc[i]  = time.strftime(\'%Y%m%d\', datetoconvert )\n#    df_stats[\'date_creation_y\'].iloc[i]= time.strftime(\'%y\'    , datetoconvert )\n#    df_stats[\'date_creation_m\'].iloc[i]= time.strftime(\'%m\'    , datetoconvert )\n#    df_stats[\'date_creation_d\'].iloc[i]= time.strftime(\'%d\'    , datetoconvert )\n#    \n#    msg=str(i) + "-----------" + str(df_stats[\'date_creation\'].iloc[i])\n#    sys.stdout.write("\\r"+msg)\n#    sys.stdout.flush()\n\ndf_stats[[\'date_creation\',\'date_creation_y\',\'date_creation_m\',\'date_creation_d\']]')


# In[ ]:


get_ipython().run_cell_magic('time', '', "#Conversion de st_mtime (timestamp) en date claire\ndf_stats['date_creation'    ] = df_stats['stats.st_mtime'].apply(lambda x: time.strftime('%Y%m%d', time.localtime(x))  if pd.notnull(x) else '')\ndf_stats['date_creation_y'  ] = df_stats['stats.st_mtime'].apply(lambda x: time.strftime('%Y'    , time.localtime(x))  if pd.notnull(x) else '')\ndf_stats['date_creation_m'  ] = df_stats['stats.st_mtime'].apply(lambda x: time.strftime('%m'    , time.localtime(x))  if pd.notnull(x) else '')\ndf_stats['date_creation_d'  ] = df_stats['stats.st_mtime'].apply(lambda x: time.strftime('%d'    , time.localtime(x))  if pd.notnull(x) else '')\ndf_stats['date_creation_hms'] = df_stats['stats.st_mtime'].apply(lambda x: time.strftime('%H%M%S', time.localtime(x))  if pd.notnull(x) else '')")


# In[ ]:


import numpy as np
mask=(df_stats['stats.st_mtime']).isna()==False
df_stats[mask][['stats.st_mtime','date_creation','date_creation_y','date_creation_m','date_creation_d',
               'date_creation_hms']]


# In[ ]:


get_ipython().run_cell_magic('time', '', '#Test de fnph_read_mongodb avec graphique pour évaluer la bonne taille des \'chunks\'\ndf_graph=pd.DataFrame(columns=[\'date\',\'size\',\'timelaps\'])\n\n#variables de test\nmycollection_name="images_videos"\ntot_enr = mydb[mycollection_name].find({}).count()\nmysize  = 10000\n\n#Test de fnph_read_mongodb(\nmyquery_match      =  {"eventgnls":""} #le null ne convient pas à Python\n#   projection-->\nmyquery_projection =  { "_id"              :0,\n                        "filename"         :1, \n                        "exif.36865"       :1, \n                        "exif.36866"       :1, \n                        "stats.st_mtime"   :1, \n                        "stats.st_size"    :1, \n                        "orgnl_dirname"    :1, \n                        "doctype"          :1, \n                        "extfile"          :1, \n                        "vid_ppty.duration":1, \n                        "exif.271"         :1, \n                        "exif.272"         :1  \n                      }\nmyquery_match={}\nmyquery_projection={}\n\n#Traitement\nmydate=datetime.now()\nfor i_size in list(range(0,tot_enr,mysize))[1:]:\n    mychunksize=i_size\n    start=datetime.now()\n    dftest4 = fnph_read_mongodb(mydb_name, mycollection_name, parm_query_match=myquery_match,parm_query_projection=myquery_projection,parm_chunksize=mychunksize, parm_no_id=False)\n    end  = datetime.now()\n    timelaps=end-start\n    print(i_size, timelaps)\n    df_graph = df_graph.append({ \'date\':mydate, \'size\':i_size, \'timelaps\': timelaps}, ignore_index=True) \n    dftest4.shape\n    \nprint(\'df_graph.shape=\', df_graph.shape, \'\\ntot_enr=\', tot_enr)\n\ndf_graph.iloc[:,1:].plot()\n\nstamp = str(datetime.timestamp(mydate)).replace(\'.\',\'_\')\n\ndf_graph_filename= \'prjph_df_graph__\' +                                   \\\n                    mycollection_name + "__" +                        \\\n                    stamp + \\\n                   \'.csv\'\ndf_graph_filename\ndf_graph.to_csv(os.path.join(mypathlog,df_graph_filename), sep=\'\\t\')')


# In[ ]:


dtest={ "st_atime" : 1602889146.53917,
        "st_atime_ns" : (1602889146539166100),
        "st_ctime" : 1602518764.40407,
        "st_ctime_ns" : (1602518764404068000),
        "st_dev" : 344933136,
        "st_file_attributes" : 32,
        "st_gid" : 0,
        "st_ino" : (4503599627581664),
        "st_mode" : 33206,
        "st_mtime" : 1592083818.37079,
        "st_mtime_ns" : (1592083818370789100),
        "st_nlink" : 1,
        "st_reparse_tag" : 0,
        "st_size" : 1055625,
        "st_uid" : 0 }


# In[ ]:


#Test d'une partie de fnph_read_mongodb

testmydb_name = "prjph_catalogue"
testmycollection_name = "test_documents"

testmyquery_match      =  {"extfile":".jpg"} #le null ne convient pas à Python
#   projection-->
testmyquery_projection =  { "_id"              :0,
                        "filename"         :1, 
                        "exif.36865"       :1, 
                        "exif.36866"       :1, 
                        "stats.st_mtime"   :1, 
                        "stats.st_size"    :1, 
                        "orgnl_dirname"    :1, 
                        "doctype"          :1, 
                        "extfile"          :1, 
                        "vid_ppty.duration":1, 
                        "exif.271"         :1, 
                        "exif.272"         :1  
                      }

#testmyquery_match={}
#testmyquery_projection={}

# Connect to MongoDB
#db = _connect_mongo(host='localhost', port=27017, username=None, password=None, db=mydb_name)
testclient = MongoClient(host='localhost', port=27017)
testdb_aux = testclient[testmydb_name]

testnb_enr  = testdb_aux[testmycollection_name].find(testmyquery_match, testmyquery_projection).count()

test_cursor = testdb_aux[testmycollection_name].find(testmyquery_match, testmyquery_projection)

test_df = pd.DataFrame(columns=["a","b","c","d","e","f"])
test_df = pd.DataFrame(testdb_aux[testmycollection_name].find(testmyquery_match, testmyquery_projection))
test_df_json = json_normalize(test_cursor)


print(test_df.shape)
print(test_df_json.shape)

test_df.head()
test_df_json.head()


# In[ ]:


test_cursor


# In[ ]:


from pandas import json_normalize

dftest2 = json_normalize(test_cursor)


# In[ ]:


dftest2


# In[ ]:


#Test affichage des données timestamp de la base
dtest={ "st_atime" : 1569531541.42495,
        "st_atime_ns" : (1602889146539166100),
        "st_ctime" : 1602518764.40407,
        "st_ctime_ns" : (1602518764404068000),
        "st_dev" : 344933136,
        "st_file_attributes" : 32,
        "st_gid" : 0,
        "st_ino" : (4503599627581664),
        "st_mode" : 33206,
        "st_mtime" : 1331003904.0,
        "st_mtime_ns" : (1592083818370789100),
        "st_nlink" : 1,
        "st_reparse_tag" : 0,
        "st_size" : 1055625,
        "st_uid" : 0 }



from datetime import date

#device
print(dtest["st_dev"])

#size of file in octets
print(dtest["st_size"])

#Date de création
# ---> si exif existe : 36867 (date et heure) (The date and time when the original image data was generated. 
                                #For a digital still camera the date and time the picture was taken are recorded.) ou 
#                       36868 (The date and time when the image was stored as digital data.)
# ---> Sinon :
print(date.fromtimestamp(dtest["st_mtime"])) #most recent content modification (date et heure - si secondes<>0)



##most recent content access
#print(date.fromtimestamp(dtest["st_atime"]))



from datetime import date

#device
print(dtest["st_dev"])

#size of file in octets
print(dtest["st_size"])

#Date de création
# ---> si exif existe : 36867 (date et heure) (The date and time when the original image data was generated. 
                                #For a digital still camera the date and time the picture was taken are recorded.) ou 
#                       36868 (The date and time when the image was stored as digital data.)
# ---> Sinon :
print(date.fromtimestamp(dtest["st_mtime"])) #most recent content modification (date et heure - si secondes<>0)



##most recent content access
#print(date.fromtimestamp(dtest["st_atime"]))

