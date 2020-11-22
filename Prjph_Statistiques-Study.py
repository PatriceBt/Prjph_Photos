#!/usr/bin/env python
# coding: utf-8

# # Projet Photos - Statistiques - Etudes des doublons

# 
# Fonction d'analyse des documents topés 'doublons' en base du fait d'un nom de fichier identique.
# 
# **Objectif** : donner des éléments pour aider à définir les actions à suivre quant à la gestion des doublons.
# 
# Date de création : le 22.11.2020.
# 
# 

# ## Avancement
# 
# ### Réalisé
# 
#     Statistiques sur les doublons par type de fichier et extensions.
# 
#     Calcul des volumes 
# 
# 
# ### A venir
# 
#     Supprimer les fonctions du module Statistique    
#         
#         
# ### A corriger
# 
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
import matplotlib.ticker as ticker
from matplotlib.ticker import FormatStrFormatter, FuncFormatter
import seaborn as sns

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
    


# In[7]:


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
    


# In[8]:


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


# In[9]:


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


# In[10]:


def fn_graph_quantite_volume_par_extension():
    
    from matplotlib.ticker import FuncFormatter

    mytitle = 'Par extension de fichier (type) - quantité et volume (Go) des fichiers'
    df_st = df_stats.groupby('extfile').agg({'filename':['count'], 'stats.st_size':['sum']})
    
    fig, ax = df_st.plot(kind='bar',subplots=True,sharex=True,sharey=False,title=mytitle,figsize=(15,5))
    #graduation
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000000))) #pour exprimer en Go

    plt.show()


# In[11]:


def fn_graph_quantite_volume_par_dossier():
    
    from matplotlib.ticker import FuncFormatter

    mytitle = 'Par dossier [à modifier pour avoir le dossier initial de recherche] - quantité et volume (Go) par répertoire'
    plt.figure(figsize=(15,5))
    df_st = df_stats.groupby('orgnl_dirname_str').agg({'filename':['count'], 'stats.st_size':['sum']})
    
    fig, ax = df_st.plot(kind='bar',subplots=True,sharex=True,sharey=False,title=mytitle,figsize=(15,5))
    
    #graduation
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000000))) #pour exprimer en Go    
    plt.show()


# In[12]:


# Croisements sur df_stats
    
def fn_graph_quantite_volume_par_an_et_catégorie():
    
    mytitle = 'Par année et catégorie - quantité et volume des fichiers'
    df_syc = df_stats.groupby(['date_creation_y','doctype']).agg({'filename':['count'], 'stats.st_size':['sum']})
    df_syc.plot(kind='bar',subplots=True,sharex=True,sharey=False,title=mytitle,figsize=(15,5))
    plt.show()
    


# In[352]:


#dataframe des doublons

def fnph_df_doublons(parm_collection):
    
    import tqdm
    """
    Fonction de calcul de statistiques sur les doublons et graphiques
    retourne le dataframe des noms de fichiers uniques du catalogue (regroupement par nom de fichier)
    """
    #-------------------
    #Requête aggrégation
    var_group = {"$group": { "_id"    : {"filename_grpd": "$filename", "extfile_grpd":"$extfile", "doctype_grpd":"$doctype"},
                       "listeIds_size": {"$addToSet": {"id":"$_id","size":"$stats.st_size","dbl":"$doublon.est_doublon_de"}},
                       "list_sizes"   : {"$addToSet": "$stats.st_size"},                   
                       "nb_doc"       : {"$sum":1},
                       "tot_size"     : {"$sum":"$stats.st_size"}
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
    print(df_result[df_result['_id']=={}].index)
    df_result = df_result.drop(df_result[df_result['_id']=={}].index)
    
    #----------------------------------------------------------------------------
    #Calcul du nombre de sizes distinctes (y.c. non renseignées)
    #----------------------------------------------------------------------------
    df_result['nb_distinct_sizes']=df_result.apply(lambda x: len(x['list_sizes']), axis=1)
    #ATTENTION !!! : list_sizes est déduit de la liste des sizes récupérées. Hors, si size n'est pas renseignée, elle
    #ATTENTION !!!   n'apparait pas dans la liste. Le nombre indiqué ne tient compte que des size renseignées.
    #                on peut donc avoir 0 si aucune size n'est renseignée (cas de 26 fichiers au 21/11)
    
    #--------------------------------
    # dégroupement de _id en trois colonnes (pour avoir _id, ext, type)
    #--------------------------------
    df_result['filename_grpd']=df_result.apply(lambda x: x['_id']['filename_grpd'], axis=1)
    df_result['extfile_grpd'] =df_result.apply(lambda x: x['_id']['extfile_grpd'], axis=1)
    df_result['doctype_grpd'] =df_result.apply(lambda x: x['_id']['doctype_grpd'], axis=1)

    #------------------------------------------------
    # Indicateur de présence de size non renseignée
    #------------------------------------------------
    df_result['flag_size_absente']=False

    #pour chaque ligne, on lit la liste ids, size et récupérer les sizes pour les stocker dans la colone list_sizes
#    for i in tqdm.tqdm(range(df_result.shape[0])):
#    #for i in range(df_result.shape[0]):
#    
#        #listeIds_size mise sous forme de liste
#        mylist=df_result.listeIds_size.iloc[i]   #on se positionne sur la ième valeur (<> loc[i]=index de valeur i)
#        
#        #test une absence de 'size' et valorisation du flag = True le cas échéant
#        for j in range(len(mylist)):
#            if not ('size' in mylist[j]):
#                #print('size absente pour ', i,j)
#                #df_result.flag_size_absente.iloc[i]=True

    for i in tqdm.tqdm(df_result.index):
    
        #listeIds_size mise sous forme de liste
        mylist=df_result.loc[i, 'listeIds_size']   #on se positionne sur la ième valeur (<> loc[i]=index de valeur i)
        
        #test une absence de 'size' et valorisation du flag = True le cas échéant
        for j in range(len(mylist)):
            if not ('size' in mylist[j]):
                #print('size absente pour ', i,j)
                df_result.loc[i, 'flag_size_absente']=True
            
            
    return(df_result)


# In[14]:


#def fnph_graph_doublons(parm_collection):

#pour test
client = pymongo.MongoClient('localhost',27017)
mydb = client["prjph_catalogue"]
mydb_name = "prjph_catalogue"
#----------------------------------------
#mycollection_name = "test_documents"          #TEST
mycollection_name = "images_videos"           #PREPROD
#----------------------------------------
mycollection = mydb[mycollection_name]

#appel dataframe des doublons
#df_test = fnph_df_doublons(mycollection).copy()
df_par_fichier = fnph_df_doublons(mycollection).copy()

#split ici car un peu long à refaire
print('done', df_par_fichier.shape)


# In[15]:


df_par_fichier.head(2)


# In[351]:


#CALCUL DES STATISTIQUES SUR DOUBLONS

#Mettre tout ça en def (cf. appel pour doublons avec size absente plus bas) :

#import matplotlib.ticker as ticker
from matplotlib.ticker import FormatStrFormatter, FuncFormatter

#Récupérer le nombre total de fichiers (cf. traitement principal)
nb_tot_fichiers  = df_par_fichier.shape[0]

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#I-ETUDE SUR L'ENSEMBLE DES FICHIERS AVEC DOUBLONS (avec ou sans occurrence n'ayant pas de 'size')
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

#on restreint les données aux fichiers avec doublons (xples occurrences : nb_doc>1 ou (nb_doc=1 et size absente))
mask1  = df_par_fichier.nb_doc>1
#mask2 = (df_par_fichier.nb_doc==1) & (df_par_fichier.flag_size_absente==True) : pas la peine car nb_doc=1 <=> un seul _id (même si size pas récupérée)
#df_dbls = df_par_fichier[mask1|mask2].copy()
df_dbls = df_par_fichier[mask1].copy()


#Les calculs à mettre en def :

#-------------------------------------------------------------------------------
#0) nombre de fichiers uniques total (avec ou sans doublons)
#-------------------------------------------------------------------------------
print("Il y a {} fichiers uniques avec ou sans doublons.".format(nb_tot_fichiers))

#-------------------------------------------------------------------------------
#1a) nombre de fichiers avec doublons
#-------------------------------------------------------------------------------
nb_fic_avec_dbls = df_dbls.shape[0]
print("Il y a {} fichiers avec de multiples occurrences.".format(nb_fic_avec_dbls))

#-------------------------------------------------------------------------------
#1b) nombre de fichiers sans doublons
#-------------------------------------------------------------------------------
nb_fic_sans_dbls = df_par_fichier[ (df_par_fichier.nb_doc==1) & (df_par_fichier.flag_size_absente==False) ].shape[0]
print("Il y a {} fichiers sans doublons.".format(nb_fic_sans_dbls))

#-------------------------------------------------------------------------------
#2b) dont nombre de fichiers avec doublons et un des doublons avec 'size' non renseignée
#-------------------------------------------------------------------------------
print("Il y a {} fichiers avec de multiples occurrences dont une avec size non renseignée.".       format(df_dbls[df_dbls.flag_size_absente==True].shape[0]))

#-------------------------------------------------------------------------------
#2) nombre de fichiers en doublons, part sur le total de fichiers uniques
#-------------------------------------------------------------------------------
print("La part de fichiers avec doublons sur le total de fichiers = {}".format(100*nb_fic_avec_dbls/nb_tot_fichiers))


#-------------------------------------------------------------------------------
#3a) nombre de fichiers 'doublons' par fichier avec doublons : moyenne
#-------------------------------------------------------------------------------
print("nombre de fichiers avec 'doublons' par fichier avec doublons : moyenne = ",df_dbls.nb_doc.mean())

#-------------------------------------------------------------------------------
#3b) nombre de fichiers 'doublons' par fichier avec doublons : distribution
#-------------------------------------------------------------------------------
plt.figure(figsize=(15,5))
nb_bins = df_dbls.nb_doc.max() - df_dbls.nb_doc.min()
#ax1 = sns.distplot(df_dbls[df_dbls.nb_doc>1].nb_doc, bins=nb_bins )   #nb : 'ax1 =' is not necessary here 
from   scipy import stats
ax1 = sns.distplot(df_dbls.nb_doc, bins=nb_bins, fit=stats.norm  )   #nb : 'ax1 =' is not necessary here 
#Graduations
ax1.xaxis.set_major_locator(ticker.MultipleLocator(25))  #les ticks majeurs pour les années (toutes les 4 graduations(u))
ax1.xaxis.set_minor_locator(ticker.MultipleLocator(1))  #les ticks mineurs pour les trimestres (à chaque graduation (u))
#ax1.yaxis.set_major_locator(ticker.MultipleLocator(0.5))  
#ax1.yaxis.set_minor_locator(ticker.MultipleLocator(0.1))  
ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x*nb_fic_avec_dbls))) #pour exprimer en nb
#ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))  #pour un séparteur de milliers
#ax1.yaxis.set_major_formatter(FormatStrFormatter('%d'))
#-------------------------------
#commentaire sur le résultat 3b)


# Remarque : on pourra utiliser hue pour distinguer selon flag_size_absente (avec mpl car sns.distplot non)
# 
# 1) Peu de fichiers avec size non renseignée => Voir lesquels sont concernés
# 
# => **Hypothèse H1 : ils concernent des fichiers qui ne sont pas des photos, plutôt des fichiers windows.**
# 
# => Si c'est le cas, on pourra les mettre de côté.
# 
# 
# 2) La moitié des fichiers en doublon sont ceux avec 2 documents (voir la copie d'un dd ou les thumbnails d'un répertoire)
# 
# => **Hypothèse H2 : les fichiers en doublons avec 2 docs sont de vrais doublons. On pourra garder un seul élément.**
# 
# 3) Une moyenne de 4+ documents par fichier doublon, ce qui explique les 300000+ documents de la base.
# 
# => **Hypothèse H3 : les fichiers +2 doc ne sont pas de vrais doublons. Il va falloir trouver un découpage plus fin.**
# 
# => **Hypothèse H4 : les fichiers avec au moins 2 documents et 1 seule size sont de vrais doublons.
# 

# In[353]:


#-------------------------------------------------------------------------------
#4a) Quels sont les types de fichiers avec doublons => nombre de fichiers en doublon par type de fichier
#-------------------------------------------------------------------------------

print("Nombre de fichiers en doublon par type")

df_dbls_image  = df_dbls[df_dbls.doctype_grpd == 'image']
df_dbls_video  = df_dbls[df_dbls.doctype_grpd == 'video']
                         
print("il y a {} fichiers 'image' avec des doublons".format(df_dbls_image.shape[0]))
print("il y a {} fichiers 'vidéo' avec des doublons".format(df_dbls_video.shape[0]))

plt.bar([0],[df_dbls_image.shape[0]], label="image")
plt.bar([1],[df_dbls_video.shape[0]], label="vidéo")
#plt.legend()
mylabels = ['image', 'vidéo']
plt.xticks(range(2), mylabels, fontsize=16, rotation=0)
plt.show()



#-------------------------------------------------------------------------------
#4b) Quels sont les types de fichiers avec doublons => nombre de fichiers en doublon par extension
#-------------------------------------------------------------------------------
#df_nb_fic_par_extension = df_dbls[['_id','extfile_grpd', 'doctype_grpd']].groupby(['extfile_grpd']).count().reset_index()
df_nb_fic_par_extension = df_dbls[['_id','extfile_grpd', 'doctype_grpd']].                                    groupby(['extfile_grpd']).agg({"_id":"count","doctype_grpd":"last"}).reset_index()
df_nb_fic_par_extension.columns=['extfile', 'count', 'type']

print("nombre de fichiers en doublon par extension (par type de fichier, échelle log ou non): ")
#df_nb_fic_par_extension[['extfile', 'count']]

#Graph

#grid -> draw 2x2 subplots and assign them to axes variables
fig, axe_array = plt.subplots(2,2, sharex=False, sharey=False, figsize=(15,5)) #with non shared axis

#pour les types 'image'
df_for_graph = df_nb_fic_par_extension[df_nb_fic_par_extension.type=='image'].sort_values(by=['type','count'], ascending=False)
sns.barplot(x=df_for_graph['extfile'], y=df_for_graph['count'], color="darkblue", log=False, data = df_for_graph, ax=axe_array[0,0])
sns.barplot(x=df_for_graph['extfile'], y=df_for_graph['count'], color="darkblue", log=True , data = df_for_graph, ax=axe_array[1,0])

#pour les types 'video'
df_for_graph = df_nb_fic_par_extension[df_nb_fic_par_extension.type=='video'].sort_values(by=['type','count'], ascending=False)
sns.barplot(x=df_for_graph['extfile'], y=df_for_graph['count'], color="darkblue", log=False, data = df_for_graph, ax=axe_array[0,1])
sns.barplot(x=df_for_graph['extfile'], y=df_for_graph['count'], color="darkblue", log=True , data = df_for_graph, ax=axe_array[1,1])

plt.show()


# ### Commentaires
# 
# On note que les fichiers en doublons sont essentiellement des jpg.
# 
# Viennent ensuite 
# - les png, puis gif, jpeg, svg, bmp, webp et tif pour les images
# - les mp4 puis 3gp, mov, avi, dat, vob et wmv pour les vidéos.
# 

# In[354]:


df_nb_fic_par_extension.sort_values(by=['type','count'], ascending = False)


# In[355]:


#OBJECTIF : COMMENT TRAITER LES DOUBLONS ? COMMENT MINIMISER LE TRAITEMENT MANUEL, LE RENDRE LE PLUS RAPIDE POSSIBLE.
#           QUELLE DIFFERENCE DE VOLUME AVEC ET SANS DOUBLONS -> QUEL VOLUME PREVOIR ?
#=====================================================================================================================


# EVALUATION DU VOLUME QUE REPRESENTENT LES FICHIERS AVEC DOUBLONS et DU GAIN DE VOLUME SANS DOUBLONS
#-------------------------------------------------------------------------------
#5a) taille des doublons : moyenne des sizes de documents par fichier avec doublon - yc fichiers sans size
#-------------------------------------------------------------------------------
#c'est possible comme ça avec df_dbls=df_fichier[mask...].copy() - cf. plus haut
df_dbls['size_moyenne']=df_dbls['tot_size']/df_dbls['nb_doc']
#avec .loc pour éviter erreurs (cf. warning iloc)
#df_dbls.loc[:,'size_moyenne']=df_dbls.loc[:, 'tot_size']/df_dbls.loc[:,'nb_doc']
#df_dbls.loc[:,'size_moyenne']=df_dbls.apply(lambda x: x['nb_doc'], axis=1)
#for i in df_dbls.index:
#    df_dbls.loc[i, 'size_moyenne']=df_dbls.loc[i, 'tot_size']/df_dbls.loc[i, 'nb_doc']
#mydf = df_dbls.tot_size/df_dbls.nb_doc
#df_dbls.size_moyenne = mydf.values


# In[356]:


#Graphique : distibution de la moyenne
fig=plt.figure(figsize=(15,5))
from   scipy import stats
sns.distplot(df_dbls.size_moyenne) #, fit=stats.norm)
#formatage des graduations
ax=plt.gca()
ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en Ko


# On ne voit pas bien les valeurs extrêmes. On va utiliser boxplot :

# In[357]:


#boxplot
plt.figure(figsize=(15,4))
ax = sns.boxplot(df_dbls.size_moyenne)
#formatage des graduations
ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en Ko


# Il y a de gros gros écarts (on ne voit même pas la boîte), on va préciser avec describe : 

# In[358]:


#quelques chiffres : 
#df_dbls.size_moyenne.describe().apply(lambda x: format(x, 'f'))
#df_dbls.size_moyenne.describe().apply(lambda x: format(x, '.f'))
df_dbls.size_moyenne.describe().apply(lambda x: format(x, '.2f'))
#for format see https://stackoverflow.com/questions/40347689/dataframe-describe-suppress-scientific-notation
#For entire DataFrame (as suggested by @databyte )
#df.describe().apply(lambda s: s.apply('{0:.5f}'.format))
#For whole DataFrame (as suggested by @Jayen):
#df.describe().apply(lambda s: s.apply(lambda x: format(x, 'g')))


# On voit que quelques fichiers 'doublons' ont une taille moyenne très importante par rapport au reste, on va les isoler.
# 75% sont en dessous de 2173939.
# 
# On procède à un découpage pour avoir une représentation claire :
# 
# 

# In[25]:


def fn_graph_study_dbls(parm_limitsize=2200000, parm_limibigsize=2300000):

    limitsize    =    parm_limitsize 
    limitbigsize =    parm_limibigsize 
    df_dbls_bigsize  = df_dbls[ df_dbls.size_moyenne> limitsize ]
    df_dbls_bigsize1 = df_dbls[(df_dbls.size_moyenne> limitsize) & (df_dbls.size_moyenne<= limitbigsize)]
    df_dbls_bigsize2 = df_dbls[ df_dbls.size_moyenne> limitbigsize ]
    df_dbls_lowsize  = df_dbls[ df_dbls.size_moyenne<=limitsize ]

    work = df_dbls_lowsize.doctype_grpd.unique()
    print("il y a {} fichiers 'doublons' de type {} de taille moyenne inférieure à {} Mo ({})."          .format(df_dbls_lowsize.shape[0], [work[i] for i in range(len(work))], limitsize/1000000, limitsize))

    work = df_dbls_bigsize.doctype_grpd.unique()
    print("il y a {} fichiers 'doublons' de type {} de taille moyenne supérieure à {} Mo ({})."          .format(df_dbls_bigsize.shape[0], [work[i] for i in range(len(work))], limitsize/1000000, limitsize))

    print("dont : ")

    work = df_dbls_bigsize1.doctype_grpd.unique()
    print("     > {} fichiers 'doublons' de type {} de taille moyenne comprise entre {} ({}) et {} Mo ({})."          .format(df_dbls_bigsize1.shape[0], [work[i] for i in range(len(work))],limitsize/1000000,limitsize                                                                                 ,limitbigsize/1000000, limitbigsize ))

    work = df_dbls_bigsize1.doctype_grpd.unique()
    print("     > {} fichiers 'doublons' de type {} de taille moyenne supérieure à {} Mo ({})."          .format(df_dbls_bigsize2.shape[0], [work[i] for i in range(len(work))], limitbigsize/1000000, limitbigsize))


    #boxplot
    sns.boxplot(df_dbls_lowsize.size_moyenne)
    plt.title("lowsize en Mo")
    #formatage des graduations
    ax=plt.gca()
    ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%.3f" % (x/1000000))) #pour exprimer en Mo
    plt.show()

    sns.boxplot(df_dbls_bigsize1.size_moyenne)
    plt.title("bigsize1 en Mo")
    #formatage des graduations
    ax=plt.gca()
    ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%.3f" % (x/1000000))) #pour exprimer en Mo
    plt.show()

    sns.boxplot(df_dbls_bigsize2.size_moyenne)
    plt.title("bigsize2 en Mo")
    #formatage des graduations
    ax=plt.gca()
    ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000))) #pour exprimer en Mo
    plt.show()

    # distibution de la moyenne
    fig=plt.figure(figsize=(15,5))
    from   scipy import stats
    sns.distplot(df_dbls_lowsize.size_moyenne) #, fit=stats.norm)
    #formatage des graduations
    ax=plt.gca()
    ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en Ko
    plt.title("lowsize")
    plt.show()

    # distibution de la moyenne
    fig=plt.figure(figsize=(15,5))
    from   scipy import stats
    sns.distplot(df_dbls_bigsize1.size_moyenne) #, fit=stats.norm)
    #formatage des graduations
    ax=plt.gca()
    ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en Ko
    plt.title("bigsize1")
    plt.show()

    # distibution de la moyenne
    fig=plt.figure(figsize=(15,5))
    from   scipy import stats
    sns.distplot(df_dbls_bigsize2.size_moyenne) #, fit=stats.norm)
    #formatage des graduations
    ax=plt.gca()
    ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en Ko
    plt.title("bigsize2")
    plt.show()


# 1er découpage : 
# autour du quartile 75: 2173939

# In[26]:


#1er découpage :
var_limitsize    = 2173939
var_limitbigsize = 2173939
fn_graph_study_dbls(var_limitsize, var_limitbigsize)


# Les lowsize sont bien décallés => la moyenne est très faible par rapport à la répartition.
# Les bigsize idem
# 
# Hypothèse : il y a trois groupes : 
# 1) entre 0 et 0.500 Mo (500ko, ce qui est petit pour une image et pourraît correspondre aux thumbnails par exemple)
# 2) de 0.500 Mo a 2.500 Mo (ie autour de la moyenne globale (2.019289 Mo) - cf. describe plus haut)
# 3) au-dessus de 2.500 qui serait encore à découper

# In[27]:


#2ème découpage :
var_limitsize    =  500000
var_limitbigsize = 2500000
fn_graph_study_dbls(var_limitsize, var_limitbigsize)


# En fait, il faudrait encore découper la première tranche puis la dernière. 
# => on va refaire histogramme en spécifiant le nombre de bins et en précisant la plage des y (car valeurs très petites):

# In[28]:


#Graphique : distibution de la moyenne
fig=plt.figure(figsize=(15,5))
from   scipy import stats
sns.distplot(df_dbls.size_moyenne, bins=100) #, fit=stats.norm)
#formatage des graduations
ax=plt.gca()
ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en Ko

bottom, top = plt.ylim()  # return the current ylim
plt.ylim(bottom, top/100000)   # set the ylim to bottom, top
print(bottom, top)

plt.show()


# On voit ici que des groupes se concentrent :
# le premier et plus gros à moins de 1Go, puis 
# - autour de 1Go, 
# - autour de 2Go (la moyenne),
# - autour de 3.2 Go
# - autour de 4.5 Go
# 
# On s'éloigne un peu  :

# In[29]:


#Graphique : distibution de la moyenne
fig=plt.figure(figsize=(15,5))
from   scipy import stats
sns.distplot(df_dbls.size_moyenne, bins=100) #, fit=stats.norm)
#formatage des graduations
ax=plt.gca()
ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en Ko

bottom, top = plt.ylim()  # return the current ylim
plt.ylim(bottom, top/10000)   # set the ylim to bottom, top
print(top, plt.ylim())

plt.show()


# Utilisons hist de mpl pour afficher en log : 
# 

# In[30]:


df_dbls.size_moyenne.hist( bins=100, log=True)
plt.show()


# On retrouve les groupes observés autour de certaines valeurs.
# Et en fait, on se rend compte qu'il n'y en a que 1 à chaque fois. 
# 
# Approchons-nous :
# 

# In[31]:


df_dbls.size_moyenne.hist( bins=100, log=True)
bottom, top = plt.ylim()
plt.ylim(bottom, 100)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
#format axis
ax=plt.gca()
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000))) #pour exprimer en Go
ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités

plt.show()


# En fait, ce sont les gros fichiers de plus de 1Go qui étendent la distribution.
# La moyenne étant 2.5 Mo, c'est sur que ça perturbe.
# 
# Approchons-nous encore du groupe 0-1Go : 
# 

# In[32]:


df_dbls.size_moyenne.hist( bins=100, log=True)

bottomy, topy = plt.ylim()
bottomx, topx = plt.xlim()
plt.ylim(bottomy, top)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
plt.xlim(bottomx, 1000000000)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)

#format axis
ax=plt.gca()
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000))) #pour exprimer en Go
ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités

plt.show()


# La moyenne est de 2.5Mo, il reste bcp de fichier au dessus. 
# On va distinguer les vidéos et les images.
# 
# 

# In[33]:


df_dbls_image  = df_dbls[df_dbls.doctype_grpd == 'image']
df_dbls_video  = df_dbls[df_dbls.doctype_grpd == 'video']

#ne marche pas : 
#fig, axe_array = plt.subplots(1,2, sharex=False, sharey=False, figsize=(15,5)) #with non shared axis
#axe_array[0] = df_dbls_video.size_moyenne.hist()
#axe_array[1] = df_dbls_image.size_moyenne.hist()

#ca ca marche bien 
#df_dbls_video.size_moyenne.hist(bins=mybins, label="vidéos")
#plt.legend()
#plt.show()
#df_dbls_image.size_moyenne.hist(bins=mybins, label="images")
#plt.legend()
#plt.show()

plt.figure(figsize=(15,5))

ax1 = plt.subplot2grid((1,2), (0,0))
ax2 = plt.subplot2grid((1,2), (0,1))

mybins=100

ax1.hist(df_dbls_image.size_moyenne,bins=mybins, label="images", color="b")
ax2.hist(df_dbls_video.size_moyenne,bins=mybins, label="vidéos", color="orange")

ax1.legend()
ax2.legend()

#format axis
ax1.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000))) #pour exprimer en Mo
ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités
ax2.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%.3f" % (x/1000000000))) #pour exprimer en Go
ax2.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités

ax1.set_xlabel("en Mo")
ax2.set_xlabel("en Go")

ax1.set_ylabel("Nombre de fichiers")
ax2.set_ylabel("Nombre de fichiers")

plt.show()


# Pourquoi l'axe des x est si grand ?
# Vérifions la présence de fichiers image avec size_moyenne > 10Mo
# et vidéo > 4 (on le sait déjà en fait, c'est une question d'échelle des axes y, parcequ'il n'y en a qu'un :)
# 

# In[34]:


df_dbls_image[df_dbls_image.size_moyenne > 10000000]


# In[35]:


df_dbls_video[df_dbls_video.size_moyenne > 4000000000]


# On se rapproche : 

# In[36]:


df_dbls_image  = df_dbls[df_dbls.doctype_grpd == 'image']
df_dbls_video  = df_dbls[df_dbls.doctype_grpd == 'video']

plt.figure(figsize=(15,5))

ax1 = plt.subplot2grid((1,2), (0,0))
ax2 = plt.subplot2grid((1,2), (0,1))

mybins=100

ax1.hist(df_dbls_image.size_moyenne,bins=mybins, label="images", color="b")
ax2.hist(df_dbls_video.size_moyenne,bins=mybins, label="vidéos", color="orange")

ax1.legend()
ax2.legend()

#format axis
ax1.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000))) #pour exprimer en Mo
ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités
ax2.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%.3f" % (x/1000000000))) #pour exprimer en Go
ax2.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités

ax1.set_xlabel("en Mo")
ax2.set_xlabel("en Go")

ax1.set_ylabel("Nombre de fichiers")
ax2.set_ylabel("Nombre de fichiers")

#zoom-------------------------
mylimy = 100
bottom1y, top1y = ax1.get_ylim()
bottom1x, top1x = ax1.get_xlim()
bottom2y, top2y = ax2.get_ylim()
bottom2x, top2x = ax2.get_xlim()
ax1.set_ylim(bottom1y, mylimy)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
ax1.set_xlim(bottom1x, top1x)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
ax2.set_ylim(bottom2y, mylimy)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
ax2.set_xlim(bottom2x, top2x)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
#zoom-------------------------

plt.show()


# On voit que 
# image : il n'y a qu'un fichier > 10Mo
# vidéo :quelques-uns seulement sont au-dessus de 500Mo
# 
# On se rapproche : 
# 

# In[37]:


df_dbls_image  = df_dbls[df_dbls.doctype_grpd == 'image']
df_dbls_video  = df_dbls[df_dbls.doctype_grpd == 'video']

plt.figure(figsize=(15,5))

ax1 = plt.subplot2grid((1,2), (0,0))
ax2 = plt.subplot2grid((1,2), (0,1))

mybins=500

ax1.hist(df_dbls_image.size_moyenne,bins=mybins, label="images", color="b")
ax2.hist(df_dbls_video.size_moyenne,bins=mybins, label="vidéos", color="orange")

ax1.legend()
ax2.legend()

#format axis
ax1.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000))) #pour exprimer en Mo
ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités
ax2.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%.3f" % (x/1000000000))) #pour exprimer en Go
ax2.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités

ax1.set_xlabel("en Mo")
ax2.set_xlabel("en Go")

ax1.set_ylabel("Nombre de fichiers")
ax2.set_ylabel("Nombre de fichiers")

#zoom-------------------------
mylim1y = 30000
mylim2y = 1700
mylim1x = 10000000   #(10Mo)
mylim2x = 1000000000 #(1Go)
bottom1y, top1y = ax1.get_ylim()
bottom1x, top1x = ax1.get_xlim()
bottom2y, top2y = ax2.get_ylim()
bottom2x, top2x = ax2.get_xlim()
ax1.set_ylim(bottom1y, mylim1y)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
ax1.set_xlim(bottom1x, mylim1x)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
ax2.set_ylim(bottom2y, mylim2y)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
ax2.set_xlim(bottom2x, mylim2x)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
#zoom-------------------------

plt.show()


# On voit : 
# images : de tous petits fichiers (moins de 250Ko) sont très nombreux : thumbnails ?
# vidéos : il y a quelques fichiers au-dessus de 100 Mo. Dispersés.
# 
# Voyons de plus près encore :

# In[38]:


get_ipython().run_line_magic('matplotlib', 'inline')

df_dbls_image  = df_dbls[df_dbls.doctype_grpd == 'image']
df_dbls_video  = df_dbls[df_dbls.doctype_grpd == 'video']

plt.figure(figsize=(15,5))

ax1 = plt.subplot2grid((1,2), (0,0))
ax2 = plt.subplot2grid((1,2), (0,1))

mybins=500

ax1.hist(df_dbls_image.size_moyenne,bins=mybins, label="images", color="b")
ax2.hist(df_dbls_video.size_moyenne,bins=mybins, label="vidéos", color="orange")

ax1.legend()
ax2.legend()

#format axis
ax1.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000))) #pour exprimer en Ko
ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités
ax2.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%.3f" % (x/1000000000))) #pour exprimer en Go
ax2.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités

ax1.set_xlabel("en Ko")
ax2.set_xlabel("en Go")

ax1.set_ylabel("Nombre de fichiers")
ax2.set_ylabel("Nombre de fichiers")

#zoom-------------------------
mylim1x = 1000000   #(1000Ko)
mylim1y = 30000
mylim2x = 1000000000 #(1Go)
mylim2y = 20
bottom1y, top1y = ax1.get_ylim()
bottom1x, top1x = ax1.get_xlim()
bottom2y, top2y = ax2.get_ylim()
bottom2x, top2x = ax2.get_xlim()
ax1.set_ylim(bottom1y, mylim1y)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
ax1.set_xlim(0, mylim1x)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
ax2.set_ylim(bottom2y, mylim2y)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
ax2.set_xlim(bottom2x, mylim2x)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
#zoom-------------------------

plt.show()


# On voit maintenant :
# images : +25000 fichiers de moins de 100 Mo
# vidéos : la grande majorité répartie sous 200 Mo.
# 
# On se rapproche encore : 
# 

# In[39]:


get_ipython().run_line_magic('matplotlib', 'inline')

df_dbls_image  = df_dbls[df_dbls.doctype_grpd == 'image']
df_dbls_video  = df_dbls[df_dbls.doctype_grpd == 'video']

plt.figure(figsize=(15,5))

ax1 = plt.subplot2grid((1,2), (0,0))
ax2 = plt.subplot2grid((1,2), (0,1))

mybins=500

ax1.hist(df_dbls_image.size_moyenne,bins=mybins, label="images", color="b")
ax2.hist(df_dbls_video.size_moyenne,bins=mybins, label="vidéos", color="orange")

ax1.legend()
ax2.legend()

#format axis
ax1.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000))) #pour exprimer en Ko
ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités
ax2.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%.3f" % (x/1000000000))) #pour exprimer en Go
ax2.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités

ax1.set_xlabel("en Ko")
ax2.set_xlabel("en Go")

ax1.set_ylabel("Nombre de fichiers")
ax2.set_ylabel("Nombre de fichiers")

#zoom-------------------------
mylim1x = 100000   #(100Ko)
mylim1y = 30000
mylim2x = 200000000 #(200Mo)
mylim2y = 2000
bottom1y, top1y = ax1.get_ylim()
bottom1x, top1x = ax1.get_xlim()
bottom2y, top2y = ax2.get_ylim()
bottom2x, top2x = ax2.get_xlim()
ax1.set_ylim(bottom1y, mylim1y)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
ax1.set_xlim(0, mylim1x)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
ax2.set_ylim(bottom2y, mylim2y)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
ax2.set_xlim(0, mylim2x)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
#zoom-------------------------

plt.show()


# Il s'agit ici d'un zoom. On refait la dist sur la sélection des images<200Ko et des vidés<100Mo
# 
# 
# 

# In[43]:


df_dbls_image_100Ko = df_dbls_image[df_dbls_image.size_moyenne<100000]
df_dbls_video_100Mo = df_dbls_video[df_dbls_video.size_moyenne<100000000]

plt.figure(figsize=(15,5))

ax1 = plt.subplot2grid((1,2), (0,0))
ax2 = plt.subplot2grid((1,2), (0,1))

mybins=500

ax1.hist(df_dbls_image_100Ko.size_moyenne,bins=mybins, label="images", color="b")
ax2.hist(df_dbls_video_100Mo.size_moyenne,bins=mybins, label="vidéos", color="orange")

ax1.legend()
ax2.legend()

#format axis
ax1.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000))) #pour exprimer en Ko
ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités
ax2.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%.3f" % (x/1000000))) #pour exprimer en Mo
ax2.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités

ax1.set_xlabel("en Ko")
ax2.set_xlabel("en Mo")

ax1.set_ylabel("Nombre de fichiers")
ax2.set_ylabel("Nombre de fichiers")

#zoom-------------------------
#sans
#zoom-------------------------
ax1.set_title("distribution des images < 100ko et des videos <100Mo")
plt.show()


# on voit maintenant que il y a un regroupement de fichiers image de moins de 30Ko ainsi que de nombreuses petites vidéos
# 
# **Hypothèse 4 : ces petits fichiers correspondent à des thumbnails**
# 
# **Hypothèse 5 : ces petites vidéos correspondent à des vidéos iphone. Les grosses aux vidéos caméscope, film...**
# 
# Je refais le graph sur : 
# 
# - les images de moins de 5Mo des tranches de 5Ko (1000 tranches)
# 
# - les vidéos de moins de 100Mo et des tranches de 1Ko (1000 tranches)
# 

# In[61]:


get_ipython().run_line_magic('time', '')

df_dbls_image_5Mo   = df_dbls_image[df_dbls_image.size_moyenne<  5000000]
df_dbls_video_100Mo = df_dbls_video[df_dbls_video.size_moyenne<100000000]

df_dbls_image  = df_dbls[df_dbls.doctype_grpd == 'image']
df_dbls_video  = df_dbls[df_dbls.doctype_grpd == 'video']

plt.figure(figsize=(15,5))

ax1 = plt.subplot2grid((1,2), (0,0))
ax2 = plt.subplot2grid((1,2), (0,1))

#-------------------plots----------------------
mybins1=1000
mybins2=1000
ax1.hist(df_dbls_image_5Mo.size_moyenne,  bins=mybins1, label="images", color="b")
ax2.hist(df_dbls_video_100Mo.size_moyenne,bins=mybins1, label="vidéos", color="orange")
#----------------------------------------------

ax1.legend()
ax2.legend()

#format axis
ax1.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000))) #pour exprimer en Mo
ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités
ax2.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000))) #pour exprimer en Mo
ax2.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités

ax1.set_xlabel("en Mo")
ax2.set_xlabel("en Mo")

ax1.set_ylabel("Nombre de fichiers")
ax2.set_ylabel("Nombre de fichiers")

#zoom-------------------------
mylim1x = 5000000   #(5Mo)
mylim1y = 200
mylim2x = 100000000 #(100Mo)
mylim2y = 30
bottom1y, top1y = ax1.get_ylim()
bottom1x, top1x = ax1.get_xlim()
bottom2y, top2y = ax2.get_ylim()
bottom2x, top2x = ax2.get_xlim()
ax1.set_ylim(bottom1y, mylim1y)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
ax1.set_xlim(0, mylim1x)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
ax2.set_ylim(bottom2y, mylim2y)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
ax2.set_xlim(0, top2x)   # set the ylim (remarque : on récupère bottom car 0 ne passe pas)
#zoom-------------------------

plt.show()


# CONCLUSION :
# 
# - IMAGES : 
#     
#     - Un grand nombre (26063 sur 65054 - soit 40%) des doublons image ont une taille moyenne inférieure à 100Ko.
#     
#     - Le reste est essentiellement réparti autour de la moyenne de 2Mo 
#     
#     - Il y a un pic autour de 750Ko
#     
#     - puis une répartition quasi homogène, légèrement croissante jusque 3Mo
#     
#     - Ensuite le nombre est plus réduit.
#     
#     => préconisations : 
#     
#         . **P1** : voir la même répartition par extension.
#         
#         . **P2** : étudier les petits fichiers (thumbnails ? à copier ?)
#         
#         . **P3** : pour des problématiques de traitements selon les volumes, considérer à part les fichiers > 3Mo (4335)
#     
# - VIDEOS : 
# 
#     - les vidéos sont pour l'essentiel à moins de 10Mo. 
#     
#     - Quelques-unes sont de très gros volumes, à mettre à part dans le cas de traitements sensibles à la volumétrie
#     
#     => préconsations :
#     
#         . **P4** : pour des problématiques de traitements selon les volumes, considérer à part les fichiers > 10Mo (778)
#         
#         . il reste 1677 fichiers de taille moyenne = 1.7Mo
#         
#         .    dont 25% (~400 à moins de 52Ko), 50% moins de 133Ko et 75% moins de 2.7Mo, max à 10Mo.
# 

# In[ ]:


#-------------------------------------------------------------------------------
#5b) taille des doublons : écart max par fichier avec doublon
#-------------------------------------------------------------------------------
import tqdm

#on travaille avec df_dbls
#pour chaque ligne 
for i in tqdm.tqdm(df_dbls.index):
    #récupérer la liste des sizes (on ne se préoccupe pas de savoir s'il y a une size absente ou non... que 26...0 si rien)
    mylist = df_dbls.loc[i, 'list_sizes']
    #calcul écart
    try:
        var_min = min(mylist)
        var_max = max(mylist)
        var_ecart  = var_max - var_min
    except:
        var_min = 0
        var_max = 0
        var_ecart  = var_max - var_min
        
    df_dbls.loc[i, 'size_ecart'] = var_ecart
    df_dbls.loc[i, 'size_min']   = var_min
    df_dbls.loc[i, 'size_max']   = var_max
        
print("--done--")

#ajout de lécart en pourcentage de la taille moyenne
df_dbls['ecart_prct']=df_dbls['size_ecart']/df_dbls['size_moyenne']


# In[188]:


#stats des écarts
#df_dbls.size_moyenne.describe().apply(lambda x: format(x, '.2f'))
#df.describe().apply(lambda s: s.apply(lambda x: format(x, 'g')))
##df.describe().apply(lambda s: s.apply('{0:.5f}'.format))
print(df_dbls[['size_ecart','size_min','size_max']].describe().apply(lambda s: s.apply('{0:.0f}'.format)))

print("")

#écarts à 0
print("Nombre de fichiers avec écart =0 :", (df_dbls.size_ecart==0).sum())
#écarts <>0
print("Nombre de fichiers avec écart >0 :", (df_dbls.size_ecart!=0).sum())


# A priori, il y aurait 55257 'vrais doublons'
# 
# Intéressons-nous aux 12252 :
# 

# In[202]:


df_dbls_ecart_nonnull = df_dbls[df_dbls.size_ecart!=0]

print(df_dbls_ecart_nonnull.size_ecart.describe().apply(lambda x: format(x, '.0f')))


# In[196]:


get_ipython().run_line_magic('matplotlib', 'notebook')

ax = df_dbls_ecart_nonnull.size_ecart.hist(bins=100, log=True)
#format axis
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000))) #pour exprimer en Mo
ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités
plt.title("Distribution des écarts en Mo")
plt.show()


# On remarque beaucoup de fichiers avec un écart inférieur à 10Mo, ce qui est normal puisque la moyenne de la taille des fichiers est de 2.5 Mo.
# 
# Regardons plutôt l'écart en terme de pourcentage de la taille du fichier (on va prendre la taille moyenne)
# 

# In[208]:



print(df_dbls_ecart_nonnull.ecart_prct.describe().apply(lambda x: format(x, '.2f')))


# Remarques : 
# des écarts > 100% : problablement sur des fichiers avec size absente
# Vérifions :
# 

# In[210]:


print((df_dbls_ecart_nonnull[df_dbls_ecart_nonnull.flag_size_absente==False]).ecart_prct.describe().apply(lambda x: format(x, '.2f')))


# En fait non. En vérifiant, les 26 avec size non renseignée ont aucune size renseignée, donc ecart = 0.
# 
# Donc, si ecart>100% de la taille moyenne, c'est probablement à du faux doublon.
# 
# Voyons la distribution :
# 

# In[234]:


df_dbls_ecart_nonnull.ecart_prct.head(4)


# In[248]:


#%matplotlib notebook
get_ipython().run_line_magic('matplotlib', 'inline')

plt.figure(figsize=(15,4))
ax = df_dbls_ecart_nonnull.ecart_prct.hist(bins=100, log=False)
#format axis
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%.2f" % (x*100))) #pour exprimer en %
ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x))) #pour exprimer en unités
plt.title("Distribution des écarts en Mo")

#set ticks in multiples for both labels
ax.xaxis.set_major_locator(ticker.MultipleLocator(5))  #les ticks majeurs pour les années (toutes les 4 graduations(u))
ax.xaxis.set_minor_locator(ticker.MultipleLocator(0.5))  #les ticks mineurs pour les trimestres (à chaque graduation (u))
#ax.yaxis.set_major_locator(ticker.MultipleLocator(10)) #les ticks majeurs tous les 10 pour l'axe y (toutes les 10 (u)
#ax.yaxis.set_minor_locator(ticker.MultipleLocator(2))  #les ticks mineurs tous les 2 pour l'axe y (toutes les 2 unités)

plt.show()


#boxplot

plt.figure(figsize=(15,4))
ax = sns.boxplot(df_dbls_ecart_nonnull.ecart_prct)

#formatage des graduations
ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x*100))) #pour exprimer en %

#set ticks in multiples for both labels
ax.xaxis.set_major_locator(ticker.MultipleLocator(5))  #les ticks majeurs pour les années (toutes les 4 graduations(u))
ax.xaxis.set_minor_locator(ticker.MultipleLocator(1))  #les ticks mineurs pour les trimestres (à chaque graduation (u))
#ax.yaxis.set_major_locator(ticker.MultipleLocator(10)) #les ticks majeurs tous les 10 pour l'axe y (toutes les 10 (u)
#ax.yaxis.set_minor_locator(ticker.MultipleLocator(2))  #les ticks mineurs tous les 2 pour l'axe y (toutes les 2 unités)

plt.show()


# Sur les 12252 fichiers qui seraient donc de 'vrais doublons', 75% (8400 environ) ont un écart de moins de 120% avec la taille moyenne. 
# Et sur la distribution (1er schéma), on voit que 4000 sont entre 0 et 25%.
# 
# #### CONCLUSION :
# 
# 12252 vrais doublons
# Parmi eux, 8400 sont à vérifier (écart < 25%).
# 

# In[307]:


i=df_dbls.index[100]
mylist = df_dbls.listeIds_size[i]
sum([mylist[i]['size'] for i in range(len(mylist))])


# In[312]:


#-------------------------------------------------------------------------------
#5c) taille des doublons : somme totale des documents par fichiers en doublon
#-------------------------------------------------------------------------------
import tqdm

cpt_err=0
list_err=[]
#on travaille avec df_dbls
#pour chaque ligne 
for i in tqdm.tqdm(df_dbls.index):
    
    #récupérer la liste des sizes (on ne se préoccupe pas de savoir s'il y a une size absente ou non... que 26...0 si rien)
    mylist = df_dbls.loc[i, 'listeIds_size']
    #calcul somme
    try:
        var_somme = sum([mylist[i]['size'] for i in range(len(mylist))])
    except:
        cpt_err+=1
        #print('erreur {:>2} on {}-{}'.format(cpt_err,i, mylist))
        list_err.append("erreur "+str(cpt_err)+" on "+str(i)+str(mylist))
        var_somme = 0
        
    df_dbls.loc[i, 'size_somme'] = var_somme        

#print les erreurs
for i in range(len(list_err)):
    print(list_err[i])

print("\n--done--")


# In[329]:


#distribution des sommes de tailles par fichier

plt.figure(figsize=(15,2))
ax=df_dbls.size_somme.hist(bins=100)
#formatage des graduations
ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000))) #pour exprimer en Mo
plt.title("distribution des sommes en Mo)")
plt.show()

plt.figure(figsize=(15,2))
ax=df_dbls[df_dbls.size_somme<=1000000000].size_somme.hist(bins=100, log=True)
#formatage des graduations
ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000))) #pour exprimer en Mo
ax.yaxis.set_major_formatter(FuncFormatter(lambda y, pos:"%d" % (y))) #pour exprimer en unités
plt.title("distribution des sommes en Mo pour les sommes <= 1Go")
plt.show()


plt.figure(figsize=(15,2))
ax=df_dbls[df_dbls.size_somme>1000000000].size_somme.hist(bins=100)
#formatage des graduations
ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000))) #pour exprimer en Mo
plt.title("distribution des sommes en Mo pour les sommes > 1Go")
plt.show()


plt.figure(figsize=(15,2))
ax=df_dbls[df_dbls.size_somme<20000].size_somme.hist(bins=100)
#formatage des graduations
ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000))) #pour exprimer en Ko
plt.title("distribution des sommes en Ko pour les sommes < 20 Mo")
plt.show()


# CONCLUSION 
# 
# - Des sommes par fichier pour la plupart inférieur à 20Mo.
# 
# - Des cas particuliers de fichiers avec des sommes de l'ordre du Go => A VOIR !!! **P6**
# 
# Voyons pour les 'vrais doublons' :
#     

# In[331]:


#distribution des sommes de tailles par fichier pour les 'vrais doublons'

plt.figure(figsize=(15,2))
ax=df_dbls_ecart_nonnull.size_somme.hist(bins=100)
#formatage des graduations
ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000))) #pour exprimer en Mo
plt.title("distribution des sommes en Mo)")
plt.show()

plt.figure(figsize=(15,2))
ax=df_dbls_ecart_nonnull[df_dbls_ecart_nonnull.size_somme<=1000000000].size_somme.hist(bins=100, log=True)
#formatage des graduations
ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000))) #pour exprimer en Mo
ax.yaxis.set_major_formatter(FuncFormatter(lambda y, pos:"%d" % (y))) #pour exprimer en unités
plt.title("distribution 'vrais doublons' des sommes en Mo pour les sommes <= 1Go")
plt.show()


plt.figure(figsize=(15,2))
ax=df_dbls_ecart_nonnull[df_dbls_ecart_nonnull.size_somme>1000000000].size_somme.hist(bins=100)
#formatage des graduations
ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000000))) #pour exprimer en Mo
plt.title("distribution 'vrais doublons' des sommes en Mo pour les sommes > 1Go")
plt.show()


plt.figure(figsize=(15,2))
ax=df_dbls_ecart_nonnull[df_dbls_ecart_nonnull.size_somme<20000].size_somme.hist(bins=100)
#formatage des graduations
ax.xaxis.grid(True, which = 'both', color = 'gray', zorder = 0) 
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos:"%d" % (x/1000))) #pour exprimer en Ko
plt.title("distribution 'vrais doublons' des sommes en Ko pour les sommes < 20 Mo")
plt.show()


# #### CONCLUSION 'VRAIS DOUBLONS' : 
# 
# Les sommes se concentrent sur moins de 20Mo
# 
# **Quelques fichiers avec des sommes de l'ordre du Go !**
# 
# 
# 

# In[332]:


#-------------------------------------------------------------------------------
#5d) taille des doublons : somme totale des documents des fichiers en doublon
#-------------------------------------------------------------------------------
print("somme totale des size de fichiers en doublon : {:>3.0f} Go.".format(df_dbls.size_somme.sum()/1000000000))
print("dont {:>3.0f} Go pour les images et".format(df_dbls[df_dbls.doctype_grpd=='image'].size_somme.sum()/1000000000))
print("     {:>3.0f} Go pour les vidéos.".format(df_dbls[df_dbls.doctype_grpd=='video'].size_somme.sum()/1000000000))

print("\nCalcul sur les 'vrais doublons':\n")
print("somme totale des size de fichiers en doublon : {:>3.0f} Go.".format(df_dbls_ecart_nonnull.size_somme.sum()/1000000000))
print("dont {:>3.0f} Go pour les images et".format(df_dbls_ecart_nonnull[df_dbls_ecart_nonnull.doctype_grpd=='image'].size_somme.sum()/1000000000))
print("     {:>3.0f} Go pour les vidéos.".format(df_dbls_ecart_nonnull[df_dbls_ecart_nonnull.doctype_grpd=='video'].size_somme.sum()/1000000000))


# 
# CONCLUSION :
# 
# 73 Go peuvent être 'économisés'. A voir avec les documents 'master' (déduire leur taille) : 
# 
# nb : en fait, il n'y a pas l'info 'master' comme je le pensais. On va donc prendre la taille moyenne :
# 

# In[333]:


#-------------------------------------------------------------------------------
#5e) taille des doublons : somme totale des documents master des fichiers en doublon
#nb : pas 'master' dispo donc on prend la taille moyenne.
#-------------------------------------------------------------------------------
print("somme totale des moyennes de size des fichiers en doublon : {:>3.0f} Go.".format(df_dbls.size_moyenne.sum()/1000000000))
print("dont {:>3.0f} Go pour les images et".format(df_dbls[df_dbls.doctype_grpd=='image'].size_moyenne.sum()/1000000000))
print("     {:>3.0f} Go pour les vidéos.".format(df_dbls[df_dbls.doctype_grpd=='video'].size_moyenne.sum()/1000000000))

print("\nCalcul sur les 'vrais doublons':\n")
print("somme totale des moyennes de size des fichiers en doublon : {:>3.0f} Go.".format(df_dbls_ecart_nonnull.size_moyenne.sum()/1000000000))
print("dont {:>3.0f} Go pour les images et".format(df_dbls_ecart_nonnull[df_dbls_ecart_nonnull.doctype_grpd=='image'].size_moyenne.sum()/1000000000))
print("     {:>3.0f} Go pour les vidéos.".format(df_dbls_ecart_nonnull[df_dbls_ecart_nonnull.doctype_grpd=='video'].size_moyenne.sum()/1000000000))


# #### CONCLUSION : (Chiffres à mettre à jour avec la mise à jour du catalogue)
# 
# Au maximum (si ce sont tous des doublons), on économiserait : 508-136 = 372 Go
# 
# Et sur uniquement les 'vrais doublons', on économiserait    :  73- 30 =  43 Go
# 
# => **Un écart très important (x10) qui implique de bien distinguer les faux de vrais doublons.**
# 
# => Une économie de 43Go minimum.
# 
# => **La copie de tous les fichiers sans distinction coûterait donc au 372 Go de plus (au maximum).**
# 
# 
# 

# In[380]:


#--------------------------------------------------------------------------------------------
#----------------------------------------------
# II - Focus sur les fichiers avec doublon sans size - vérification de l'hypothèse H1
#--------------------------------------------------------------------------------------------
#----------------------------------------------
#les mêmes indicateurs sur :
df_dbls_size_absente = df_dbls[df_dbls.flag_size_absente==True]
print("il y a {:>3} fichiers avec size absente.".format(df_dbls_size_absente.shape[0]))

print("\nVérification des noms et de la liste des size sur ces fichiers:\n")
df_dbls_size_absente[['listeIds_size','filename_grpd','list_sizes','nb_distinct_sizes','nb_doc']]


# CONCLUSION SUR LES FICHIERS AVEC SIZE ABSENTE : 
# 
# Types : 
# - Il s'agit de fichiers .svg pour la plupart puis .jpg et .png.
# 
# Noms :
# - les noms ne sont pas ceux de photos prises par téléphone ou appareil.
# 
# Nombre de documents : 
# - le max est de 2 documents

# In[ ]:





# In[373]:


#--------------------------------------------------------------------------------------------
#----------------------------------------------
# III - Vérification des hypothèses H2 et H4
#--------------------------------------------------------------------------------------------
#OBJECTIF : COMMENT TRAITER LES DOUBLONS ? COMMENT MINIMISER LE TRAITEMENT MANUEL, LE RENDRE LE PLUS RAPIDE POSSIBLE.
#----------------------------------------------
#=> **Hypothèse H2 : les fichiers en doublons avec 2 docs sont de vrais doublons. On pourra garder un seul élément.**

#Si ce sont de vrais doublons, alors les tailles récupérées de chacun des 2 documents sont les mêmes : 
#on va vérifier que le nombre de sizes distinctes récupérées est = 1
df_dbls_nbdoc2 = df_dbls[df_dbls.nb_doc==2]

df_dbls_nbdoc2.nb_distinct_sizes.value_counts()


# 1715 avec 2 sizes           : donc 2 fichiers et 2 sizes distinctes. Ceux-là peuvent être classés parmi les faux doublons.
# 
# Les 28715 avec 1 seule size : il s'agit des cas où les tailles des deux documents sont identiques. 
# 
# **Ces fichiers peuvent être classés parmi les vrais doublons** (A valider 'visuellement' + tard).
# 
# Les 9 avec 0 distinct_sizes : il s'agit des 9 documents sans size (cf. plus haut). 
# **A classer parmi les vrais doublons.**
# 
# 
# CONCLUSION : H2 est 'presque' vraie :
# 
# - les 1715 (faux doublons) sont à conserver et étudier (calcul de la taille)
#   **ils devront être détopés 'doublons'**
# 
# - **les 27715 ('vrais doublons') sont à conserver et devront être vérifiés (doublons ou pas)**
# 
# - les 9 sans size : vrais doublons à vérifer, mis à part.

# In[406]:


# ON ENREGISTRE LES 'VRAIS DOUBLONS' (dont ceux 'size absente' à part)

df_dbls_vrais_nbdoc2 = df_dbls_nbdoc2[df_dbls_nbdoc2.nb_distinct_sizes==1]

df_dbls_vrais_sizeabste = df_dbls_nbdoc2[(df_dbls_nbdoc2.nb_distinct_sizes==0)]

print("vrais doublons sur les doc2 et sizeabste :", df_dbls_vrais_nbdoc2.shape, df_dbls_vrais_sizeabste.shape)


# In[405]:


# ON ENREGISTRE LES 'FAUX DOUBLONS'

df_dbls_faux_nbdoc2 = df_dbls_nbdoc2[df_dbls_nbdoc2.nb_distinct_sizes>1]


print("faux doublons sur les doc2 : ",df_dbls_faux_nbdoc2.shape)


# Vérification des hypothèses :
#     
#    **Hypothèse H3 : les fichiers +2 doc ne sont pas de vrais doublons. Il va falloir trouver un découpage plus fin.**
# 
#    **Hypothèse H4 : les fichiers avec au moins 2 documents et 1 seule size sont de vrais doublons.**
#    
#    
# On a déjà vu pour ceux avec 2 documents. Voyons pour les autres :
# 

# In[ ]:





# In[402]:


#On définit le df des fichiers en doublon avec + de 2 documents 
#(remarque : size est renseignée pour tous.cf. size absente plus haut)
df_dbls_nbdocsup2 = df_dbls[df_dbls.nb_doc>2]

print("il y a {} fichiers 'doublons' avec plus de 2 documents".format(df_dbls_nbdocsup2.shape[0]))

#Voyons le nombre de sizes distinctes = 1 => de 'vrais doublons' 
print("\nil y a {:>6} fichiers où le nombre de size distinctes = 1."      .format(df_dbls_nbdocsup2[df_dbls_nbdocsup2.nb_distinct_sizes==1].shape[0]))
print("il s'agit très probablement de 'vrais doublons'. A vérifier 'visuellement'")
print("ils sont ajoutés avec ceux nb_doc=2.")

#ajout des fichiers dans le df des 'vrais doublons'
df_dbls_vrais_nbdocsup2 = df_dbls_nbdocsup2[df_dbls_nbdocsup2.nb_distinct_sizes==1]

df_dbls_vrais = pd.concat([df_dbls_vrais_nbdoc2, df_dbls_vrais_nbdocsup2])

#compte de vrais
print("\n(remarque : il y a actuellement {} fichiers 'vrais doublons'.)".format(df_dbls_vrais.shape[0]))
                          


# In[414]:


#Cas des +2 doc avec size distinctes > 1
print("Cas des +2 doc avec size distinctes > 1 :")
print("il s'agit probablement de faux doublons")
print("on les conserve avec les autres pour détopage plus tard.")

df_dbls_faux_nbdocsup2 = df_dbls_nbdocsup2[df_dbls_nbdocsup2.nb_distinct_sizes>1]
print("il y a {} fichiers 'doublons' avec plus de 2 doc et nb size <> est > 1".format(df_dbls_faux_nbdocsup2.shape[0]))

#ajout des fichiers dans le df des 'faux doublons'
df_dbls_faux = pd.concat([df_dbls_faux_nbdoc2, df_dbls_faux_nbdocsup2])

#compte de faux
print("\n(remarque : il y a actuellement {} fichiers 'faux doublons'.)".format(df_dbls_faux.shape[0]))


# In[449]:


#EVALUATION DE LA MEMOIRE CONSERVEE OU AJOUTEE SELON LES VRAIS ET FAUX DOUBLONS

#CAS DES VRAIS DOUBLONS
#les vrais doublons peuvent être enregistrés seulement sur le master. 
#On va tenir compte de la taille moyenne (qui est celle du master donc d'après la détermination des 'vrais' cf. plus haut).
#et on va la déduire du total.

#0) calcul du nombre de fichiers et documents
var_nb_master = df_dbls_vrais.shape[0]
var_nb_docs   = df_dbls_vrais.nb_doc.sum()

#1) calcul du total de size pour les vrais doublons
var_sum = df_dbls_vrais.tot_size.sum()

#2) Calcul du total des moyennes des vrais doublons
var_master_sum = df_dbls_vrais.size_moyenne.sum()

#3) Calcul de la mémoire 'économisée'
var_size_saved = var_sum - var_master_sum

mydiv=1000000000
print("Vrais doublons : ")
print("Total size des documents                : {:.0f} Go pour {:>6} documents".format(var_sum/mydiv, var_nb_docs))
print("Total size des fichiers master          : {:.0f} Go pour {:>6} 'master'".format(var_master_sum/mydiv, var_nb_master))
print("Total volume 'économisé'                : {:.0f} Go".format(var_size_saved/mydiv))


#CAS DES FAUX DOUBLONS
#les faux doublons devront être splités. Peut-être pas complètement ! (Cela reste à voir ici) 
#Ici on fait comme si on splittait tout.

#0) calcul du nombre de fichiers et documents
var_nb_master = df_dbls_faux.shape[0]
var_nb_docs   = df_dbls_faux.nb_doc.sum()

#1) calcul du total de size pour les faux doublons
var_sum = df_dbls_faux.tot_size.sum()

#2) Calcul du total des moyennes des vrais doublons 
#(pour une estimation de la taille du master - on n'a pas l'indicateur master ici)
var_master_sum = df_dbls_faux.size_moyenne.sum()

#3) Calcul de la mémoire 'non économisée'
var_size_saved = var_sum - var_master_sum

mydiv=1000000000
print("\nFaux doublons : ")
print("Total size des documents                : {:>3.0f} Go pour {:>6} documents".format(var_sum/mydiv, var_nb_docs))
print("Total 'size moyenne' des fichiers master: {:>3.0f} Go pour {:>6} 'master'".format(var_master_sum/mydiv, var_nb_master))
print("Total volume 'non économisé'            : {:>3.0f} Go".format(var_size_saved/mydiv))



#STATS VRAIS ET FAUX CUMULEES

#0) calcul du nombre de fichiers et documents
var_nb_master = df_dbls_vrais.shape[0] + df_dbls_faux.shape[0]
var_nb_docs   = df_dbls_vrais.nb_doc.sum() +  df_dbls_faux.nb_doc.sum()

#1) calcul du total de size
var_sum = df_dbls_vrais.tot_size.sum() + df_dbls_faux.tot_size.sum()

#2) Calcul du total des size moyennes par fichier
#(pour une estimation de la taille du master - on n'a pas l'indicateur master ici)
var_master_sum = df_dbls_vrais.size_moyenne.sum() + df_dbls_faux.size_moyenne.sum()

mydiv=1000000000
print("\nCumul Vrais/Faux doublons : ")
print("Total size des documents                : {:>3.0f} Go pour {:>6} documents".format(var_sum/mydiv, var_nb_docs))
print("Total 'size moyenne' des fichiers master: {:>3.0f} Go pour {:>6} 'master'".format(var_master_sum/mydiv, var_nb_master))

print("\nNB : les 'vrais doublons' sont tout de même à valider 'visuellement'")


# ### STATISTIQUES FINALES (corrections) :
# 
# #En fait, pour les faux doublons, il faut considérer nb_doc et nb_size
# #on sait que 9 n'ont pas de size
# #si nb_doc = nb_size, on peut les compter dans les vrais doublons
# #pour le reste : il faut faire des regroupements par size:
# 
#                nb_doc  nb_size
# f1    id1 s1     3        2
#       id2 s1
#       id3 s2
#         
#         
# Dans ce cas :
# f1 va être splitté en 2 :
# f1    taille s1
# f1bis taille s2
# => la mémoire utile pour f1 est s1 + s2
# 
# En général, dans le cas des doublons.
# La mémoire utile est pour chaque fichier : la somme des size distinctes
# Le nombre de fichier en sortie           : le nombre de size distinctes.
# 
# 
# Ainsi : 
# 
# 
# 

# In[521]:



#CAS DES DOUBLONS

#0) calcul du nombre de fichiers et docs topé doublons
var_nb_master = df_dbls.shape[0]
var_nb_docs   = df_dbls.nb_doc.sum()

#1) calcul du nombre de fichiers qui seront conservés = cumul du nb de size différentes par fichier
var_nb_files_conserved = df_dbls.nb_distinct_sizes.sum()

#2) Calcul du volume total minimal qui sera nécessaire (en ne gardant que le master) = cumul des size distinctes
var_vol_total_min = df_dbls.list_sizes.apply(lambda x: sum(x)).sum()

mydiv=1000000000
print("\nStats finales sur les doublons (sans distinction vrai/faux): ")
print("Total fichiers doublons forcément conservés (~'master') (A): {:>3.0f} Go pour {:>6} documents topés doublons.".      format(var_vol_total_min/mydiv, var_nb_files_conserved))
print("")
print("sur (rappel):")
print("\nCumul Vrais/Faux doublons : ")
print("Total size des documents                 (B)   : {:>3.0f} Go pour {:>6} documents".format(var_sum/mydiv, var_nb_docs))
print("Total 'size moyenne' des fichiers master (C)   : {:>3.0f} Go pour {:>6} 'master'".format(var_master_sum/mydiv, var_nb_master))

print("\nNB : les 'vrais doublons' sont tout de même à valider 'visuellement'")

print("\nCes calculs sont ok sous l'hypothèse que :")
print("    si la taille est différente, alors il ne s'agit pas des mêmes fichiers -> à voir...")
print("    si la taille est identique, alors ce sont les mêmes fichiers -> probable mais à valider")

var_nb_doc_non_doublons = df_par_fichier[df_par_fichier.nb_doc==1].shape[0]
var_vol_non_doublons    = df_par_fichier[df_par_fichier.nb_doc==1].list_sizes.apply(lambda x: sum(x)).sum()
#il suffit aussi de cumuler size puisqu'il n'y a qu'un seul doc...
print("\nRemarque : documents non doublons (D) : {:>3.0f} Go pour {:>6} docs.".      format(var_vol_non_doublons/mydiv,var_nb_doc_non_doublons))
print("")


# ### CONCLUSION FINALE :
# 
# Il faudra avoir tous les fichiers pour pouvoir valider ou non les doublons.
# Mais, cela peut se faire par étape:
# 
# 1) copie des fichiers non topés doublons                    => 135 Go pour   37004 doc ((D) ci-dessus)
# 
# 2) copie des fichiers topés doublons, uniquement les master => 179 Go pour   85645 documents topés doublons ((A) ci-dessus)
# 
# 3) copie des fichiers topés doublons, hors master (B)-(A)   => 339 Go pour ~240000 documents 
# 
# Donc, à ce stade et cette date, on aurait besoin de :
#                     min : 314 Go pour +12000 docs (1+2).
#                     max : 653 Go au total pour +300000 docs (1+2+3).
# 
# 

# In[519]:


df_par_fichier[df_par_fichier.nb_doc==1].head()


# In[447]:



#============================================================
#============================================================
#============================================================
#============================================================
#============================================================
#============================================================
#============================================================
#============================================================


# # Traitement principal

# In[183]:


#REMARQUE : A VOIR !!!!!
# https://stackoverflow.com/questions/7571635/fastest-way-to-check-if-a-value-exists-in-a-list


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

