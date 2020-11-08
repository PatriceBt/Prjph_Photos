#!/usr/bin/env python
# coding: utf-8

# In[5]:


#imports

import os
from os import listdir
from os.path import isfile, join, isdir

from PIL import Image, IptcImagePlugin
import numpy as np
from matplotlib import pyplot as plt # pour imgshow

import pandas as pd

import datetime as dt #pour timedelta
from datetime import datetime

import tqdm

import pymongo
from pymongo import MongoClient
#pour les images
import PIL.Image
import gridfs
#pour les images webp
#!pip3 install webptools  #webp... et pas web...
from webptools import dwebp
#pip install opencv-python (pour cv2)
#pour les vidéos
import cv2
import subprocess

import sys


# ## Création des fonctions de lecture du catalogue et d'insertion dans la base

# In[30]:


def fnph_copy_file(parm_filename, parm_sourcename, parm_destname):
    
    import os
    import shutil

    #print("debug-->", "parm_filename",  parm_filename)
    #print("debug-->", "parm_sourcename",parm_sourcename)
    #print("debug-->", "parm_destname",  parm_destname)
    """
    Fonction de copie de fichier
    retourne un code status et un commentaire
    """
    myfile_source_fullname      = os.path.join(parm_sourcename, parm_filename)
    myfile_destination_fullname = os.path.join(parm_destname,   parm_filename)

    #print("debug-->", "myfile_source_fullname",      myfile_source_fullname)
    #print("debug-->", "myfile_destination_fullname", myfile_destination_fullname)

    
            #------------------#    
            # test de présence #
            #------------------#    
    
    if os.path.exists(myfile_destination_fullname): #ajout du test car shutil semble ne pas recopier (dcreation inchangée) 
                                                    #mais ne renvoie pas d'erreur (^donc n'écrase pas)
        status   = "ko-doublon"
        comment  = "Ce nom de fichier existe déjà dans le répertoire de copie" 
        print("Debug-->doublon: parm_filename=", parm_filename)
    else:
    
        try: 
            #------------------#    
            # copie du fichier #
            #------------------#    
            myfile_destination_created = shutil.copy2(myfile_source_fullname, myfile_destination_fullname) 
            status="ok"
            comment="File copied successfully."
            #print("Debug-->File copied successfully.") 
            #print('Debug-->done - file', myfile_destination_created, 'created.')


        # If source and destination are same 
        except shutil.SameFileError: 
            status="ko-other"
            comment="Source and destination represents the same file."
            print("\nSource and destination represents the same file.")
            print("------")
            print("myfile_source_name:")
            print(myfile_source_name)
            print("myfile_destination_fullname:")
            print(myfile_destination_fullname)
            print("------")

        # If there is any permission issue 
        except PermissionError: 
            status="ko-other"
            comment="Impossible to write. Permission denied."
            print("Permission denied.") 

        # For other errors 
        except: 
            status="ko-other"
            comment="Error occurred while copying file."
            print("Error occurred while copying file.") 
            #print("Unexpected error:", sys.exc_info()[0])
            #raise

    return[status, comment, myfile_source_fullname, myfile_destination_fullname]


# In[31]:


#test def fnph_copy_file
import shutil
import os
parm_fname="Capturefondbl.JPG"
parm_sourcename=r"C:\Users\LENOVO\Documents\Projets\Prj_photos\Prjph_repertoire_test\Test_fonction_copie_source"
parm_destname  =r"C:\Users\LENOVO\Documents\Projets\Prj_photos\Prjph_repertoire_test\Test_fonction_copie_destination"

#parm_fname="Captureph1.PNG"
#parm_sourcename=r"C:\Users\LENOVO\Documents\Projets\Prj_photos\Prjph_repertoire_test\test_shutil"
#parm_destname  =r"C:\Users\LENOVO\Documents\Projets\Prj_photos\Prjph_repertoire_test\test_shutil\copies"

print(fnph_copy_file(parm_fname, parm_sourcename, parm_destname))
print("done")


# ## Programme principal

# In[34]:


get_ipython().run_cell_magic('time', '', '#Programme principal\n\n#----------------\n#initialisations\n#----------------\n\n#------ Répertoire de sauvegarde de l\'historique des répertoires sauvegardés\nparm_df_log_rep = r\'C:\\Users\\LENOVO\\Documents\\Projets\\Prj_photos\\Prjph_log\'\n\n#Paramètres de connexion\n#------ Base  -------------------*\nclient = pymongo.MongoClient(\'localhost\',27017)\nmydb = client["prjph_catalogue"]\n\n#début Paramètres utilisateur *************************************************************************************\n\n#------ Collection -------------------*      #1<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\nmycollection_name = "test_documents"        #TEST\n#mycollection_name = "images_videos"          #PREPROD\n#------ ---------- -------------------*\nmycollection = mydb[mycollection_name]\n\n#------ Répertoire initial à copier ------*  #2<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\nparm_source_dir_name= r"C:\\Users\\LENOVO\\Documents\\Projets\\Prj_photos\\Prjph_repertoire_test\\Test_fonction_copie_source" \n\n#------ Répertoire destinataire des fichiers copiés -----*\nparm_dest_dir_name  = r"C:\\Users\\LENOVO\\Documents\\Projets\\Prj_photos\\Prjph_repertoire_test\\Test_fonction_copie_destination" #0b<<<<\n\n#------ Libellé du traitement ------------*            #3<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\nmontitre_traitement  = "Copie de fichiers - "          #Est systématiquement complété plus bas par le nom du rep à copier\nmontitre_traitement += "(" + parm_source_dir_name +")"\n#fin Paramètres****************************************************************************************************\n\n#--------------------------------\n#Tests de contrôle des fichiers\n#--------------------------------\n\n#test l\'existence du répertoire de sauvegarde du log avant de démarrer\nif not os.path.exists(parm_df_log_rep):\n    print(\'ERREUR : le répertoire de sauvegarde n\'\'existe pas\')\n    print(\'ERREUR : \' + parm_df_log_rep)\n    print(\'ERREUR : veuillez vérifier et relancer\')\n    sys.exit("haa! errors! vérifier le nom du répertoire du log en paramètre et relancer")\n\n#test l\'existence du répertoire de destination des copies\nif not os.path.exists(parm_dest_dir_name):\n    print(\'ERREUR : le répertoire de copie n\'\'existe pas\')\n    print(\'ERREUR : \' + parm_dest_dir_name)\n    print(\'ERREUR : veuillez vérifier et relancer\')\n    sys.exit("haa! errors! vérifier le nom du répertoire de destination des copies et relancer")\n\n#--------------------------------\n#df historique des répertoires traités\n#--------------------------------\n\n\n#--------------------------------\n#Compteurs - indicateurs\n#--------------------------------\n#nombre de fichiers copiés\ncpt_nb_f_copy_ok     = 0\n#nombre de fichiers à copier mais non copiés\ncpt_nb_f_copy_failed = 0\n\n\n#global_unique_id : Variable de valeur unique pour rsgner unique_id lors des différents appels\nglobal_unique_id_time_format = datetime.now()\nglobal_unique_id             = str(datetime.timestamp(global_unique_id_time_format))\nprint(\'\\n           global_unique_id:\', global_unique_id,\'\\n\')\n\n\n#-----------------------------------\n# Displays de début de traitement\n#-----------------------------------\nstart_time = datetime.now()\nprint(\'------------------------------------------------------------\')\nprint(\'start......................... : \', start_time,         )\nprint(\'collection.................... : \', mycollection_name   )\nprint(\'répertoire destination........ : \', parm_dest_dir_name  )\n#----------------------------------------------------------------------------\n#Connection au serveur mongodb 27017\n#----------------------------------------------------------------------------\nclient = pymongo.MongoClient(\'localhost\',27017)\nmydb = client["prjph_catalogue"]\n\n#----------------------------------------------------------------------------\n#Récupération de la liste des répertoires en base (requête -> fichier)\n#----------------------------------------------------------------------------\n    #--------------------------------------------------\n    #version actuelle : pas effectué\n    #--------------------------------------------------\n\n#----------------------------------------------------------------------------\n#Lecture de la liste des répertoires en base (fichier) et affichage\n#----------------------------------------------------------------------------\n    #--------------------------------------------------\n    #version actuelle : pas effectué\n    #--------------------------------------------------\n\n#----------------------------------------------------------------------------\n#Choix du répertoire à copier\n#----------------------------------------------------------------------------\n    #--------------------------------------------------\n    #version actuelle : pas effectué\n    #--------------------------------------------------\n\nprint(\'directory asked to be copied.. : \', parm_source_dir_name,      \'\\n\')\n\n#----------------------------------------------------------------------------\n#Détermination du critère de recherche des fichiers à copier (requête -> cursor)\n#----------------------------------------------------------------------------\n#le caractère spécial \\ dans le nom de répertoire doit être doublé \\\\ pour être considéré comme \\ non spécial\nmyquest = parm_source_dir_name.replace("\\\\", "\\\\\\\\")  #nb : dans cette écriture, on double également le car. spécial\n                                                      #     pour dire que \\ doit être remplacé par \\\\\n\n#puis ajout de "^" pour spécifier que le nom doit commencer par... et pour obtenir tous les sous-répertoires\nmyquest = "^" + myquest\nprint(\'critère de recherche (myquest) : \', myquest)\n\n#----------------------------------------------------------------------------\n#Création du curseur des fichiers à copier (requête -> cursor)\n#----------------------------------------------------------------------------\n#cursor des fichiers à sauvegarder\nmycursor_files = mycollection.find({"orgnl_dirname":{"$regex":myquest}})\n\n#----------------------------------------------------------------------------\n#Statistiques : nombre de fichiers dans le répertoire\n#----------------------------------------------------------------------------\nnb_files_in_rep = mycursor_files.count()\nprint(\'nb of files present in dir.... : \', nb_files_in_rep,           \'\\n\')\n\n#----------------------------------------------------------------------------\n#Statistiques : nombre de fichiers à copier\n#----------------------------------------------------------------------------\n    #--------------------------------------------------\n    #version actuelle : pas calculé\n    #--------------------------------------------------\n\n#----------------------------------------------------------------------------\n#Boucle sur les fichiers à copier\nfor f in mycursor_files:\n\n#---Vérification du top copie (déjà copié ?)\n    flag_f_already_copied=False\n    \n    #--------------------------------------------------\n    #version actuelle : tous les fichiers sont copiés\n    #--------------------------------------------------\n    \n#---Cas déjà copié : ajout comment -> already copied + id_unique\n    if flag_f_already_copied:\n        print(f.name + "already copied")\n#---Cas pas copié  : lecture / copie\n    else:\n#------ copie via la fonction\n        result = fnph_copy_file(f["filename"], f["orgnl_dirname"][0], parm_dest_dir_name) #[status, comment, source, dest]\n        status = result[0]\n\n        if status == "ok":\n            cpt_nb_f_copy_ok     += 1\n            #------------------------------------------------------------#\n            # Mise à jour du document db copié  \n            #------------------------------------------------------------#\n                #--------------------------------------------------\n                #version actuelle : non effectué\n                #--------------------------------------------------           \n                \n        else:\n            cpt_nb_f_copy_failed += 1\n            #------------------------------------------------------------#\n            # Mise à jour du document db non copié\n            #------------------------------------------------------------#\n                #--------------------------------------------------\n                #version actuelle : non effectué\n                #--------------------------------------------------           \n\n            \n#-----------------------------------\n# Displays de fin de traitement\n#-----------------------------------\nend_time = datetime.now()\nprint(\'ended......................... : \', end_time)\nprint(\'nb de fichiers à copier....... : \', \'non calculé\')\nprint(\'nb de fichiers copiés......... : \', cpt_nb_f_copy_ok)\nprint(\'nb de fichiers copie ko....... : \', cpt_nb_f_copy_failed)\nprint(\'------------------------------------------------------------\')\n\n\n\n\n#Programme principal fin\n')


# In[ ]:





# In[ ]:




