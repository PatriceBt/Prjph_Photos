#!/usr/bin/env python
# coding: utf-8

# # Création du catalogue de photos
# 
# ## Recherche de photos dans l'arbordescence sous-jacente au répertoire indiqué par l'utilisateur.
# 
# ### Objectif : avoir un outil permettant de référencer les photos et d'obtenir des statistiques :
# 
# - permettre une sauvegarde des photos dans un dd unique (les dossiers d'origine sont conservés) tout en permettant
# de sauvegarder et 
# 
# - référencer d'éventuelles nouvelles photos sans avoir à tout référencer à nouveau (celles déjà
# référencés seront topées 'déjà présentes'
# 
# - obtenir des dossiers (albums) constitués de copies de photos par classement souhaité (doublons possibles).
# 
# Stats :
# - nombre d'images par année, mois, jour
# - nombre de doublons
# - nombre de photos par sujet
# 
# 
# 
# ### A voir : 
# 
# la gestion du réseau (paramétrages sécurisés)
# 
# la taille de l'image stockée si stockée. A réduire éventuellement.
# 
# 
# ### Priorités :
# 
# La fonction de copie
# La création d'albums en fonction des dates.
# La sauvegarde d'albums sur clés usb (par années, par personne, par lieu).
# 
# 
# 
# ### Prochains points : 
# 
# - Copie des fichiers catalogués dans le matériel de stockage qui aura été choisi.
# 
# - Vérifier la récupération de la date de fichier : 
# 
#     - Récupérer en priorité la date 'Origine/Prise de vue' des données exif (?) puis st_mtimt, notamment pour les jpg (si modif sur iPhone, alors la valeur est différente de la date de dernière modification (st_mtime).
# 
# 
# - Ajout d'un appel pour récupérer le nombre total de répertoires dans l'arboresence du répertoire initial
#   afin de pouvoir avoir une estimation de la durée de traitement.
#   
# - Ajout du nombre de fichiers lus (qqsoit le type de fichier) (pour avoir une idée de l'écart)
# 
# - Notification des traitements sur cahier au fur et à mesure.    
# 
# - Calcul du nombre de fichier et du volume mémoire avec la requête ci-dessus
# 
# - Mise sous forme dataFrame de la base et sauvegarde en csv (pour manips sur Excel)
# 
# - Stats sous Excel
# 
# - Fin
# 
# Le volume global va servir à étudier les solutions de stockage des copies.
# 
# 
# #### Points traités :
# 
# - Identification de la donnée 'taille mémoire' de l'image (pour calcul du volume global) ----------------OK
# 
# - Calcul de la taille globale : Requête de calcul de la somme des tailles mémoire -----------------------OK
# 
# - Lancement d'une requête mongodb sur la bd via python et récupération du résultat dans une variable ----OK
# 
# - Définition de la liste des extensions 'image' et 'vidéo' ----------------------------------------------OK
# 
#     - Ajout d'un document 'log' qui contient ------------------------------------------------------------OK
#     - un titre illustrant le traitement lancé -----------------------------------------------------------OK
#     - la date -------------------------------------------------------------------------------------------OK
#     - le répertoire racine paramétré --------------------------------------------------------------------OK
#     - la liste des répertoires explorés (faire un df au fur et à mesure du print) -----------------------OK
#     
#   (utilité : avoir l'historique des répertoires consultés pour éviter de les refaire en cas de doute)
# 
# 
# 
# ### A venir :
# 
# Fonctions à venir : 
# COPIE - GESTION DES DOUBLONS - RECHERCHE - CLASSEMENT - CREATION D'ALBUMS SELON CRITERES - RECONNAISSANCE
# 
# ajout de la sauvegarde d'une vignette de l'image dans la bd
# ajout du référencement des vidéos (=> ajout des types vidéos)
# faire le point sur les types de fichiers image et vidéo
# 
# penser à faire un test (copie des répertoires photo, avec écriture bloquée)
# 
# obtenir la liste des répertoires de photos avant la copie (pour les mettre en écriture interdite)
# 
# ajout de la fonction COPIE:
#     même fonctionnement mais avec en plus une copie dans un dossier spécifé
#     + mise à jour de la bd (ajout événement copie / création enreg avec deux événements - insert et copie)
#     
# ajout de la fonction de gestion des doublons
#     à préciser
#     
# définir le lieu de stockage de la bd (cf. volume important si images stockées)
# 
# 
# ### Réalisé
# 
# 21/10 : Les statistiques quantités et volumes par année, mois, jour, catégories, types de fichier (notebook statistiques).
# 
# 15/10 : création réseau. Accès au répertoire partagé.
# 
# test sur le répertoire partagé
# 
# ajout du stockage de l'image
# 
# annulation du stockage de l'image - pourra être effectué ultérieurement.
# 
# la correction de la récup des données exif (cf. en cours)
# 
# le traitement de la donnée exif 37500, bytes très longue. A priori, le pop ne fonctionne pas.
# 
# Répertoire et types de fichiers entrés en paramètres
# 
# appel à la fonction prjph_listdirectory : récupération dans un dictionnaire des noms des fichiers avec os.listdir contenus dans le répertoire passé en paramètre et de type passé en paramètre également.
# 
# Avec pour chaque fichier :
# 
#     - récupération et ajout dans le dictionnaire des métadonnées exif avec PIL getexif (via fnph_getexif)
#       (avec suppression des données 37500, 37510 et 59932 car trop longues et a priori non utiles
#       , et suppression des clés dictionnaires)
# 
#     - récupération et ajout dans le dictionnaire des stats par os.stats avec conversion en dicitonnaire (via fnph_getstats)
#     
# Mise à jour de la base mongo avec les fichiers contenus dans la liste obtenue :
#     
#     - recherche de la présence du fichier dans la base (par appel à la fonction prjph_alreadyexists
#     
#     - si le fichier n'est pas déjà présent : écriture des éléments du dictionnaire de données liées au fichier en cours 
#     
#     - sinon, mise à jour du document dans la base par ajout d'un champ événement (identifiant unique créé avec la date)
#     
# 
# 
#    
# 
# ### Fonctions cibles :
# 
# - Demande de répertoire de recherche à l'utilisateur
# 
# - Recherche d'images dans le répertoire indiqué et l'arborescence sous-jacente
#     format sélectionné : img, jpeg
#     
# - Pour chaque image : 
# 
#     - Récupération des caractéristiques de l'image
#     
#         . nom du fichier image
#         . emplacement d'origine (chemin)
#         . matériel (nom du disque dur)
#         . date de prise de vue
#         . appareil
#         . date de création du fichier origine
#         
#     - Mise à jour de la base de données :
#     
#         . date de mise à jour
#         . caractéristiques de l'image
# 
#     En cas de présence dans le catalogue du couple (source, fichier) (cas d'un catalogue déjà effectué)
#     
#         . mise à jour de l'instance par ajout de l'événement (référencement ko) 
# 
# 
# - Gestion d'un journal
# 
#     Début de traitement :
#     
#         . Ecriture ligne :
#         
#             - références du traitement
#             - répertoire indiqué par l'utilisateur
#             - répertoire de copie (lieu de stockage de la base de données)
#             
#     Pour chaque copie
#     
#         . Ecriture ligne : 
#         
#             - chemin du répertoire origine
#             - nom du fichier origine
#             - nom fichier sauvegardé (après gestion de doublon)
#             
# 
# Remarques
# sur les extensions image
# https://developer.mozilla.org/fr/docs/Web/Media/Formats/Types_des_images
#     
# sur les extensions video
# https://www.reneelab.fr/extension-video.html
# 

# In[1]:


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


# ## Le code de récupération des fichiers images et référencement dans la base mongodb prjph

# ## Création des fonctions de lecture du catalogue et d'insertion dans la base

# In[2]:


def fnph_getexif(parm_monfichier):
    """
        Fonction de recherche des métadonnées exif des images  -  see https://www.exiv2.org/tags.html pour les définitions
        avec en plus un thumbnail
        Retourne un dictionnaire
        Problème de getexif sur une image de type gif
        => solution mep : exception et sortie dictionnaire indiquant pbl
                          retrait des types ".gif" de la liste des types (extensions)
        Problème PIL.Image.open sur un fichier m6c8282f3.jpg (dans un répertoire ...HTC_121212.bookmark_thumb1)
        => solution mep : exception et sortie dico avec pbl
    """
    bool_img_ok=True
    bool_dict_exif_ok=True
    try:
        img = PIL.Image.open(parm_monfichier)
    except:
        bool_img_ok=False
        bool_dict_exif_ok=False
        msg_err = "Problème ouverture PIL.Image.open"
        
    #données exif
    if bool_img_ok:
        try:
            dict_exif = img._getexif()
        except:
            bool_dict_exif_ok=False
            msg_err = "Données exif non récupérées - problème de format de fichier (gif par exemple) ?"
        
    if bool_dict_exif_ok:
        #initialisation de la liste des données à supprimer (pas directement sinon pbl de taille de dico qui varie...)
        listpop=[]
        try:
            for k,v in dict_exif.items():
                #conversion du type PIL.TiffImagePlugin.IFDRational en string avec ajout du liellé IFDRational
                if type(v)==PIL.TiffImagePlugin.IFDRational:
                    dict_exif[k]='IFDRational '+str(v)
                #et gestion de 42034 (valeur de type tuple avec données Tiff, transformée en string)
                if k == 42034: #éventuellement à remplacer par un test sur type tuple s'il y en a d'autres
                    dict_exif[k]=str(v)
                #je retire les données bytes de plus de 12 de long
                if type(v) == bytes and len(str(v)) > 12:
                    listpop.append(k)

            #Suppression des données 
            for i in range(len(listpop)):
                dict_exif.pop(listpop[i])

        except:
            mytrue=True

        #suppression des clés de type dictionnaire
        listpop=[]  #initialisation de la liste des données à supprimer (pas directement sinon pbl de taille de dico qui varie...)
        try:
            for k,v in dict_exif.items():
                if type(v)==dict:
                    listpop.append(k)
            for i in range(len(listpop)):
                dict_exif.pop(listpop[i])
        except:
            mytrue=True

        #conversion des clés (elles sont toutes en numériques) en string (si le dictionnaire est renseigné)
        if dict_exif != None:
            dict_exif = {str(key):value for (key,value) in dict_exif.items()}
    
    else:
        #Cas où dict_exif._getexif ne fonctionne pas (fichiers .gif par exemple) -> restitution d'un dico 'vide'
        dict_exif = {"msg_err":msg_err}
        
    #ajout du thumbnail dans le dictionnaire exif
    #...
    # A VOIR
    #...
    #size=(100,100)
    #imgth=img.copy()
    #print(imgth)
    #dict_exif['thumbnail']=imgth.thumbnail(size)
    #dict_exif['image']=img

    return dict_exif


# In[3]:


#test de la fonction fnph_getexif

testRepertoire = r"C:\Users\LENOVO\Documents\Projets\Prj_photos\Prjph_repertoire_test\uneimage"

f='JL_091103 019.jpg'
#f='test.jpg'
#f='Captureph1.jpg'
#f='Capturefondbl.JPG'
test=fnph_getexif(join(testRepertoire,f))
len(test)

#for k,v in test.items():
#    print('test-->', k,v,type(v),len(str(v)))
#    if type(v)==PIL.TiffImagePlugin.IFDRational:
#        print('class pil -> conversion en string', str(v))
        


# In[4]:


def fnph_getiptc(parm_dir, parm_file):
    # pip install pillow
    #parm_dir=r'C:\Users\LENOVO\Pictures'
    #parm_file='Capturenb.JPG'
    """
    Recherche les informations iptc d'une image 
    Retourne une liste : 1er élément vrai/faux 2ème:iptc (format dico pour mongo) si ok
    """

    #from PIL import Image, IptcImagePlugin


    im = Image.open(os.path.join(parm_dir, parm_file))
    
    iptc = IptcImagePlugin.getiptcinfo(im)

    if iptc:
        for k, v in iptc.items():
            print("{} {}".format(k, repr(v.decode())))
        print('debug--> IPTC INFO TROUVEES !!!!!!!!!!!!!!!!!!!!!!', parm_file)
        result=[{"getiptc":True ,"iptc":iptc}]
    else:
        #print("debug--> This image has no iptc info")
        result=[{"getiptc":False , "iptc":"no iptc info"}]
        
    return(result)
    # We can user getter function to get values
    # from specific IIM codes
    # https://iptc.org/std/photometadata/specification/IPTC-PhotoMetadata
    #def get_caption():
    #    return iptc.get((2,120)).decode()
    #
    #print(get_caption())


# In[5]:


#Test fnph_getiptc


parm_dir=r'C:\Users\LENOVO\Pictures'
parm_file='Capturenb.JPG'
fnph_getiptc(parm_dir, parm_file)


# In[6]:


def fnph_getstats_file(parm_mondir, parm_monfichier):
    """fonction de recherche des données stats de l'image par os.stats
       avec restitution des données st_ sous forme de dictionnaire
       pour os.stat see https://docs.python.org/2/library/os.html 
       os.stat(path) perform the equivalent of a stat() system call on the given path. 
       
    The return value is an object whose attributes correspond to the members of the stat structure, namely:
        st_mode - protection bits,
        st_ino - inode number,
        st_dev - device,
        st_nlink - number of hard links,
        st_uid - user id of owner,
        st_gid - group id of owner,
        st_size - size of file, in bytes,
        st_atime - time of most recent access,
        st_mtime - time of most recent content modification,
        st_ctime - platform dependent; time of most recent metadata change on Unix, or the time of creation on Windows)

    Changed in version 2.3: If stat_float_times() returns True, the time values are floats, measuring seconds. 
    Fractions of a second may be reported if the system supports that. See stat_float_times() for further discussion.

    On some Unix systems (such as Linux), the following attributes may also be available:
    st_blocks - number of 512-byte blocks allocated for file
    st_blksize - filesystem blocksize for efficient file system I/O
    st_rdev - type of device if an inode device
    st_flags - user defined flags for file

    On other Unix systems (such as FreeBSD), the following attributes may be available (but may be only filled out if root tries to use them):
    st_gen - file generation number
    st_birthtime - time of file creation

    On RISCOS systems, the following attributes are also available:
    st_ftype (file type)
    st_attrs (attributes)
    st_obtype (object type).

    Note The exact meaning and resolution of the st_atime, st_mtime, and st_ctime attributes depend on the operating system 
    and the file system. For example, on Windows systems using the FAT or FAT32 file systems, st_mtime has 2-second resolution,
    and st_atime has only 1-day resolution. 
    See your operating system documentation for details.

    For backward compatibility, the return value of stat() is also accessible as a tuple of at least 10 integers giving the most important (and portable) members of the stat structure, in the order st_mode, st_ino, st_dev, st_nlink, st_uid, st_gid, st_size, st_atime, st_mtime, st_ctime. More items may be added at the end by some implementations.
    The standard module stat defines functions and constants that are useful for extracting information from a stat structure. (On Windows, some items are filled with dummy values.)
    """
    #print('.............<fnph_getstats_file>..............')

    s_obj=os.stat(os.path.join(parm_mondir, parm_monfichier))
    mydicoresult = {k:getattr(s_obj,k) for k in dir(s_obj) if k.startswith("st_")}
    #print('\n',parm_monfichier, ':', mydicoresult)
    return mydicoresult


# In[7]:


def fnph_copy_file(parm_filename, parm_sourcename, parm_destname):
    
    """
    Fonction de copie de fichier
    retourne un code status et un commentaire
    """
    myfile_source_name         = os.path.join(parm_sourcename, parm_filename)
    myfile_destination_name    = os.path.join(parm_destname,   parm_filename)

    
    if os.path.exists(myfile_destination_name): #ajout du test car shutil semble ne pas recopier (dcreation inchangée) 
                                                #mais ne renvoie pas d'erreur
        status = "ko"
        comment = "Existe déjà dans le répertoire " + myfile_destination_name
        
    else:
    
        try: 
            myfile_destination_created = shutil.copy2(myfile_source_name, myfile_destination_name) 
            status="ok"
            comment="File copied successfully."
            print("File copied successfully.") 
            print('done - file', myfile_destination_created, 'created.')


        # If source and destination are same 
        except shutil.SameFileError: 
            status="ko"
            comment="Source and destination represents the same file."
            print("\nSource and destination represents the same file.")
            print("------")
            print("myfile_source_name:")
            print(myfile_source_name)
            print("myfile_destination_name:")
            print(myfile_destination_name)
            print("------")

        # If there is any permission issue 
        except PermissionError: 
            status="ko"
            comment="Impossible to write. Permission denied."
            print("Permission denied.") 

        # For other errors 
        except: 
            status="ko"
            comment="Error occurred while copying file."
            print("Error occurred while copying file.") 

    return[status,comment]


# In[8]:


#test def fnph_copy_file
import shutil
import os
parm_fname="Captureph123.PNG"
parm_sourcename=r"C:\Users\LENOVO\Documents\Projets\Prj_photos\Prjph_repertoire_test\test_shutil"
parm_destname=r"C:\Users\LENOVO\Documents\Projets\Prj_photos\Prjph_repertoire_test\test_shutil\copies"

#print(fnph_copy_file(parm_fname, parm_sourcename, parm_destname))


# In[9]:


def fnph_listDirectory_metadata_copy(parm_directory_name, parm_fileExtDico, parm_db, 
                                     parm_do_copy=False, parm_do_copy_dest_dir_name=""):                                        
    """ 
        Fonction de recherche la liste des fichiers du répertoire parm_directory_name
        
        Pour chaque fichier :
        - vérifie s'il est déjà présent ou non dans la base
        - s'il n'est pas présent dans la base : 
             recherche les metadonnées 
        - copie si souhaitée (présente ou non en base - un event 'copy' indiquera en base le statut de la copie)
        
        Remarques : 
        
        - parm_db est utilisé pour préparer le stockage de l'image par la fonction fnph_getimage
        - parm_fileExtDico est la liste des extensions sous forme de dictionnaire {extension, libellé type de fichier}
          Ce paramètre permet d'indiquer le type de fichier en clair dans doctype
          Le dico est traduit en liste ensuite ici
          C'est la liste qui est transmise aux fonctions appelées ici (elles n'ont pas besoin du label)

        Retour : la fonction retourne une liste de dictionnaire des données à insérer - un item par fichier
                 - avec un indicateur "already_exist_in_db" True/False si le fichier existe ou non dans la base
    """
    #Traitement du dico des types
    #Création de la liste des types
    fileExtList = [ ext for ext,label in parm_fileExtDico.items()]

    #pour la date 
    dnow = datetime.now()
    strtimestamp = str(datetime.timestamp(dnow)).replace('.','_')
    fieldname = 'event_'+global_unique_id.replace('.','_') #on met un identifiant unique pour tous les fichiers du même traitement
   
    #initialisations avant la boucle sur les fichiers du répertoire
    nb_files=0
    listdata=[]
    display1_ok=False
    #------- pour l'affichage ------------------------------------d
    fnph_incremente_GLOBAL_NUM_FLDR()
    prct=GLOBAL_NUM_FLDR/(monRepertoire_totalnb_fldrs+1) #+1 pour compter le répertoire racine également traité
    percentage = "{:.0%}".format(prct) 
    print('{}/{}[{}]Rép:{}'.format(GLOBAL_NUM_FLDR,monRepertoire_totalnb_fldrs+1,percentage,parm_directory_name))
    #------- pour l'affichage ------------------------------------f
    
    #création de la liste des fichiers du répertoire (pour extraction des métadonnées et svg dans bd)
    mylistfile = [myfile for myfile in os.scandir(parm_directory_name) if myfile.is_file()]
    lenlistfile=len(mylistfile)                  #pour print uniquement
    cpt=0                                        #pour print uniquement

    #------------------------------------------------------------------
    #Boucle sur les fichiers de la liste pour recherche des metadonnées
    #       et préparation de l'enregistrement à insérer dans la base (données + description événement)
    #------------------------------------------------------------------
    for f in mylistfile: #ATTENTION : LES ELEMENTS DE mylistfile SONT DES NT.ENTRIES (pas des strings)
        cpt+=1
        fileDict={"data":{},"already_exist_in_db":True} #le dictionnaire qui va contenir les metadonnées du fichier
        f_name=f.name
        f_ext ="."+f_name.split('.')[1].lower()
        #if (str(os.path.splitext(f)[1])).lower() in fileExtList:    #ajout de .lower() pour être insensible à la casse
        if f_ext in fileExtList:    #ajout de .lower() pour être insensible à la casse
            #pour affichage sysout ------------d
            #--pourcentage
            prct=cpt/lenlistfile
            percentage = "{:.2%}".format(prct)
            #--la durée restante :
            nb_totalfiles_treated = fnph_incremente_GLOBAL_NBTOTFILES_TREATED()
            duree_restante        = fnph_duree_restante(start_time, datetime.now(), nb_totalfiles_treated, 
                                                        monRepertoire_totalnb_files) #nombre surestimé puisque tous types confondus
            msgtodisplay1="   <lus/inRep:"+ str(cpt) + "/" + str(lenlistfile) + ") " +                           "[" + percentage + "] " +                           "... Mdata: " + f_name + " ---" +                           " (cumul:"+ str(nb_totalfiles_treated) +"/"+ str(monRepertoire_totalnb_files) + ")"+                           "timeleft:" + str(duree_restante).split('.')[0] + ">"
            sys.stdout.write("\r" + msgtodisplay1)
            sys.stdout.flush()
            display1_ok=True #pour gestion de l'affichage
            #pour affichage sysout ------------f
            #----

            var_catfile=parm_fileExtDico[f_ext.lower()]
            fileDict["data"]['doctype']=var_catfile  #récupération dans le dico du label du type
            #chemin d'origine
            fileDict["data"]['orgnl_dirname']=parm_directory_name, 
            #nom du fichier complet original
            #fileDict['filename']=f
            fileDict["data"]['filename']=f.name            #suite à l'utilisation de scandir
            #extension du fichier (en lower case)
            #fileDict['extfile']=os.path.splitext(f)[1].lower()
            fileDict["data"]['extfile']=f_ext

            #Recherche des metadonnées
            if not fnph_alreadyexists_in_db(parm_directory_name, f.name):
                #Test de présence dans la base
                #catégorie de fichier (image, vidéo)
                #stats du fichier

                fileDict["data"]['stats']=fnph_getstats_file(parm_directory_name,f)  #récupération de stats : 
                                                             #mode, ino, dev, nling, uid, gid, size, atime, mtime, ctime) 
                #Metadonnées (images et vidéos)
                #exif
                if var_catfile=='image':  #à voir comment généraliser (pour éviter la valorisation en dur)

                    #si image (VOIR SI NECESSAIRE DE TESTER LE TYPE DE FICHIER POUR LES METATDATA EXIF - NOTMT FICHIERS .MOV)
                    fileDict["data"]['exif'] =fnph_getexif(os.path.join(parm_directory_name,f.name))  #récupération des métadonnées exif
                    #l'image elle même (réduite?) si image (rendu inactif pour l'instant - ralentit pas mal)
                   #fileDict['image']=fnph_getimage(parm_directory_name,f, parm_db)      #récup des métadonnées pour stockage de l'image (nécessite la bd)

                #video metadata
                if var_catfile=='video':  #à voir comment généraliser (pour éviter la valorisation en dur)
                    fileDict["data"]['vid_ppty']=fnph_get_video_properties(os.path.join(parm_directory_name,f))

                #iptc
                #pour tous les types d'images : recherche des données iptc - annulé pour l'instant (ne fonctionne pas)
                #fileDict['data']['iptc']=fnph_getiptc(parm_directory_name,f)      #récup des données iptc (si existent) (inactif pour l'instant - ralentit)

                #events
                fileDict['data']['events']={fieldname:{'edate':str(dnow),
                                               'ename':'insert' ,
                                               'estatus':'ok'   }}

                #indication de l'existence ou non du fichier dans la bd
                fileDict['already_exist_in_db']=False

        
                #initialisation de l'item copy_events
                fileDict['data']['copy_events'] = [{"event_init":"initialisation copy_events "+ str(datetime.now())}]

                
            else:
                #indication de l'existence ou non du fichier dans la bd
                fileDict['already_exist_in_db']=True

                #initialisation de l'item copy_events
                fileDict['data']['copy_events']=[]
                
                
                
            #copie--------------------------------------------------------------------------
            copy_event_to_add              = {}           #dictionnaire à ajouter à l'item copy_events
            copy_event_to_add["copy_id"  ] = global_unique_id.replace('.','_')
            copy_event_to_add["copy_date"] = str(datetime.now())

            if parm_do_copy: #copie effectuée si demandée (que le fichier existe ou non dans la base)

                #---------------------------------------------------------------
                copy_status_comment = fnph_copy_file(f.name, parm_directory_name, parm_do_copy_dest_dir_name)
                #---------------------------------------------------------------
                copy_event_to_add["copy_source" ] = os.path.join(parm_directory_name        , f.name)
                copy_event_to_add["copy_dest"   ] = os.path.join(parm_do_copy_dest_dir_name , f.name)
                copy_event_to_add["copy_status" ] = copy_status_comment[0]
                copy_event_to_add["copy_comment"] = copy_status_comment[1]

            else:
                copy_event_to_add["status"]="na"
                copy_event_to_add["copy_comment"] = "copy not asked"
                
            fileDict['data']['copy_events'].append(copy_event_to_add)    
            #Append des données du fichier dans la liste
            listdata.append(fileDict)
            nb_files+=1

            
    #Affichage du nombre de fichiers trouvés
    if display1_ok: #pour passer à la ligne après le dernier sys.std (display1) s'il y en a eu
        msglibre="" #se place en fin de ligne
        print(msglibre)
        
    return listdata


# In[10]:


mydico={}
mydico['data']={}
mydico['data']['copy_event']=[{"event_init":"initialisation"}]
mydico


# In[11]:


mylist=[]
mylist.append("element")
mylist


# #test de la fonction fnph_listDirectory (nécessite fnph...)
# 
# strtimestamp = str(datetime.timestamp(datetime.now()))
# global_unique_id = strtimestamp
# 
# #connection au serveur mongodb 27017, base test
# client = pymongo.MongoClient('localhost',27017)
# mydb = client["prjph_catalogue"]
# mycollection = mydb["test_documents"]
# 
# #ou encore mycollection = client.prjph_catalogue.test_documents
# test_dir=r'C:\Users\LENOVO\Pictures'
# #il faut maintenant un dictionnaire
# test_type={'.jpg':'image','.png':'image'}
# 
# test=fnph_listDirectory(test_dir, test_type, mydb)                                        
# test

# In[12]:


#Fonction de recherche de présence de document dans la base
def fnph_alreadyexists_in_db(parm_dirname, parm_filename):
    """
    retourne un booléen
    """

    f=parm_filename
    if mycollection.count_documents({"orgnl_dirname":parm_dirname,"filename":parm_filename}, limit=1):
        result = True
    else:
        result = False
    
    return result
    


# In[13]:


#fonction d'incrémentation de la variable globale global_num_fldr
def fnph_incremente_GLOBAL_NUM_FLDR():
    """
    incrémente la variable global_num_fldr qui donne le numéro d'ordre du répertoire traité
    """
    global GLOBAL_NUM_FLDR
    GLOBAL_NUM_FLDR = GLOBAL_NUM_FLDR + 1
    return(GLOBAL_NUM_FLDR)


# In[14]:


#fonction d'incrémentation de la variable globale global_nbtotfiles_treated
def fnph_incremente_GLOBAL_NBTOTFILES_TREATED():
    """
    incrémente la variable global_num_fldr qui donne le numéro d'ordre du répertoire traité
    """
    global GLOBAL_NBTOTFILES_TREATED
    GLOBAL_NBTOTFILES_TREATED = GLOBAL_NBTOTFILES_TREATED + 1
    return(GLOBAL_NBTOTFILES_TREATED)


# In[15]:


def fnph_duree_restante(parm_timestart, parm_timenow, parm_nb_treated, parm_nb_tot):
    """
    calcul de la durée restante estimée après traitement de parm_nb_treated éléments sur une durée
    donnée par parm_timenow - parm_timestart
    """
    timepast           = parm_timenow - parm_timestart
    timepast_per_file  = timepast/parm_nb_treated
    nb_files_remaining = parm_nb_tot - parm_nb_treated
    duree_restante = nb_files_remaining * (timepast_per_file + dt.timedelta(seconds=0.055)) #ajout de 0.055s par file
                                                                                             # pour la partie insert/update

    return(duree_restante) #à afficher avec str


# In[16]:


#TEST fnph_duree_restante
GLOBAL_NBTOTFILES_TREATED = 0
test_monRepertoire_totalnb_files = 2 #monRepertoire_totalnb_files

test_dnow       = datetime.now()
test_start_time = test_dnow.replace(minute=test_dnow.minute-1) # moins 1 minute

test_nbtotfiles_treated = fnph_incremente_GLOBAL_NBTOTFILES_TREATED()
duree_restante = fnph_duree_restante(test_start_time, 
                                     test_dnow,
                                     test_nbtotfiles_treated, 
                                     test_monRepertoire_totalnb_files) #nombre surestimé puisque tous types confondus

str(test_start_time), str(test_dnow), test_monRepertoire_totalnb_files, str(duree_restante)


# In[17]:


def fnph_cat_filesofonedirectory_majdb(parm_monRepertoire, parm_dicotypes, 
                                       parm_mydb, parm_do_copy=False, parm_do_copy_dest_dir_name=""):
    """
    fonction de recherche des fichiers de type parm_types dans le répertoire parm_monRepertoire
    (via la fonction fnph_listDirectory_metadata_copy)
    avec récupération des metadonnées (exif, ...)
    et insertion du fichier et des metadonnées dans la bd si non déjà présent
    et retourne le nombre d'insertions effectuées, et le nombre de mise à jour
    (ajout de parm_mydb pour l'appel à fnph_listDirectory_metadata_copy pour l'appel à fnph_getimage)
    
    remarque : parm_types est un dictionnaire et uniquement passé aux fonctions appelées
    """
    
    #print('debug-->........<fnph_cat_filesofonedirectory_majdb>..............'+str(datetime.now()))

    nb_insert=0
    nb_update=0
    display2_ok=False #pour gestion de l'affichage
    #on peut ajouter ici les stats par catégorie image/vidéo avec mylist[i]["data"]['cat_img']
    
    #1-------------------------------------
    #Recherche des données à enregistrer dans la base pour chaque fichier dans parm_monRepertoire -> mylist
    mylist = fnph_listDirectory_metadata_copy(parm_monRepertoire, parm_dicotypes, parm_mydb,
                                              parm_do_copy, parm_do_copy_dest_dir_name)
    
    #2-------------------------------------
    #Ecriture de la liste des éléments dans la bd, élément par élément avec test de présence
    #for i in tqdm.tqdm(range(len(mylist))):    #avec affichage de la barre de progression
    nb_files_in_rep=len(mylist) #pour l'affichage et éviter les calculs redondants (va plus vite ?)
    for i in range(nb_files_in_rep):
        test_dirname  = mylist[i]["data"]["orgnl_dirname"]
        test_filename = mylist[i]["data"]["filename"]

        #pour l'affichage-----sysout sur la même ligne pour donner une vision de l'avancement-------------d
        prct=(i+1)/nb_files_in_rep
        percentage = "{:.2%}".format(prct)

        msgtodisplay2="\r" + "   <indb/toPut:" + str(i+1) + "/" + str(len(mylist)) + ")" +                          "[" + percentage + "] ... InsUp: " +                           test_filename + " " +                          "---" + ">"
        sys.stdout.write("\r" + msgtodisplay2)
        sys.stdout.flush()
        display2_ok=True #pour gestion de l'affichage
        #print(msgtodisplay2)
        #pour l'affichage---------------------------------------------------------------------------------f
        
        #----------------------------------------------------------------------------------------

        #test de présence dans la base
        if mylist[i]["already_exist_in_db"]:
            #UPDATE
            ##### mise à jour par ajout de champ dans un champ existant (utilisation de .)
            dnow = datetime.now()
            #strnow = str(dnow.year)[:2]+str(dnow.month)+str(dnow.day)+str(dnow.hour)+str(dnow.minute)+str(dnow.second)+'_'+str(dnow.microsecond)
            strtimestamp = str(datetime.timestamp(dnow)).replace('.','_')
           #fieldname = 'events.event_'+str(strtimestamp)
            fieldname = 'events.event_'+global_unique_id.replace('.','_') #on met un identifiant unique pour tous les fichiers du même traitement
               
            #set pour ajouter/màj un champ de type objet, push pour ajouter un élément dans un item de type liste"
            result = mycollection.update_many( 
                                                 {"orgnl_dirname":test_dirname,"filename":test_filename}, 
                                                 {'$set' :{fieldname    :{'edate':str(dnow),
                                                                         'ename':'insert ko',
                                                                         'estatus':'already exists',
                                                                         }
                                                          },
                                                  '$push':{"copy_events":{"$each":mylist[i]["data"]["copy_events"]}}
                                                  }
                                             )
            
            nb_update+=1
            
        else:
            #INSERT
            #print('<fnph_cat_f...> n''existe pas -> insertion de mylist[',i,'] --------', str(mylist[i])[0:40], '.........')
            mycollection.insert_one(mylist[i]["data"])
            nb_insert+=1
            
    
    if display2_ok:
        msglibre=""
        print(msglibre) #ce print pour passer à la ligne - s'ajoute en fin de ligne
    return [nb_insert,nb_update]
            


# In[18]:


test_dirname="C:\\Users\\LENOVO\\Documents\\Projets\\Prj_photos\\Prjph_repertoire_test\\diximages\\sr_deuximages"
test_dirname="C:\\Users\\LENOVO\\Documents\\Projets\\Prj_photos\\Prjph_repertoire_test\\diximages"
test_filename="Capturefondnb.JPG"

#mylist[i]["data"]["copy_events"]

#result = mycollection.update_many( 
#                                                 {"orgnl_dirname":test_dirname,"filename":test_filename}, 
#                                                 {"$push"        :{"copy_events":[{"nouveau":8}]}        })


# In[19]:


#fonction récursive de traitement d'un répertoire

def fnph_traitementrep(parm_rep, parm_dicotypes, parm_mydb, parm_df_log_cols, 
                       parm_do_copy=False, parm_do_copy_dest_dir_name=""):
    """fonction récursive de traitement d'un répertoire et de ses sous-répertoires
        par lecture des sous-répertoires
        et pour chacun, appel récursif de la fonction pour effectuer le même traitement
        puis traitement des fichiers du répertoire courant
        parm_df_log_cols sert à récupérer la structure du df historique (pour simplifier les éventuelles modif de structure)
        retourne 
        - le nombre d'insert et d'update
        - le nombre d'update
        - le nombre de répertoires explorés
        - le df_log (!)
        (ajout de parm_mydb pour appel à fnph_getimage appelée dans les fonctions appelées)
        
        remarque : parm_dicotypes est un dictionnaire, et il est juste transmis aux fonctions appelées
    """
    #from os import listdir
    #from os.path import isfile, join, isdir

    #print('.............<fnph_traitementrep>..............')
    #print('Répertoire exploré :', parm_rep)
    
    #Initialisation des compteurs d'insertion et mise à jour
    nbtot_insert = 0
    nbtot_update = 0
    #Initialisation des compteurs de répertoires explorés et du dataframe historique
    local_log_nb_rep = 0
    local_df_log     = pd.DataFrame(columns=parm_df_log_cols) 

    #----------------------------------------------
    #1)recherche des répertoires (sous-répertoires)
    #----------------------------------------------
    liste_repertoires = [r for r in listdir(parm_rep) if isdir(join(parm_rep, r))]
    #possible ici de remplacer par scandir... à voir
    
    #Traitement des répertoires (appel récursif) pour traitement des fichiers et des (sous-)répertoires
    #for onerep in tqdm.tqdm(liste_repertoires):   #avec affichage de la barre de progression
    for onerep in liste_repertoires:
        #pour chaque sous-rep trouvé :
        local_log_nb_rep+=1 #incrémentation du nombre de répertoires traités à chaque répertoire traité
        
        #déplacé plus bas
        #ajout (append) du répertoire traité 'parm_rep' dans le df historique
        #local_df_log     = local_df_log.append({'time'        : str(datetime.now()),
        #                                        'rep_explored': os.path.join(parm_rep,onerep),
        #                                        'comment'     : "sous-répertoire"}, 
        #                                        ignore_index=True)
        
        #appel récursif --------------------------------------------------------------------------------
        list_rep_nb = fnph_traitementrep(join(parm_rep,onerep), parm_dicotypes, parm_mydb, parm_df_log_cols, 
                                         parm_do_copy, parm_do_copy_dest_dir_name)
        #appel récursif --------------------------------------------------------------------------------
        if type(list_rep_nb)==list:
            nbtot_insert    +=list_rep_nb[0]
            nbtot_update    +=list_rep_nb[1]
            local_log_nb_rep+=list_rep_nb[2]
            local_df_log     =local_df_log.append(list_rep_nb[3])

    #----------------------------------------------
    #2)traitement des fichiers du répertoire courant avec insert et/ou update dans la fonction fnph_cat_filesononedirectory()
    #----------------------------------------------
    #appel à la fonction de traitement des fichiers du répertoire 
    # (recherche des fichiers dans le répertoire, de leurs metadonnées et mise à jour de la bd)
    loc_debut_time=datetime.now() #pour avoir une idée de la durée de l'appel (insertion/update)
    #--------------------------------------------------------------------------------
    list_files_nb = fnph_cat_filesofonedirectory_majdb(parm_rep, parm_dicotypes, parm_mydb, 
                                                       parm_do_copy, parm_do_copy_dest_dir_name)
    #--------------------------------------------------------------------------------
    loc_fin_time  =datetime.now() #
    loc_duree = loc_fin_time - loc_debut_time
    if type(list_files_nb)==list:
        nbtot_insert+=list_files_nb[0]
        nbtot_update+=list_files_nb[1]
        #ce print pour aller à la ligne après les sys.stdout.write dans fnph_cat_filesofonedirectory_majdb
        #print(" <... (put in bd:" + str(list_files_nb[0]+list_files_nb[1]) + ")>") 
        #sys.stdout.write("\r" + "<sys3" +
        #           str(list_files_nb[0]+list_files_nb[1]) + 
        #           "/" + str(nbtot_insert+nbtot_update) + " files" +
        #           #"-d:"+str(loc_debut_time) + "-f:"+str(loc_fin_time) +
        #           " - durée : " + str(loc_duree).split('.')[0] + "finsys3>" ) 
        #sys.stdout.flush()
        #
    #ajout (append) du répertoire traité 'parm_rep' dans le df historique
    local_df_log     = local_df_log.append({ 'unique_id'        :  global_unique_id,
                                             'time'              : str(datetime.now()),
                                             'rep_explored'      : parm_rep,
                                             'nb_sous_rep'       : len(liste_repertoires), 
                                             'nb_of_files'       : list_files_nb[0]+list_files_nb[1],
                                             'nb_of_files_cumul' : nbtot_insert+nbtot_update,
                                             'comment'           : ""}, 
                                             ignore_index=True) 
    

    return [nbtot_insert, nbtot_update, local_log_nb_rep, local_df_log]
        
        


# In[20]:


#test appel fnph_traitementrep

start_time=datetime.now()
GLOBAL_NUM_FLDR=0
GLOBAL_NBTOTFILES_TREATED=0
strtimestamp = str(datetime.timestamp(datetime.now()))
global_unique_id = strtimestamp
monRepertoire_totalnb_fldrs=10 #forcé pour test
monRepertoire_totalnb_files=10 #forcé pour test

#connection au serveur mongodb 27017, base test
client = pymongo.MongoClient('localhost',27017)
mydb = client["prjph_catalogue"]
mycollection = mydb["test_documents"]
#ou encore mycollection = client.prjph_catalogue.test_documents

#testRepertoire = r"C:\Users\LENOVO\Documents\Projets\Prj_photos\Prjph_repertoire_test\diximages"
#testRepertoire = '\\\\DESKTOP-AQNKR8B\\Pictures'
testRepertoire = r"C:\Users\LENOVO\Documents\Projets\Prj_photos\Prjph_repertoire_test\uneimage"

mydico_types={'.JPG':'image','.jpg':'image'}
mynbrep=0
mydflog=pd.DataFrame([{'time':str(datetime.now()),'rep_explored':'a path', 'comment':'test'}])

#appel
#fnph_traitementrep(testRepertoire, mydico_types, mydb, mydflog.columns)


mydflog


# In[21]:


def fnph_getimage(parm_path, parm_file, parm_db):
    """
    Cette fonction convertit un fichier image en une donnée stockable dans mongodb
    Elle restitue les métadonnées à stocker dans la base mongo (passée en paramètres pour le stockage intermédaire je suppose)
    """
    #print('.............<fnph_getimage>..............')

    
    #from pymongo import MongoClient
    #import gridfs
    #pip install opencv-python (pour cv2)
    #import cv2
    #import os
    
    pathfile=os.path.join(parm_path, parm_file)

    # access our image collection
    #client = MongoClient('localhost', 27017)
    #db = client['prjph_catalogue']
    #testCollection = db['test_documents']
    fs = gridfs.GridFS(parm_db) #donc envoyer client['prjph_catalogue'] dans parm_db

    # read the image and convert it to RGB
    myfile=pathfile

    image = cv2.imread(myfile)
    image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

    # convert ndarray to string
    imageString = image.tostring()

    # store the image (stockage intermédiaire quelquepart dans la base avant insertion des métadonnées)
    imageID = fs.put(imageString, encoding='utf-8')

    # create our image meta data 
    meta = {
        'name': parm_file,
        'images': [
            {
                'imageID': imageID,
                'shape': image.shape,
                'dtype': str(image.dtype)
            }
        ]
    }

    return(meta)


# In[22]:


def fnph_retreive_image(parm_filename, parm_mydb, parm_mycollection):
    #see https://stackoverflow.com/questions/49493493/python-store-cv-image-in-mongodb-gridfs
    """
    Fonction retreive image dans mongo pour affichage de l'image stockée avec la fonction getimages
    """
    
    #Retreive
    #import numpy as np
    #import gridfs
    #from matplotlib import pyplot as plt

    #base
    #client = pymongo.MongoClient('localhost',27017)
    #mydb = client["prjph_catalogue"]
    #mycollection = mydb["test_documents"]

    #nom de l'image à récupérer (valeur du champ 'name')
    #parm_name = parm_filename

    # get the image meta data
    image = parm_mycollection.find_one({'name': parm_filename})['images'][0]

    # get the image from gridfs
    fs = gridfs.GridFS(mydb) #donc envoyer client['prjph_catalogue'] dans parm_db

    gOut = fs.get(image['imageID'])

    # convert bytes to ndarray
    img = np.frombuffer(gOut.read(), dtype=np.uint8)

    # reshape to match the image size
    img = np.reshape(img, image['shape'])

    #Affichage
    plt.imshow(img, interpolation='nearest')
    plt.show()


# In[23]:


#test fnph_retreive_image

#filename='Capturenb.JPG'

#testimage=fnph_retreive_image(filename, mydb, mycollection)


# In[24]:


#Fonction de calcul du nombre et de la taille totale des fichiers catalgués dans la base

def fnph_clc_nb_st_size_tot(parm_collection):
    """
    Fonction de calcul du nombre et de la taille totale des fichiers catalgués dans la base
    retourne une liste de deux éléments : taille, nombre
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


# In[25]:


#Test appel fonction fnph_clc_nb_st_size_tot
client = pymongo.MongoClient('localhost',27017)
mydb = client["prjph_catalogue"]
mycollection = mydb["test_documents"]

#fnph_clc_nb_st_size_tot(mycollection)


# In[26]:


#test fnph_getimage

testRepertoire = r"C:\Users\LENOVO\Documents\Projets\Prj_photos\Prjph_repertoire_test\diximages"
testfile='Captureph1.jpg'
testfile='Capturenb.JPG'

testmeta=fnph_getimage(testRepertoire,testfile, mydb)

testmeta


# In[27]:


def fnph_get_video_properties(filename):

    """
    Récupère les metadata du fichier avec la bibliothèque 'hachoir'
    """
    #!pip3 install hachoir
    #import subprocess
    
    getresult_ok=True
    try:
        result = subprocess.Popen(['hachoir-metadata', filename, '--raw', '--level=3'],
            stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
    except:
        msg_err="ERREUR result=subprocess... dans fnph_get_video_properties pour le fichier " + filename
        getresult_ok=False
    
    if getresult_ok:
        results = result.stdout.read().decode('utf-8').split('\r\n')

        properties = {}
        #formatage des données duration, width, height
        for item in results:

            if item.startswith('- duration: '):
                duration = item.lstrip('- duration: ')
                if '.' in duration:
                    #t = datetime.datetime.strptime(item.lstrip('- duration: '), '%H:%M:%S.%f')
                    t = datetime.strptime(item.lstrip('- duration: '), '%H:%M:%S.%f')
                else:
                    #t = datetime.datetime.strptime(item.lstrip('- duration: '), '%H:%M:%S')
                    t = datetime.strptime(item.lstrip('- duration: '), '%H:%M:%S')
                seconds = (t.microsecond / 1e6) + t.second + (t.minute * 60) + (t.hour * 3600)
                properties['duration'] = round(seconds)

            if item.startswith('- width: '):
                properties['width'] = int(item.lstrip('- width: '))

            if item.startswith('- height: '):
                properties['height'] = int(item.lstrip('- height: '))

        #ajout de toutes les données (yc duration, width, height)
        #properties['metadata_all']=results                                 <<<<<<<<<<<<< A TESTER
        
    else:
        properties={'msg_err':msg_err}
        
    return properties


# In[28]:


get_ipython().run_cell_magic('time', '', '#test appel fnph_get_video_properties\nmonRepertoire = r"C:\\Users\\LENOVO\\Videos\\Captures"\nmonRepertoire = r"C:\\Users\\LENOVO\\Documents\\Projets\\Prj_photos\\Prjph_repertoire_test\\Fichiers_MOV"\nmonFichier = "IMG_0578.MOV"\n\nmyfile=os.path.join(monRepertoire,monFichier)\nfnph_get_video_properties(myfile)\n')


# In[29]:


def fnph_startandend_ajout_evnmtppl_mongodb(parm_state,        parm_titretraitement, parm_dir,   parm_types, 
                                            parm_mycollection, parm_tailletot,       parm_nbtot, parm_nb_insupd,
                                            parm_nb_rep,       parm_duration,
                                            parm_do_copy,      parm_do_copy_dest_dir_name
                                           ):

    """
    parm_state indique s'il s'agit du début du traitement ("début")
        alors on regarde s'il faut écrire eventgnl_000000000
    ou s'il s'agit de la fin de traitement ("fin") 
        alors on écrit les stats
    fonction d'écriture dans la base mongodb de l'événement 'traitement'
    on écrit un événement avec les éléments suivants
    - identifiant événement
    - date heure
    - nom du traitement
    - nom du répertoire exploré
    - nombre de fichiers traités
    - (nombre de fichiers image)
    - (nombre de fichiers vidéo)
    - nombre d'insersions
    - nombre de mises à jour
    - volume après traitement (cumul des tailles de fichiers)
    - nombre total de documents 
    """
    
    #print('.............<fnph_startandend_ajout_evnmtppl_mongodb>..............')

    #Création des variables
    dnow = datetime.now()
    strtimestamp = str(datetime.timestamp(dnow)).replace('.','_')
    dnow=str(dnow)

    #Création du dico

    #on créée la donnée eventppl si elle n'existe pas déjà (cas de création de la base)
    if parm_state=="début":
        if not mycollection.count_documents({"eventgnls.eventgnl_000000000.epname":"création"}, limit=1):
            mycollection.insert_one({"eventgnls":{"eventgnl_000000000":{"epdate":str(dnow),"epname":"création"}}})

    if parm_state=="fin":
    #Ecriture des données (mise à jour de eventppl - il n'y en a qu'un - par ajout d'un sous-document)   
       #fieldname = 'eventgnls.eventgnl_'+str(strtimestamp)
        fieldname = 'eventgnls.eventgnl_'+global_unique_id.replace('.','_') #on met l'identifiant unique utilisé 
                                                                            #pour tous les fichiers du même traitement
               
        result = mycollection.update_many( 
                     {"eventgnls.eventgnl_000000000.epname":'création'}, #on cherche eventppl renseigné (il n'y en a qu'un)
                     {'$set':{fieldname:{'epdate'           :str(dnow),  #ajout des données suivantes
                                         'epname'           :parm_titretraitement,
                                         'epdir'            :parm_dir,
                                         'ep_copy_asked'    :parm_do_copy, 
                                         'ep_copy_dest'     :parm_do_copy_dest_dir_name,
                                         'eptypes'          :parm_types,
                                         'epnb_ins'         :parm_nb_insupd[0],
                                         'epnb_upd'         :parm_nb_insupd[1],
                                         'eptot_size_files' :parm_tailletot,
                                         'epnb_files_in_bd' :parm_nbtot,
                                         'epnb_rep_explored':parm_nb_rep,
                                         'epduration_trtmt' :parm_duration
                                         }}} )



    
    return
            


# In[30]:


def fnph_getstats_dir(parm_dir):

    """
    Cette fonction renvoie le nombre de fichier et de sous-répertoires sur toute l'arborescence de parm_dir
    """
    nb_files   = 0
    nb_folders = 0

    for _, dirnames, filenames in os.walk(parm_dir): # _ for 'root' not used here
        nb_files   += len(filenames)
        nb_folders += len(dirnames)

    #print("{:,} files, {:,} folders".format(files, folders))
    return [nb_files, nb_folders]


# In[31]:


#test fnph_getstats_dir()
path=r"C:\Users\LENOVO\Documents\Projets\Prj_photos\Prjph_repertoire_test"
path=r"C:\Users\LENOVO\Documents\Projets\Prj_photos"

fnph_getstats_dir(path)


# ## Programme principal

# In[32]:


get_ipython().run_cell_magic('time', '', '#Programme principal\n#Lecture du catalogue de fichier et constitution du dictionnaire des données à insérer dans la base (fnp_listDirectory)\n#puis écriture dans la base de chaque fichier et de ses données s\'il n\'est pas déjà présent. Sinon, mise à jour de ses\n#données (ajout d\'un événement).\n\n\n#Paramètres utilisateurs***********************************************************************************\n#Paramètres utilisateurs***********************************************************************************\n#Paramètres utilisateurs***********************************************************************************\n#Paramètres utilisateurs***********************************************************************************\n#Paramètres utilisateurs***********************************************************************************\n#Paramètres utilisateurs***********************************************************************************\n#Paramètres utilisateurs***********************************************************************************\n\n#Répertoire de sauvegarde de l\'historique des répertoires explorés\nparm_df_log_rep = r\'C:\\Users\\LENOVO\\Documents\\Projets\\Prj_photos\\Prjph_log\'\n\n#Paramètres de connexion\n#------ Base  -------------------*\n#connection au serveur mongodb 27017, base test\nclient = pymongo.MongoClient(\'localhost\',27017)\nmydb = client["prjph_catalogue"]\n\n#------ COPIE DES FICHIERS ? -------------*  #0<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\nmydo_copy = True\nmydo_copy_dest_dir_name = r"C:\\Users\\LENOVO\\Documents\\Projets\\Prj_photos\\Prjph_repertoire_test\\test_shutil\\copies"\n\n#------ LIBELLE DU TRAITEMENT-------------*  #1<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\nmontitre_traitement="Catalogue de fichiers - " #Est systématiquement complété plus bas\n                                                                             #par monRepertoire\n#------ COLLECTION -------------------*      #2<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\nmycollection_name = "test_documents"        #TEST\n#mycollection_name = "images_videos"          #PREPROD\n#------ ---------- -------------------*\nmycollection = mydb[mycollection_name]\n#------ REPERTOIRE INITIAL ---------------*  #3<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n#.........................................*\n#REPERTOIRES DE TEST (dans la collection test_documents)\n#.........................................*\nmonRepertoire = r"C:\\Users\\LENOVO\\Documents\\Projets\\Prj_photos\\Prjph_repertoire_test\\test_shutil"\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\Pictures\\MyPhotos"\n#monRepertoire = r"C:\\Users\\LENOVO\\Videos\\Captures"\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\Pictures"\n#monRepertoire = r"C:\\Users\\LENOVO\\Documents\\Projets\\Prj_photos\\Prjph_repertoire_test\\diximages"\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\MyPhotos"\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\Photos" #SUR LES .MOV UNIQUEMENT 181020 - 00h45 env (durée : qq minutes) ok\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\Picosmos" #nouveaux types d\'images : webp\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\Svgd_iPhone" ok\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\SvgWhatsapp\\iPhone de Patrice iPhone 5S\\Messages\\2019-06-06\\WhatsApp\\Coeur de kid\\Coeur de kid [1]"\n#.........................................*\n#REPERTOIRES DE PREPROD (dans la collection images_videos)\n#.........................................*\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\De_DD_Verbatim" #171020 - 00h33 environ. ko \n#monRepertoire = r"C:\\Users\\LENOVO\\Documents\\Projets\\Prj_photos\\Prjph_repertoire_test\\Nouveau dossier" #pour résoudre pbl ouverture img=PIL....img\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\De_DD_Verbatim" #171020 - 01h03 environ. ok (durée : 1h52)\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\Chiens" #171020 - 16h00 env (durée : qq secondes)\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\Ecole" #171020 - 16h05 env (durée : qq secondes)\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\Photos" #181020 - 00h22 env (durée : ...) ko (manque subprocess pour les mov)\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\Photos" #181020 - 00h57 env (durée : ...) ko (encore subprocess - pourtant corrigé -> ajout d\'une exception)\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\Photos" #181020 - 1h36 env (durée : 4h env) ok\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\Picosmos" #181020 - 13h30 env (qq secondes) ok (nouveaux types d\'images : webp)\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\Svgd_iPhone"\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\Photos_classement" #181020 - 17h env (45min) ok\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\Photos_svg_sur_MyBook_le181122" #181020 - 19h10 env (2h) ok\n#monRepertoire = r"\\\\DESKTOP-AQNKR8B\\SvgWhatsapp"\n#----------------------------------------------------------------------------*\n\n#ajout de monRepertoire à montitre_traitement\nmontitre_traitement += "("+monRepertoire+")"\n#------  TYPES DE FICHIERS  ------------------*\n#listes des extensions et leur label\n#Remarque : les contrôles se font sans la casse\nmes_types_images = {\n                \'.BMP\'  : \'image\',                \'.TIFF\' : \'image\',\n                \'.tif\'  : \'image\',                \'.JPEG\' : \'image\',\n                \'.jpg\'  : \'image\',                \'.jfif\' : \'image\',\n                \'.pjpeg\': \'image\',                \'.pgp\'  : \'image\',\n                \'.GIF\'  : \'image\',                \'.PNG\'  : \'image\',\n                \'.svg\'  : \'image\',                \'.webp\' : \'image\'\n                        }\n\nmes_types_videos = {\n                \'.mp4\' : \'video\',                \'.mov\' : \'video\',\n                \'.avi\' : \'video\',                \'.flv\' : \'video\',\n                \'.wmv\' : \'video\',                \'.mpeg\': \'video\',\n                \'.mkv\' : \'video\',                \'.asf\' : \'video\',\n                \'.rm\'  : \'video\',                \'.vob\' : \'video\',\n                \'.ts\'  : \'video\',                \'.dat\' : \'video\'\n                        }\n\n#mes_types_images = {\'.xxx\'  : \'pour test aucune image\'}\n#mes_types_videos = {\'.mov\'  : \'pour test videos mov\'  }\n\n#fin Paramètres************************************************************************************************\n#fin Paramètres************************************************************************************************\n#fin Paramètres************************************************************************************************\n#fin Paramètres************************************************************************************************\n#fin Paramètres************************************************************************************************\n#fin Paramètres************************************************************************************************\n\n\n#----------------\n#DEBUT TRAITEMENT\n#----------------\n\n#----------------\n#initialisations\n#----------------\n\n#Compteurs\nlog_nb_rep                = 0 #nombre de répertoires traités    \nGLOBAL_NUM_FLDR           = 0 #numéro du répertoire traité\nGLOBAL_NBTOTFILES_TREATED = 0 #nombre total de fichiers traités\n#df historique des répertoires traités\ndf_log = pd.DataFrame(columns=[\'unique_id\',\n                               \'time\',\n                               \'rep_explored\',\n                               \'nb_sous_rep\', \'nb_of_files\', \n                               \'nb_of_files_cumul\', \n                               \'comment\'])\n#globa_unique_id : Variable de valeur unique pour rsgner unique_id lors des différents appels\nstrtimestamp = str(datetime.timestamp(datetime.now()))\nglobal_unique_id = strtimestamp\nprint(\'\\n           global_unique_id:\', global_unique_id,\'\\n\')\n\n\n#teste l\'existence du répertoire de sauvegarde du log avant de démarrer\nif not os.path.exists(parm_df_log_rep):\n    print(\'ERREUR : le répertoire de sauvegarde n\'\'existe pas\')\n    print(\'ERREUR : \' + parm_df_log_rep)\n    print(\'ERREUR : veuillez vérifier et relancer\')\n    sys.exit("haa! errors! vérifier le nom du répertoire du log en paramètre et relancer")\n\n#------------\n# TRAITEMENT\n#------------\nstart_time=datetime.now()\nprint(\'start..................... : \', start_time, \'\\n\')\n\nprint(\'accès à\', monRepertoire, \'pour récupérer le nombre de sous-répertoires et de fichiers...\')\nprint(\'...\')\nmonRepertoire_totalnb_files, monRepertoire_totalnb_fldrs = fnph_getstats_dir(monRepertoire) \nprint(\'... ok\') #- accès à\', monRepertoire, \'pour récupérer le nombre de sous-répertoires et de fichiers...\\n\')\n\nprint("\\n**** Collection            :", mycollection_name, "****\\n")\nprint("**** Répertoire            :"  , monRepertoire, "****\\n")\nprint("****  nb folders (total)   :"  , monRepertoire_totalnb_fldrs+1, "****\\n")\nprint("****  nb files (tous types):"  , monRepertoire_totalnb_files  , "****\\n" )\n\nprint(\'remarque : les deux nombres après chaque répertoire correspondent\')\nprint(\'           au nombre de fichiers à la racine du répertoire et celui cumulé avec ceux des sous-répertoires\')\nprint(\'           de type recherché et qui ont été insérés ou mis à jours dans la base\\n\')\n\n\n#ajout de la ligne start dans df_log \n#rmq : nb_sous_rep et nb_of_files sont les documents à la racine du répertoire, pas le total\ndf_log=df_log.append({\'unique_id\'   :global_unique_id,\n                      \'time\'        :str(datetime.now()),\n                      \'rep_explored\':" <--- début de traitement - <"+montitre_traitement+">-->",\n                      \'comment\'     :"start "}, ignore_index=True)\n\n#------------------------------------------------------------------------\n#mise sous format lower pour comparaison sans prise en compte de la casse \n#(les extensions de fichiers lues sont mises en minuscules)\n# avec fusion des deux listes\nA, B = mes_types_images, mes_types_videos\nmydico_types = {key.lower():value for d in (A, B) for key,value in d.items()}\n\n#Début - insertion - Ajout de l\'insertion d\'un log + stats (taille globale et nombre de fichiers)\nfnph_startandend_ajout_evnmtppl_mongodb("début",      montitre_traitement, monRepertoire, mydico_types, \n                                        mycollection, 0                  ,0             ,           [], \n                                        log_nb_rep, "nb folders : "+str(monRepertoire_totalnb_fldrs) +\n                                                    "nb files   : "+str(monRepertoire_totalnb_files),\n                                        mydo_copy, mydo_copy_dest_dir_name\n                                        )\n\n#incrémentation du nombre de répertoires traités (+1) et écriture dans le df log du traitement du répertoire initial\n#remarque : en fait, cette ligne (répertoire initial) sera réécrite par la fonction fnph_traitementrep avec\n#           les nombres correspondants de sous-répertoires. Je la laisse car elle permet d\'identifier rapidement\n#           dans le df les répertoires de base de chaque traitement.\nlog_nb_rep+=1\ndf_log=df_log.append({\'unique_id\'   : global_unique_id,\n                      \'time\'        : str(datetime.now()),\n                      \'rep_explored\': monRepertoire,\n                      \'comment\'     :"Répertoire initial"}, ignore_index=True)\n\n#Appel pour traitement du répertoire initial et cumul des compteurs et du nombre de répertoires\n#---------------------------------------------------------------------------\nresult = fnph_traitementrep(monRepertoire, mydico_types, mydb, df_log.columns, \n                            mydo_copy, mydo_copy_dest_dir_name)\n#---------------------------------------------------------------------------\nlog_nb_rep+=result[2]\ndf_log=df_log.append(result[3])\n\n#Calcul de la taille globale de la base et du nombre de fichiers présents dans la base\nvar_tailletot, var_nbtot = fnph_clc_nb_st_size_tot(mycollection)\n\n#Fin - Ajout de l\'insertion d\'un log + stats (taille globale et nombre de fichiers)\nend_time=datetime.now()\nfnph_startandend_ajout_evnmtppl_mongodb("fin",        montitre_traitement, monRepertoire, mydico_types, \n                                        mycollection, var_tailletot,       var_nbtot,     result,\n                                        log_nb_rep, str(end_time - start_time),\n                                        mydo_copy, mydo_copy_dest_dir_name\n                                        )\n\n#-----------------------------------------------------------------------------\nmsg    = [\'\']*12\nend_time=datetime.now()\nmsg[0] = \'\\ndone----------------------------------------\'\nmsg[1] = \'nombre répertoires explorés       : \' + str(log_nb_rep)\nmsg[2] = \'nombre de fichiers traités        : \' + str(result[0]+result[1])\nmsg[3] = \'dont nb insertions                : \' + str(result[0])\nmsg[4] = \'et   nb updates                   : \' + str(result[1])\nmsg[5] = \'taille ttle des fichiers en base  : \' + str(var_tailletot)\nmsg[6] = \'nombre ttl de  documents en base  : \' + str(var_nbtot)\nmsg[7] = \'Durée du traitement               : \' + str(end_time - start_time)\nmsg[8] = \'\'\nmsg[9] = \'\'\nmsg[10] = \'done----------------------------------------\'\nfor i in range(len(msg)):\n    if msg[i]!=\'\':\n        print(msg[i])\n\n#svg du df pour enregistrement du répertoire traité dans un df historique.\nfor i in range(len(msg)):\n    if msg[i]!="":\n        df_log=df_log.append({\'unique_id\'   : global_unique_id,\n                              \'time\'        : str(end_time),\n                              \'rep_explored\': " <--- " + monRepertoire + " --- fin de traitement --->",\n                              \'comment\'     : msg[i]}, ignore_index=True)\n\n#Sauvegarde du df_log - nom complété de la collection et nom du répertoire exploré\ndnow = datetime.now()\nstrtimestamp = str(datetime.timestamp(dnow)).replace(\'.\',\'_\')\nmypathlog=r\'C:\\Users\\LENOVO\\Documents\\Projets\\Prj_photos\\Prjph_log\'\n#df_log_filename=\'prjph_df_log_\'+ str(strtimestamp) + \'.csv\'\ndf_log_filename= \'prjph_df_log__\' +                                   \\\n                    global_unique_id.replace(\'.\',\'_\') + "__" +        \\\n                    mycollection_name + "__" +                        \\\n                   (monRepertoire.split("\\\\")[-1]).replace(\' \',\'_\') + \\\n                 \'.csv\' #référencement avec global_unique_id utilisé pour référencer les documents dans la base.\ndf_log.to_csv(os.path.join(mypathlog,df_log_filename), sep=\'\\t\')\nprint(\'df_log savec into\', mypathlog)\n\nprint(\'\\nended..................... : \', end_time, \'\\n\')\n\n#Programme principal fin\n')


# In[33]:


pd.set_option('display.max_colwidth', None)  
pd.set_option('display.max_rows', df_log.shape[0]+1)
#df_log

