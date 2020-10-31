# Prjph_Photos - Projet de gestion des photos et videos : Catalogue - Statistiques - Copies - Création d'albums



## Catalogue de photos

Recherche de photos dans l'arborescence sous-jacente au répertoire indiqué par l'utilisateur.

Objectif : avoir un outil permettant de référencer les photos et d'obtenir des statistiques :

permettre une sauvegarde des photos dans un dd unique (les dossiers d'origine sont conservés) tout en permettant de sauvegarder et

référencer d'éventuelles nouvelles photos sans avoir à tout référencer à nouveau (celles déjà référencés seront topées 'déjà présentes'

obtenir des dossiers (albums) constitués de copies de photos par classement souhaité (doublons possibles).

### Anomalie

an01 : sur lecture fichier 3gp, pbl de format de metadonnées (utf8...)
		--> corriger (test sur le format des données) puis faire ev01 avant relance

### Evolutions possibles

ev01 : Gestion des reprises
	Ajout d'une clé 'répertoires traités' dans eventgnl donnant la liste des répertoires traités pendant la session
        pour chaque répertoire, un indicateur 'répertoire terminé' indique s'il a été traité en entier
		Ceci pour permettre, en cas d'arrêt (plantage sur une donnée par exemple) pendant le traitement, de reprendre sans avoir 
		à parcourir tous les fichiers de chaque répertoire. Test à faire en début de répertoire sur l'indicateur.

ev01bis : Gestion de la durée de traitement - interruptions - reprise
	Permettre un arrêt sur Touche appuyée ou selon un délai/horaire prédéterminé par l'utilisateur
	Suggestion : En début de traitement de nouveau répertoire :
			ajout d'un test dans la fonction récursive sur Touche appuyée/délai/horaire
			sortie de la fonction 
			Le répertoire précédemment traité sera topé 'traité' dans la base 
			(création d'un item 'répertoires traités':[liste des répertoires traités])

ev02 : Affichage sysout :
        modifier l'affichage des fichiers lus/insupd pour que les données restent alignées
		
ev03 : Gestion des doublons : 
		créer une fonction 'Gestion des doublons' qui détecte les doublons dans la base 
		- choisir un algorithme
		- pour chaque fichier traité :
			- une variable clé "doublon" est ajoutée (niveau filename) :
				{"doublon": {"est_en_double"   :True/False,
							 "nb_of_same_files":nnnn}}
							 "est_doublon_de"  :xxxx}}
				- un indicateur 'est_en_double' (True/False) est valorisé à True si d'autres fichiers identiques sont dans la base
				- un compteur 'nb_of_same_files' indique le nombre de fichiers identiques trouvés
				- la variable "est_doublon_de" n'est pas valorisée
				- pour chaque fichier trouvé identique, 
					- la variable clé "est_en_double"  est valorisée à True
					- la variable clé "est_doublon_de"  est valorisée avec le fichier traité comparé


		
## Statistiques :

nombre d'images par année, mois, jour
nombre de doublons
nombre de photos par sujet


# Avancement

## Réalisé

15/10 : création réseau. Accès au répertoire partagé.

test sur le répertoire partagé

ajout du stockage de l'image

annulation du stockage de l'image - pourra être effectué ultérieurement.

la correction de la récup des données exif (cf. en cours)

le traitement de la donnée exif 37500, bytes très longue. A priori, le pop ne fonctionne pas.

la taille de l'image stockée. A réduire éventuellement.

Identification de la donnée 'taille mémoire' de l'image (pour calcul du volume global) ----------------OK

Calcul de la taille globale : Requête de calcul de la somme des tailles mémoire -----------------------OK

Lancement d'une requête mongodb sur la bd via python et récupération du résultat dans une variable ----OK

Définition de la liste des extensions 'image' et 'vidéo' ----------------------------------------------OK

Ajout d'un document 'log' qui contient ------------------------------------------------------------OK
un titre illustrant le traitement lancé -----------------------------------------------------------OK
la date -------------------------------------------------------------------------------------------OK
le répertoire racine paramétré --------------------------------------------------------------------OK
la liste des répertoires explorés (faire un df au fur et à mesure du print) -----------------------OK
(utilité : avoir l'historique des répertoires consultés pour éviter de les refaire en cas de doute)

## Prochains points

### Partie Catalogue

Lancement du traitement sur différents répertoires :

Ajout d'un appel pour récupérer le nombre total de répertoires dans l'arboresence du répertoire initial afin de pouvoir avoir une estimation de la durée de traitement.

Ajout du nombre de fichiers lus (qqsoit le type de fichier) (pour avoir une idée de l'écart)

Notification des traitements sur cahier au fur et à mesure.

Calcul du nombre de fichier et du volume mémoire avec la requête ci-dessus

Mise sous forme dataFrame de la base et sauvegarde en csv (pour manips sur Excel)

Stats sous Excel ?

Le volume global va servir à étudier les solutions de stockage des copies.

La copie des fichiers catalogués dans le matériel de stockage qui aura été choisi.

### Fonctions suivantes

Fonctions suivantes : COPIE - GESTION DES DOUBLONS - RECHERCHE - CLASSEMENT - CREATION D'ALBUMS SELON CRITERES - RECONNAISSANCE

ajout de la sauvegarde d'une vignette de l'image dans la bd ajout du référencement des vidéos (=> ajout des types vidéos) faire le point sur les types de fichiers image et vidéo

penser à faire un test (copie des répertoires photo, avec écriture bloquée)

obtenir la liste des répertoires de photos avant la copie (pour les mettre en écriture interdite)

ajout de la fonction COPIE: même fonctionnement mais avec en plus une copie dans un dossier spécifé

+ mise à jour de la bd (ajout événement copie / création enreg avec deux événements - insert et copie)

ajout de la fonction de gestion des doublons à préciser

définir le lieu de stockage de la bd (cf. volume important si images stockées)

# Priorités

La création du catalogue (répertoire, fichier, taille). Les stats par dates. Le calcul du volume total (pour prévoir le matériel de stockage des copies). La création d'albums en fonction des dates. La sauvegarde d'albums sur clés usb (par années, par personne, par lieu).

appel à la fonction prjph_listdirectory : récupération dans un dictionnaire des noms des fichiers avec os.listdir contenus dans le répertoire passé en paramètre et de type passé en paramètre également.

Avec pour chaque fichier :

- récupération et ajout dans le dictionnaire des métadonnées exif avec PIL getexif (via fnph_getexif)
  (avec suppression des données 37500, 37510 et 59932 car trop longues et a priori non utiles
  , et suppression des clés dictionnaires)

- récupération et ajout dans le dictionnaire des stats par os.stats avec conversion en dicitonnaire (via fnph_getstats)

Mise à jour de la base mongo avec les fichiers contenus dans la liste obtenue :

- recherche de la présence du fichier dans la base (par appel à la fonction prjph_alreadyexists

- si le fichier n'est pas déjà présent : écriture des éléments du dictionnaire de données liées au fichier en cours 

- sinon, mise à jour du document dans la base par ajout d'un champ événement (identifiant unique créé avec la date)


# Catalogue : schéma du traitement (à mettre à jour)

Demande de répertoire de recherche à l'utilisateur

Recherche d'images dans le répertoire indiqué et l'arborescence sous-jacente format sélectionné : img, jpeg

Pour chaque image :

Récupération des caractéristiques de l'image

. nom du fichier image . emplacement d'origine (chemin) . matériel (nom du disque dur) . date de prise de vue . appareil . date de création du fichier origine

Mise à jour de la base de données :

. date de mise à jour . caractéristiques de l'image

En cas de présence dans le catalogue du couple (source, fichier) (cas d'un catalogue déjà effectué)

. mise à jour de l'instance par ajout de l'événement (référencement ko)

Gestion d'un journal

Début de traitement :

  . Ecriture ligne :

      - références du traitement
      - répertoire indiqué par l'utilisateur
      - répertoire de copie (lieu de stockage de la base de données)

Pour chaque copie

  . Ecriture ligne : 

      - chemin du répertoire origine
      - nom du fichier origine
      - nom fichier sauvegardé (après gestion de doublon)
Remarques sur les extensions image https://developer.mozilla.org/fr/docs/Web/Media/Formats/Types_des_images

sur les extensions video https://www.reneelab.fr/extension-video.html
