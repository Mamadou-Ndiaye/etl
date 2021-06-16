#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import pymongo
import pprint 
#import dns
from pprint import *


#******** Connexion au serveur mongodb ********

#con=pymongo.MongoClient("mongodb+srv://root:root@cluster0.gz60h.mongodb.net/dbalso?retryWrites=true&w=majority")
#con=pymongo.MongoClient("mongodb+srv://root:root@cluster0.gz60h.mongodb.net/")
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
#client =pymongo.MongoClient(host='10.153.54.249',port=27017)


#client.database_names()
my=myclient.list_database_names()
print('+++++++++++++liste des db+++++++++++++++++++++',my)
db=myclient.testrestoredb
print('-------------------connect to testrestoredb-------------------',db)
mescollections=db.list_collection_names()
print('-------------------liste des collections-------------------',mescollections)
#res=historique_sante.find()
#print(list(res))
#con.database_names()
#db = con.dbalso


# In[2]:


#******* Extraction **********
data = pd.read_csv("D:/AlSO/WP2/Déterminants_de_la_dynamique_évolutive_de_la_COVID-19_au_Bénin_final.xlsx - Déterminants de la dynamique....csv")
#print(data)


# In[3]:


#*************** Tranformation *****************
data["_1_Sexe_"]=data["_1_Sexe_"].replace([1,2],["Feminin","Masculin"])
#print(data["_1_Sexe_"])

data["_1_1_Quel_est_votre_statut_mat"]=data["_1_1_Quel_est_votre_statut_mat"].replace([1,2,3,4,999,888],["Marié ","Célibataire","Concubinage","Voeuf ou divorcé ","Refus","Reponse Non applicable"])
#Evolution
data["_3_De_quel_groupe_socio_cultur"]=data["_3_De_quel_groupe_socio_cultur"].replace([1,2,3,4,5,6,7,8,9,10,11,999,888],["Fon ","Adja","Nago","Xwla","Gen","Bariba","Peulh","Goun","Dendi","Yoruba",0,"Refus","Reponse Non applicable"])
data["_3_1_De_confession_religieuse_"]=data["_3_1_De_confession_religieuse_"].replace([1,2,3,4,5,6,7,999,888],["Traditionnelle","Musulmane","Catholique ","Protestant","Evangélique","Céleste",0,"Refus","Reponse Non applicable"])
data["_4_Quel_est_votre_statut_profe"]=data["_4_Quel_est_votre_statut_profe"].replace([1,2,3,4,5,6,7,8,9,10,999,888],["Travailleur indépendant  ","Fonctionnaire du secteur public ","Fonctionnaire du secteur privé ","Apprenant en formation formelle (Elève, étudiant) ","Apprenant en formation informelle (apprentissage) ","Retraité","En recherche d’emploi/Chômeur ","Ménagère","Revendeuse/Commerçante ",0,"Refus","Reponse Non applicable"])
data["_5_Quel_est_votre_principal_se"]=data["_5_Quel_est_votre_principal_se"].replace([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,999,888],["Agriculture/Elevage/Pêche/Transformation agro-alimentaire","Artisanat","Santé","Economie/Finance","Educatif ","Sécurité publique ","Energies, mines et hydraulique ","Affaires publiques ","Sport ","Environnement ","TIC ","Industriel/Commercial  ","Divertissement ","Construction ","Transport ","Religieux ","Sans emploi  ",0,"Refus","Reponse Non applicable"])
data["_6_Dans_quelle_tranc_otre_revenu_mensuel_"]=data["_6_Dans_quelle_tranc_otre_revenu_mensuel_"].replace([1,2,3,4,999,888],["Moins de 40 000 FCFA  ","Entre 40 000 et 100 000 FCFA","Entre 100 000 et 300 000 FCFA","Plus de 300 000 FCFA","Refus","Reponse Non applicable"])
data["_7_Quel_est_votre_pl_haut_niveau_d_tude_"]=data["_7_Quel_est_votre_pl_haut_niveau_d_tude_"].replace([0,1,2,3,4,999,888],["Non scolarisé ","Primaire","Secondaire 1er cycle (6ème à 3ème) ","Secondaire second cycle (Seconde à Terminale) ","Universitaire","Refus","Reponse Non applicable"])

# Deplacement
data["_8_Avez_vous_effectu_des_voya"]=data["_8_Avez_vous_effectu_des_voya"].replace([1,0],["Oui","Non"])
data["_8_Avez_vous_effectu_des_voya_001"]=data["_8_Avez_vous_effectu_des_voya_001"].replace([1,0],["Oui","Non"])
data["_9_Si_oui_la_question_8_veu/1"]=data["_9_Si_oui_la_question_8_veu/1"].replace(1,"Afrique")
data["_9_Si_oui_la_question_8_veu/2"]=data["_9_Si_oui_la_question_8_veu/2"].replace(1,"Asie")
data["_9_Si_oui_la_question_8_veu/3"]=data["_9_Si_oui_la_question_8_veu/3"].replace(1,"Europe")
data["_9_Si_oui_la_question_8_veu/4"]=data["_9_Si_oui_la_question_8_veu/4"].replace(1,"Amerique du Nord")
data["_9_Si_oui_la_question_8_veu/5"]=data["_9_Si_oui_la_question_8_veu/5"].replace(1,"Amerique du Sud")
data["_9_Si_oui_la_question_8_veu/999"]=data["_9_Si_oui_la_question_8_veu/999"].replace(1,"Refus")
data["_10_Dans_quelle_commune_habite"]=data["_10_Dans_quelle_commune_habite"].replace([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,999,888],["Cotonou","Abomey-Calavi","Allada","Ouidah","Tori-Bossito","Kpomassè","Toffo","Zè","So-Ava","Aguégués","Sèmè-Podji","Porto-Novo","Akpro-Missérété","Adjara","Atchoukpa",0,"Refus","Reponse Non applicable"])
data["_11_Au_cours_du_dern_s_avez_vous_visit_e_/1"]=data["_11_Au_cours_du_dern_s_avez_vous_visit_e_/1"].replace(1,"Communes du cordon sanitaire")
data["_11_Au_cours_du_dern_s_avez_vous_visit_e_/2"]=data["_11_Au_cours_du_dern_s_avez_vous_visit_e_/2"].replace(1,"Sud du Bénin(en dehors des communes du Cordon sanitaires)")
data["_11_Au_cours_du_dern_s_avez_vous_visit_e_/3"]=data["_11_Au_cours_du_dern_s_avez_vous_visit_e_/3"].replace(1,"Nord du Bénin(en dehors des communes du Cordon sanitaires)")
data["_11_Au_cours_du_dern_s_avez_vous_visit_e_/4"]=data["_11_Au_cours_du_dern_s_avez_vous_visit_e_/4"].replace(1,"Centre du Bénin(en dehors des communes du Cordon sanitaires)")
data["_11_Au_cours_du_dern_s_avez_vous_visit_e_/999"]=data["_11_Au_cours_du_dern_s_avez_vous_visit_e_/999"].replace(1,"Refus")

data["_12_Quel_s_sont_le_s_moyen_s/1"]=data["_12_Quel_s_sont_le_s_moyen_s/1"].replace([1],["Transport individuel"])
data["_12_Quel_s_sont_le_s_moyen_s/2"]=data["_12_Quel_s_sont_le_s_moyen_s/2"].replace([1],["Taxi-moto"])
data["_12_Quel_s_sont_le_s_moyen_s/3"]=data["_12_Quel_s_sont_le_s_moyen_s/3"].replace([1],["Transport en commun"])
data["_12_Quel_s_sont_le_s_moyen_s/999"]=data["_12_Quel_s_sont_le_s_moyen_s/999"].replace([1],["Refus"])

# Condition logement
data["_13_Dans_quel_type_d_habitatio"]=data["_13_Dans_quel_type_d_habitatio"].replace([1,2,3,4,5,888],["Une maison individuelle, haut et moyen standing (Villa)","Une maison, cour commune, faible standing","Un logement dans un immeuble collectif","Une construction provisoire, une habitation de fortune","Aucun logement (à la belle étoile)","Reponse Non applicable"])
data["_14_Combien_de_perso_pal_vous_y_compris_"]=data["_14_Combien_de_perso_pal_vous_y_compris_"].replace(888,"Refus")
data["_16_Est_ce_que_vous_oment_de_la_journ_e_"]=data["_16_Est_ce_que_vous_oment_de_la_journ_e_"].replace([0,1,2,3,4,5,6],["jamais","dans la journée" ,"dans la soirée","dans la nuit","dans la journée et la soirée","dans la journée et la nuit","dans la journée, nuit et soirée"])
data["_17_Utilisez_vous_un_oment_de_la_journ_e_"]=data["_17_Utilisez_vous_un_oment_de_la_journ_e_"].replace([0,1,2,3,4,5,6],["jamais","dans la journée" ,"dans la soirée","dans la nuit","dans la journée et la soirée","dans la journée et la nuit","dans la journée, nuit et soirée"])
data["_18_Utilisez_vous_un_oment_de_la_journ_e_"]=data["_18_Utilisez_vous_un_oment_de_la_journ_e_"].replace([0,1,2,3,4,5,6,999],["jamais","dans la journée" ,"dans la soirée","dans la nuit","dans la journée et la soirée","dans la journée et la nuit","dans la journée, nuit et soirée","Refus"])
data["_19_Quelle_source_d_approvisio"]=data["_19_Quelle_source_d_approvisio"].replace([0,1,2,3,4],["SONEB (robinet intégré à la construction)","Puit domestique " ,"Marécage ou basfonds" ,"Forage",0 ])
data["_20_Quel_dispositif_logement_principal_"]=data["_20_Quel_dispositif_logement_principal_"].replace([0,1,2,3,4],["Aucun dispositif disponible","Alimentation (Cuvette, robinet) d’eau seule","Alimentation d’eau avec du savon (liquide ou solide) seuls","Alimentation d’eau javellisée","Distributeur de gel hydro-alcoolique "])

#Condition de travail[PROFESSIONNEL]
data["_21_Dans_quelle_commune_se_tro"]=data["_21_Dans_quelle_commune_se_tro"].replace([0,1,2,3,999],["Communes du cordon sanitaire","Sud du Bénin(en dehors des communes du Cordon sanitaires)","Nord du Bénin(en dehors des communes du Cordon sanitaires)","Centre du Bénin(en dehors des communes du Cordon sanitaires)","Refus"])
data["_22_Dans_quel_type_de_b_timent"]=data["_22_Dans_quel_type_de_b_timent"].replace([1,2,3,4,5,6,7,8,9],["bureau","boutique","Atelier","Magasin","Hall du bâtiment","Hangar","Aucun bâtiment (pour les non-travailleurs sur site)","Classe(moins de 50 personnes)","Amphi(plus de 50 perseonnes)"])
data["_25_Utilisez_vous_un_oment_de_la_journ_e_"]=data["_25_Utilisez_vous_un_oment_de_la_journ_e_"].replace([0,1,2,3,4,5,6],["jamais","dans la journée" ,"dans la soirée","dans la nuit","dans la journée et la soirée","dans la journée et la nuit","dans la journée, nuit et soirée"])
data["_26_Utilisez_vous_un_ent_dans_la_journ_e_"]=data["_26_Utilisez_vous_un_ent_dans_la_journ_e_"].replace([0,1,2,3,4,5,6],["jamais","dans la journée" ,"dans la soirée","dans la nuit","dans la journée et la soirée","dans la journée et la nuit","dans la journée, nuit et soirée"])
data["_27_Quelle_source_d_approvisio"]=data["_27_Quelle_source_d_approvisio"].replace([0,1,2,3,4,999],["SONEB (robinet intégré à la construction)","Puit domestique " ,"Marécage ou basfonds" ,"Forage",0,"Refus" ])
data["_28_Quel_dispositif_e_travail_principal_"]=data["_28_Quel_dispositif_e_travail_principal_"].replace([0,1,2,3,4,999],["Aucun dispositif disponible","Alimentation (Cuvette, robinet) d’eau seule","Alimentation d’eau avec du savon (liquide ou solide) seuls ","Alimentation d’eau javellisée","Distributeur de gel hydro-alcoolique","Refus"])

#Pratiques relatives à la COVID-19
data["_30_O_avaient_elles_lieu_et_q/1"]=data["_30_O_avaient_elles_lieu_et_q/1"].replace(1,"A domicile")
data["_30_O_avaient_elles_lieu_et_q/2"]=data["_30_O_avaient_elles_lieu_et_q/2"].replace(1,"Chez un proche de la famille")
data["_30_O_avaient_elles_lieu_et_q/3"]=data["_30_O_avaient_elles_lieu_et_q/3"].replace(1,"Dans un lieu public")
data["_30_O_avaient_elles_lieu_et_q/999"]=data["_30_O_avaient_elles_lieu_et_q/999"].replace(1,"Refus")
data["_41_1_Aviez_vous_l_i_taient_respect_es_"]=data["_41_1_Aviez_vous_l_i_taient_respect_es_"].replace([0,1,2],["Oui","Non","Ne sais pas"])
data["_43_1_Aviez_vous_l_i_taient_respect_es_"]=data["_43_1_Aviez_vous_l_i_taient_respect_es_"].replace([0,1,2],["Oui","Non","Ne sais pas"])
data["_45_1_Aviez_vous_l_i_taient_respect_es_"]=data["_45_1_Aviez_vous_l_i_taient_respect_es_"].replace([0,1,2],["Oui","Non","Ne sais pas"])
data["_45_bis_1_2_Aviez_vo_taient_respect_es_"]=data["_45_bis_1_2_Aviez_vo_taient_respect_es_"].replace([0,1,2],["Oui","Non","Ne sais pas"])
data["_45_bis_2_2_Aviez_vo_taient_respect_es_"]=data["_45_bis_2_2_Aviez_vo_taient_respect_es_"].replace([0,1,2],["Oui","Non","Ne sais pas"])
data["_46_1_Aviez_vous_l_i_taient_respect_es_"]=data["_46_1_Aviez_vous_l_i_taient_respect_es_"].replace([0,1,2],["Oui","Non","Ne sais pas"])
data["_47_Quel_type_de_masque_avez_v"]=data["_47_Quel_type_de_masque_avez_v"].replace([0,1,2,3,4],["Aucun masque","masque en tissus","masque chirurgical","FFP2","visière plastique "])

data["_47_1_Quelles_sont_les_raisons/0"]=data["_47_1_Quelles_sont_les_raisons/0"].replace(1,"Aucune raison particulière")
data["_47_1_Quelles_sont_les_raisons/1"]=data["_47_1_Quelles_sont_les_raisons/1"].replace(1,"Choix personnel sans raison particulière")
data["_47_1_Quelles_sont_les_raisons/2"]=data["_47_1_Quelles_sont_les_raisons/2"].replace(1,"Accessibilité financière")
data["_47_1_Quelles_sont_les_raisons/3"]=data["_47_1_Quelles_sont_les_raisons/3"].replace(1,"Accessibilité geographique ")
data["_47_1_Quelles_sont_les_raisons/4"]=data["_47_1_Quelles_sont_les_raisons/4"].replace(1,"Possibilité de réutilisation ")
data["_47_1_Quelles_sont_les_raisons/5"]=data["_47_1_Quelles_sont_les_raisons/5"].replace(1,"Bonne protection")
data["_47_1_Quelles_sont_les_raisons/6"]=data["_47_1_Quelles_sont_les_raisons/6"].replace(1,"Confort à porter")

data["_47_2_Si_pas_de_port_du_masque/0"]=data["_47_2_Si_pas_de_port_du_masque/0"].replace(1,"Aucune raison particulière")
data["_47_2_Si_pas_de_port_du_masque/1"]=data["_47_2_Si_pas_de_port_du_masque/1"].replace(1,"Cherté du masque")
data["_47_2_Si_pas_de_port_du_masque/2"]=data["_47_2_Si_pas_de_port_du_masque/2"].replace(1,"Douleur aux oreilles")
data["_47_2_Si_pas_de_port_du_masque/3"]=data["_47_2_Si_pas_de_port_du_masque/3"].replace(1,"Picotement de la peau ")
data["_47_2_Si_pas_de_port_du_masque/4"]=data["_47_2_Si_pas_de_port_du_masque/4"].replace(1,"difficulté à respirer")
data["_47_2_Si_pas_de_port_du_masque/5"]=data["_47_2_Si_pas_de_port_du_masque/5"].replace(1,"Chaleur")
data["_47_2_Si_pas_de_port_du_masque/6"]=data["_47_2_Si_pas_de_port_du_masque/6"].replace(1,"Difficulté à réutiliser")
data["_47_2_Si_pas_de_port_du_masque/7"]=data["_47_2_Si_pas_de_port_du_masque/7"].replace(1,"Fragilité")

data["_48_1_A_quelle_fr_qu_d_autres_personnes_"]=data["_48_1_A_quelle_fr_qu_d_autres_personnes_"].replace([0,1,2,3,999],["jamais","rarement","souvent","toujours","Refus"])
data["_48_2_A_quelle_fr_qu_masque_dans_la_rue_"]=data["_48_2_A_quelle_fr_qu_masque_dans_la_rue_"].replace([0,1,2,3,999],["jamais","rarement","souvent","toujours","Refus"])
data["_48_3_A_quelle_fr_qu_uel_voiture_moto_"]=data["_48_3_A_quelle_fr_qu_uel_voiture_moto_"].replace([0,1,2,3,999],["jamais","rarement","souvent","toujours","Refus"])
data["_48_4_A_quelle_fr_qu_voiture_moto_bus_"]=data["_48_4_A_quelle_fr_qu_voiture_moto_bus_"].replace([0,1,2,3,999],["jamais","rarement","souvent","toujours","Refus"])
data["_48_5_A_quelle_fr_qu_d_autres_personnes_"]=data["_48_5_A_quelle_fr_qu_d_autres_personnes_"].replace([0,1,2,3,999],["jamais","rarement","souvent","toujours","Refus"])
data["_48_6_A_quelle_fr_qu_ch_lieu_religieux_"]=data["_48_6_A_quelle_fr_qu_ch_lieu_religieux_"].replace([0,1,2,3,999],["jamais","rarement","souvent","toujours","Refus"])

data["_49_A_quelle_fr_quen_viez_vous_ce_masque_"]=data["_49_A_quelle_fr_quen_viez_vous_ce_masque_"].replace([0,1,2,3,4,999],["deux fois ou plus par jour ","une fois par jour ","tous les deux jours ","tous les trois jours ","Plus de trois jours ","Refus"])
data["_49_1_Vous_arrivait_du_menton_ou_du_cou_"]=data["_49_1_Vous_arrivait_du_menton_ou_du_cou_"].replace([0,1,999],["Non","Oui","Refus"])
data["_50_A_quelle_fr_quen_ch_lieu_religieux_"]=data["_50_A_quelle_fr_quen_ch_lieu_religieux_"].replace([0,1,2,3,999],["jamais","rarement","souvent","toujours","Refus"])
data["_51_A_quelle_fr_quen_ch_lieu_religieux_"]=data["_51_A_quelle_fr_quen_ch_lieu_religieux_"].replace([0,1,2,3,999],["jamais","rarement","souvent","toujours","Refus"])
data["_52_A_quelle_fr_quen_r_de_votre_logement_"]=data["_52_A_quelle_fr_quen_r_de_votre_logement_"].replace([0,1,2,3,999],["jamais","rarement","souvent","toujours","Refus"])
data["_53_A_quelle_fr_quen_ur_votre_logement_"]=data["_53_A_quelle_fr_quen_ur_votre_logement_"].replace([0,1,2,3,999],["jamais","rarement","souvent","toujours","Refus"])
data["_54_A_quelle_fr_quen_n_e_de_porte_cl_s_"]=data["_54_A_quelle_fr_quen_n_e_de_porte_cl_s_"].replace([0,1,2,3,999],["jamais","rarement","souvent","toujours","Refus"])

data["_55_A_quelle_fr_quen_de_mani_re_g_n_rale_"]=data["_55_A_quelle_fr_quen_de_mani_re_g_n_rale_"].replace([0,1,2,3,999],["jamais","une fois par jour ","deux fois par jour ","plus de deux fois par jour ","Refus"])
data["_56_A_quelle_fr_quen_urs_du_dernier_mois_"]=data["_56_A_quelle_fr_quen_urs_du_dernier_mois_"].replace([0,1,2,3,999],["jamais","une à trois fois par semaine ","quatre à six fois par semaine ","tous les jours","Refus"])
data["_57_A_quelle_fr_quen_urs_du_dernier_mois_"]=data["_57_A_quelle_fr_quen_urs_du_dernier_mois_"].replace([0,1,2,3,999],["jamais","une à trois fois par semaine ","quatre à six fois par semaine ","tous les jours","Refus"])

data["_58_A_quelle_fr_quen_urs_du_dernier_mois_"]=data["_58_A_quelle_fr_quen_urs_du_dernier_mois_"].replace([0,1,2,3,999],["jamais","rarement","souvent","très souvet","Refus"])
data["_59_A_quelle_fr_quen_urs_du_dernier_mois_"]=data["_59_A_quelle_fr_quen_urs_du_dernier_mois_"].replace([0,1,2,3,999],["jamais","rarement","souvent","très souvet","Refus"])

data["_61_Avez_vous_t_malade_au_co"]=data["_61_Avez_vous_t_malade_au_co"].replace([0,1],["Non","Oui"])
data["_62_Si_oui_avez_vous_eu_recou/1"]=data["_62_Si_oui_avez_vous_eu_recou/1"].replace(1,"un hôpital public")
data["_62_Si_oui_avez_vous_eu_recou/2"]=data["_62_Si_oui_avez_vous_eu_recou/2"].replace(1,"un centre de santé publique  ")
data["_62_Si_oui_avez_vous_eu_recou/3"]=data["_62_Si_oui_avez_vous_eu_recou/3"].replace(1,"une clinique ou cabinet privés ")
data["_62_Si_oui_avez_vous_eu_recou/4"]=data["_62_Si_oui_avez_vous_eu_recou/4"].replace(1,"un guérisseur traditionnel ")
data["_62_Si_oui_avez_vous_eu_recou/5"]=data["_62_Si_oui_avez_vous_eu_recou/5"].replace(1,"un lieu de prière ")
data["_62_Si_oui_avez_vous_eu_recou/6"]=data["_62_Si_oui_avez_vous_eu_recou/6"].replace(1,"de l’automédication  ")

# Perception sur la covid 19
data["_63_Selon_vous_qu_est_ce_que_/1"]=data["_63_Selon_vous_qu_est_ce_que_/1"].replace(1,"maladie due à un virus")
data["_63_Selon_vous_qu_est_ce_que_/2"]=data["_63_Selon_vous_qu_est_ce_que_/2"].replace(1,"épidémie/ pandémie")
data["_63_Selon_vous_qu_est_ce_que_/3"]=data["_63_Selon_vous_qu_est_ce_que_/3"].replace(1,"maladie provenant de la Chine")
data["_63_Selon_vous_qu_est_ce_que_/4"]=data["_63_Selon_vous_qu_est_ce_que_/4"].replace(1,"maladie créée par les chinois  ")
data["_63_Selon_vous_qu_est_ce_que_/5"]=data["_63_Selon_vous_qu_est_ce_que_/5"].replace(1,"maladie d’origine divine ")

data["_64_Avez_vous_peur_de_la_COVID_19_"]=data["_64_Avez_vous_peur_de_la_COVID_19_"].replace([0,1],["Non","Oui"])
data["_65_Pour_quelles_raisons_pense/0"]=data["_65_Pour_quelles_raisons_pense/0"].replace(1,"aucune raison particulière")
data["_65_Pour_quelles_raisons_pense/1"]=data["_65_Pour_quelles_raisons_pense/1"].replace(1,"maladie affectant beaucoup de personnes")
data["_65_Pour_quelles_raisons_pense/2"]=data["_65_Pour_quelles_raisons_pense/2"].replace(1,"maladie très contagieuse  ")
data["_65_Pour_quelles_raisons_pense/3"]=data["_65_Pour_quelles_raisons_pense/3"].replace(1,"maladie mortelle   ")
data["_65_Pour_quelles_raisons_pense/4"]=data["_65_Pour_quelles_raisons_pense/4"].replace(1,"maladie qui impacte l’économie   ")
data["_65_Pour_quelles_raisons_pense/5"]=data["_65_Pour_quelles_raisons_pense/5"].replace(1,"maladie qui n’a pas de vaccin  ")
data["_65_Pour_quelles_raisons_pense/6"]=data["_65_Pour_quelles_raisons_pense/6"].replace(1,"maladie qui n’a pas de traitement efficace  ")
data["_65_Pour_quelles_raisons_pense/7"]=data["_65_Pour_quelles_raisons_pense/7"].replace(1,"maladie non totalement comprise par la science  ")

data["_66_Quelles_ont_t_les_cons_q/0"]=data["_66_Quelles_ont_t_les_cons_q/0"].replace(1,"aucune")
data["_66_Quelles_ont_t_les_cons_q/1"]=data["_66_Quelles_ont_t_les_cons_q/1"].replace(1,"méfiance")
data["_66_Quelles_ont_t_les_cons_q/2"]=data["_66_Quelles_ont_t_les_cons_q/2"].replace(1,"réduction des sorties ")
data["_66_Quelles_ont_t_les_cons_q/3"]=data["_66_Quelles_ont_t_les_cons_q/3"].replace(1,"distanciation sociale ")
data["_66_Quelles_ont_t_les_cons_q/4"]=data["_66_Quelles_ont_t_les_cons_q/4"].replace(1,"désolidarisation")
data["_66_Quelles_ont_t_les_cons_q/5"]=data["_66_Quelles_ont_t_les_cons_q/5"].replace(1,"stigmatisation des personnes malades ")
data["_66_Quelles_ont_t_les_cons_q/6"]=data["_66_Quelles_ont_t_les_cons_q/6"].replace(1,"décès de personnes proches ")
data["_66_Quelles_ont_t_les_cons_q/7"]=data["_66_Quelles_ont_t_les_cons_q/7"].replace(1,"plus d’importance accordée à la santé ")
data["_66_Quelles_ont_t_les_cons_q/8"]=data["_66_Quelles_ont_t_les_cons_q/8"].replace(1,"moins d’importance accordée à la santé ")

data["_67_Quelles_ont_t_les_cons_q/0"]=data["_67_Quelles_ont_t_les_cons_q/0"].replace(1,"aucune")
data["_67_Quelles_ont_t_les_cons_q/1"]=data["_67_Quelles_ont_t_les_cons_q/1"].replace(1,"réduction des sorties  ")
data["_67_Quelles_ont_t_les_cons_q/2"]=data["_67_Quelles_ont_t_les_cons_q/2"].replace(1,"modification de l’alimentation  ")
data["_67_Quelles_ont_t_les_cons_q/3"]=data["_67_Quelles_ont_t_les_cons_q/3"].replace(1,"hygiène accrue  ")
data["_67_Quelles_ont_t_les_cons_q/4"]=data["_67_Quelles_ont_t_les_cons_q/4"].replace(1,"réduction de la sexualité ")
data["_67_Quelles_ont_t_les_cons_q/5"]=data["_67_Quelles_ont_t_les_cons_q/5"].replace(1,"attention accrue aux informations dans les médias ")

data["_68_Quelles_ont_t_les_cons_q/0"]=data["_68_Quelles_ont_t_les_cons_q/0"].replace(1,"aucune")
data["_68_Quelles_ont_t_les_cons_q/1"]=data["_68_Quelles_ont_t_les_cons_q/1"].replace(1,"baisse des revenus ")
data["_68_Quelles_ont_t_les_cons_q/2"]=data["_68_Quelles_ont_t_les_cons_q/2"].replace(1,"augmentation des revenus  ")
data["_68_Quelles_ont_t_les_cons_q/3"]=data["_68_Quelles_ont_t_les_cons_q/3"].replace(1,"fermeture des commerces et ateliers")
data["_68_Quelles_ont_t_les_cons_q/4"]=data["_68_Quelles_ont_t_les_cons_q/4"].replace(1,"plus de chômage")
data["_68_Quelles_ont_t_les_cons_q/5"]=data["_68_Quelles_ont_t_les_cons_q/5"].replace(1,"moins de chômage")
data["_68_Quelles_ont_t_les_cons_q/6"]=data["_68_Quelles_ont_t_les_cons_q/6"].replace(1,"augmentation des dépenses des ménages")
data["_68_Quelles_ont_t_les_cons_q/7"]=data["_68_Quelles_ont_t_les_cons_q/7"].replace(1,"baisse des dépenses des ménages ")
data["_68_Quelles_ont_t_les_cons_q/8"]=data["_68_Quelles_ont_t_les_cons_q/8"].replace(1,"augmentation des charges financières pour les travailleurs et entreprise ")
data["_68_Quelles_ont_t_les_cons_q/9"]=data["_68_Quelles_ont_t_les_cons_q/9"].replace(1,"baisse des charges financières pour les travailleurs et entreprises ")
data["_68_Quelles_ont_t_les_cons_q/10"]=data["_68_Quelles_ont_t_les_cons_q/10"].replace(1,"plus d’aides de l’Etat aux travailleurs et entreprises  ")
data["_68_Quelles_ont_t_les_cons_q/11"]=data["_68_Quelles_ont_t_les_cons_q/11"].replace(1,"moins d’aides de l’Etat aux travailleurs et entreprises ")

data["_69_Quelles_ont_t_les_cons_q/0"]=data["_69_Quelles_ont_t_les_cons_q/0"].replace(1,"aucune")
data["_69_Quelles_ont_t_les_cons_q/1"]=data["_69_Quelles_ont_t_les_cons_q/1"].replace(1,"réduction fréquentation des formations sanitaires ")
data["_69_Quelles_ont_t_les_cons_q/2"]=data["_69_Quelles_ont_t_les_cons_q/2"].replace(1," augmentation fréquentation des formations sanitaires ")
data["_69_Quelles_ont_t_les_cons_q/3"]=data["_69_Quelles_ont_t_les_cons_q/3"].replace(1,"meilleure organisation des soins dans les formations sanitaires ")
data["_69_Quelles_ont_t_les_cons_q/4"]=data["_69_Quelles_ont_t_les_cons_q/4"].replace(1,"moins bonne organisation des soins dans les formations sanitaires ")

data["_70_Selon_vous_y_a_t_il_des_c/0"]=data["_70_Selon_vous_y_a_t_il_des_c/0"].replace(1,"aucune")
data["_70_Selon_vous_y_a_t_il_des_c/1"]=data["_70_Selon_vous_y_a_t_il_des_c/1"].replace(1," consommation d’animaux sauvages (pangolin et chauve-souris) ")
data["_70_Selon_vous_y_a_t_il_des_c/2"]=data["_70_Selon_vous_y_a_t_il_des_c/2"].replace(1,"mauvaises manipulations de laboratoires en Chine ")
data["_70_Selon_vous_y_a_t_il_des_c/3"]=data["_70_Selon_vous_y_a_t_il_des_c/3"].replace(1,"péchés ou mauvaises conduites morales des hommes ")
data["_70_Selon_vous_y_a_t_il_des_c/4"]=data["_70_Selon_vous_y_a_t_il_des_c/4"].replace(1,"volonté des riches de s’enrichir en vendant un vaccin")
data["_70_Selon_vous_y_a_t_il_des_c/5"]=data["_70_Selon_vous_y_a_t_il_des_c/5"].replace(1,"en vadant un vaccin")

data["_71_Est_ce_que_la_Covid_19_peu"]=data["_71_Est_ce_que_la_Covid_19_peu"].replace([0,1],["Oui","Non"])
data["_72_Si_oui_comment_"]=data["_72_Si_oui_comment_"].replace("888","Reponse Non Appplicable")
data["_73_Si_non_pourquoi_"]=data["_73_Si_non_pourquoi_"].replace("888","Reponse Non Appplicable")

#H- Connaissances sur la COVID-19

data["_74_Comment_la_COVID_19_se_tra/0"]=data["_74_Comment_la_COVID_19_se_tra/0"].replace(1,"Ne sait pas")
data["_74_Comment_la_COVID_19_se_tra/1"]=data["_74_Comment_la_COVID_19_se_tra/1"].replace(1," par contact direct avec le virus ")
data["_74_Comment_la_COVID_19_se_tra/2"]=data["_74_Comment_la_COVID_19_se_tra/2"].replace(1,"par contact indirect par l’intermédiaire d’objets ou de surfaces contaminés par le virus ")
data["_74_Comment_la_COVID_19_se_tra/3"]=data["_74_Comment_la_COVID_19_se_tra/3"].replace(1,"par contact avec les sécrétions buccales (salive) lorsqu’une personne infectée tousse, parle ou chante ")
data["_74_Comment_la_COVID_19_se_tra/4"]=data["_74_Comment_la_COVID_19_se_tra/4"].replace(1,"par contact avec les sécrétions nasales (morve) lorsqu’une personne infectée éternue")
data["_74_Comment_la_COVID_19_se_tra/5"]=data["_74_Comment_la_COVID_19_se_tra/5"].replace(1,"lorsqu’on se touche les yeux, le nez ou la bouche avec les mains, après avoir touché un objet ou une surface contaminée par le virus")
data["_74_Comment_la_COVID_19_se_tra/6"]=data["_74_Comment_la_COVID_19_se_tra/6"].replace(1,"lorsqu’on se touche les yeux, le nez ou la bouche avec les mains, après avoir touché un objet ou une surface contaminée par le virus")

data["_75_Quels_sont_les_sympt_mes_d/0"]=data["_75_Quels_sont_les_sympt_mes_d/0"].replace(1,"Aucun")
data["_75_Quels_sont_les_sympt_mes_d/1"]=data["_75_Quels_sont_les_sympt_mes_d/1"].replace(1,"Toux ")
data["_75_Quels_sont_les_sympt_mes_d/2"]=data["_75_Quels_sont_les_sympt_mes_d/2"].replace(1,"Fièvre")
data["_75_Quels_sont_les_sympt_mes_d/3"]=data["_75_Quels_sont_les_sympt_mes_d/3"].replace(1,"Maux de tête")
data["_75_Quels_sont_les_sympt_mes_d/4"]=data["_75_Quels_sont_les_sympt_mes_d/4"].replace(1,"Essoufflement")
data["_75_Quels_sont_les_sympt_mes_d/5"]=data["_75_Quels_sont_les_sympt_mes_d/5"].replace(1,"Mal de gorge")
data["_75_Quels_sont_les_sympt_mes_d/6"]=data["_75_Quels_sont_les_sympt_mes_d/6"].replace(1,"Douleur musculaire non attribuée à une activité spécifique ")
data["_75_Quels_sont_les_sympt_mes_d/7"]=data["_75_Quels_sont_les_sympt_mes_d/7"].replace(1,"Diarrhée")
data["_75_Quels_sont_les_sympt_mes_d/8"]=data["_75_Quels_sont_les_sympt_mes_d/8"].replace(1,"Conjonctivite")
data["_75_Quels_sont_les_sympt_mes_d/9"]=data["_75_Quels_sont_les_sympt_mes_d/9"].replace(1,"Perte de l’odorat ou du goût")
data["_75_Quels_sont_les_sympt_mes_d/10"]=data["_75_Quels_sont_les_sympt_mes_d/10"].replace(1,"Eruption cutanée, ou décoloration des doigts ou des orteils")
data["_75_Quels_sont_les_sympt_mes_d/11"]=data["_75_Quels_sont_les_sympt_mes_d/11"].replace(1,"Perte d’élocution ou de motricité")

data["_76_Quelles_sont_les_personnes/0"]=data["_76_Quelles_sont_les_personnes/0"].replace(1,"Aucunes")
data["_76_Quelles_sont_les_personnes/1"]=data["_76_Quelles_sont_les_personnes/1"].replace(1,"Les femmes enceintes ou les femmes dont la grossesse a pris fin récemment")
data["_76_Quelles_sont_les_personnes/2"]=data["_76_Quelles_sont_les_personnes/2"].replace(1,"Les personnes en surpoids ou obèsest")
data["_76_Quelles_sont_les_personnes/3"]=data["_76_Quelles_sont_les_personnes/3"].replace(1,"Les personnes qui souffrent de diabète")
data["_76_Quelles_sont_les_personnes/4"]=data["_76_Quelles_sont_les_personnes/4"].replace(1,"Les personnes qui souffrent d’hypertension artérielle")
data["_76_Quelles_sont_les_personnes/5"]=data["_76_Quelles_sont_les_personnes/5"].replace(1,"Les personnes qui souffrent de problèmes respiratoires (cardiopathie, pneumopathie)")
data["_76_Quelles_sont_les_personnes/6"]=data["_76_Quelles_sont_les_personnes/6"].replace(1,"Les personnes qui souffrent de cancer")
data["_76_Quelles_sont_les_personnes/7"]=data["_76_Quelles_sont_les_personnes/7"].replace(1,"Les personnes âgées")

data["_77_Quelle_est_la_p_tion_du_coronavirus_"]=data["_77_Quelle_est_la_p_tion_du_coronavirus_"].replace([999,888],["Refus","Reponse non applicable"])
data["_78_Des_personnes_pe_senter_de_sympt_mes_"]=data["_78_Des_personnes_pe_senter_de_sympt_mes_"].replace([0,1,999,888],["Faux","Vrai","Refus","Reponse Non applicable"])
data["_79_Une_personne_qui_une_autre_personne_"]=data["_79_Une_personne_qui_une_autre_personne_"].replace([0,1,999,888],["Faux","Vrai","Refus","Reponse Non applicable"])
data["_80_Pour_se_prot_ger_de_la_COV/0"]=data["_80_Pour_se_prot_ger_de_la_COV/0"].replace(1,"Aucune")
data["_80_Pour_se_prot_ger_de_la_COV/1"]=data["_80_Pour_se_prot_ger_de_la_COV/1"].replace(1,"Ne sortir que pour l’essentiel")
data["_80_Pour_se_prot_ger_de_la_COV/2"]=data["_80_Pour_se_prot_ger_de_la_COV/2"].replace(1,"Respecter la distanciation sociale d’1 mètre")
data["_80_Pour_se_prot_ger_de_la_COV/3"]=data["_80_Pour_se_prot_ger_de_la_COV/3"].replace(1,"Limiter les contacts étroits entre les personnes contagieuses et les autres")
data["_80_Pour_se_prot_ger_de_la_COV/4"]=data["_80_Pour_se_prot_ger_de_la_COV/4"].replace(1,"Recourir au télétravail si possible")
data["_80_Pour_se_prot_ger_de_la_COV/5"]=data["_80_Pour_se_prot_ger_de_la_COV/5"].replace(1,"Se laver les mains à l’eau et au savon fréquemment")
data["_80_Pour_se_prot_ger_de_la_COV/6"]=data["_80_Pour_se_prot_ger_de_la_COV/6"].replace(1,"Se couvrir la bouche avec un mouchoir ou le creux du coude lorsqu’on éternue ou tousse")
data["_80_Pour_se_prot_ger_de_la_COV/7"]=data["_80_Pour_se_prot_ger_de_la_COV/7"].replace(1,"Porter un masque en tissu lorsque la distanciation physique n’est pas possible")
data["_80_Pour_se_prot_ger_de_la_COV/8"]=data["_80_Pour_se_prot_ger_de_la_COV/8"].replace(1,"S’informer chaque jour sur l’évolution de la situation")

data["_81_Quelles_sont_vos_sources_d/1"]=data["_81_Quelles_sont_vos_sources_d/1"].replace(1,"La télévision")
data["_81_Quelles_sont_vos_sources_d/2"]=data["_81_Quelles_sont_vos_sources_d/2"].replace(1,"La radio")
data["_81_Quelles_sont_vos_sources_d/3"]=data["_81_Quelles_sont_vos_sources_d/3"].replace(1,"les journeaux")
data["_81_Quelles_sont_vos_sources_d/4"]=data["_81_Quelles_sont_vos_sources_d/4"].replace(1,"les réseaux sociaux")
data["_81_Quelles_sont_vos_sources_d/5"]=data["_81_Quelles_sont_vos_sources_d/5"].replace(1,"site internet de l'état beninois")
data["_81_Quelles_sont_vos_sources_d/6"]=data["_81_Quelles_sont_vos_sources_d/6"].replace(1,"site internet de l'OMS")
data["_81_Quelles_sont_vos_sources_d/7"]=data["_81_Quelles_sont_vos_sources_d/7"].replace(1,"les proches")
data["_81_Quelles_sont_vos_sources_d/8"]=data["_81_Quelles_sont_vos_sources_d/8"].replace(1,"collegues de travails")
data["_82_En_laquelle_de_ces_sources"]=data["_82_En_laquelle_de_ces_sources"].replace([0,1,2,3,4,5,6,7,8,999],["La télévision","La radio","les journeaux","les réseaux sociaux","site internet de l'état beninois","site internet de l'OMS","les proches","collegues de travails",0,"Refus"])
data["_83_Pensez_vous_que_les_inform"]=data["_83_Pensez_vous_que_les_inform"].replace([0,1,999],["Oui","Non","Refus"])
data["_84_Quel_est_votre_d_es_gouvernementales_"]=data["_84_Quel_est_votre_d_es_gouvernementales_"].replace([1,2,3,4,999],["Totalement confiant","Trés confiant","Peu confiant","Pas du tout confiant","Refus"])
data["_84_1_Quel_est_votre_ons_internationales_"]=data["_84_1_Quel_est_votre_ons_internationales_"].replace([1,2,3,4,999],["Totalement confiant","Trés confiant","Peu confiant","Pas du tout confiant","Refus"])
data["_84_2_Quel_est_votre_es_dans_les_m_dias_"]=data["_84_2_Quel_est_votre_es_dans_les_m_dias_"].replace([1,2,3,4,999],["Totalement confiant","Trés confiant","Peu confiant","Pas du tout confiant","Refus"])


#I- Données sanitaires
data["_85_Quel_est_votre_s_terme_du_d_pistage_"]=data["_85_Quel_est_votre_s_terme_du_d_pistage_"].replace([0,1,2,999],["dépisté négatif","dépisté positif asymptomatique ","dépisté positif symptomatique","Refus"])

data["_86_Quelles_raisons_vous_ont_m/0"]=data["_86_Quelles_raisons_vous_ont_m/0"].replace(1,"Aucune raison particulière ")
data["_86_Quelles_raisons_vous_ont_m/1"]=data["_86_Quelles_raisons_vous_ont_m/1"].replace(1,"Cas confirmés ou suspectés parmi les proches de la famille")
data["_86_Quelles_raisons_vous_ont_m/2"]=data["_86_Quelles_raisons_vous_ont_m/2"].replace(1,"Cas confirmés ou suspectés parmi les amis ou collègues de travail")
data["_86_Quelles_raisons_vous_ont_m/3"]=data["_86_Quelles_raisons_vous_ont_m/3"].replace(1,"Suspicion personnelle suite à des symptômes")
data["_86_Quelles_raisons_vous_ont_m/4"]=data["_86_Quelles_raisons_vous_ont_m/4"].replace(1,"Dépistage non volontaire :")
data["_86_Quelles_raisons_vous_ont_m/5"]=data["_86_Quelles_raisons_vous_ont_m/5"].replace(1,"Dépistage pour raisons de voyage")
data["_86_Quelles_raisons_vous_ont_m/6"]=data["_86_Quelles_raisons_vous_ont_m/6"].replace(1,"Dépistage volontaire sans symptômes")
data["_86_Quelles_raisons_vous_ont_m/7"]=data["_86_Quelles_raisons_vous_ont_m/7"].replace(1,"Dépistage volontaire sans symptômes")

data["_88_Combien_de_cas_o_es_0_si_aucun_cas"]=data["_88_Combien_de_cas_o_es_0_si_aucun_cas"].replace([888,999],["Reponse non applicable","Refus"])
data["_89_Quels_sont_les_m_contre_la_COVID_19_"]=data["_89_Quels_sont_les_m_contre_la_COVID_19_"].replace([0,888,999],["Aucun","Reponse non applicable","Refus"])

data.loc[data['_86_Quelles_raisons_vous_ont_m/0'] == 0].iloc[1:20,295:].head()
#data.iloc[1:20,295].head()

data["_90_Souffrez_vous_de_/0"]=data["_90_Souffrez_vous_de_/0"].replace(1,"Aucune maladie chronique")
data["_90_Souffrez_vous_de_/1"]=data["_90_Souffrez_vous_de_/1"].replace(1,"Surpoids ou obésité")
data["_90_Souffrez_vous_de_/2"]=data["_90_Souffrez_vous_de_/2"].replace(1,"Diabète ")
data["_90_Souffrez_vous_de_/3"]=data["_90_Souffrez_vous_de_/3"].replace(1,"Hypertension artérielle")
data["_90_Souffrez_vous_de_/4"]=data["_90_Souffrez_vous_de_/4"].replace(1,"Hépatite chronique")
data["_90_Souffrez_vous_de_/5"]=data["_90_Souffrez_vous_de_/5"].replace(1,"Drépanocytose (SS, SC) ")
data["_90_Souffrez_vous_de_/6"]=data["_90_Souffrez_vous_de_/6"].replace(1,"Tuberculose")
data["_90_Souffrez_vous_de_/7"]=data["_90_Souffrez_vous_de_/7"].replace(1,"Cancer")
data["_90_Souffrez_vous_de_/8"]=data["_90_Souffrez_vous_de_/8"].replace(1,"VIH sous ARV")
data["_90_Souffrez_vous_de_/9"]=data["_90_Souffrez_vous_de_/9"].replace(1,"VIH sans ARV")
data["_90_Souffrez_vous_de_/10"]=data["_90_Souffrez_vous_de_/10"].replace(1,"Problèmes respiratoires")
data["_90_Souffrez_vous_de_/11"]=data["_90_Souffrez_vous_de_/11"].replace(1,"Maladies rénales chroniques")

data["_91_tes_vous_encein_accouch_r_cemment_"]=data["_91_tes_vous_encein_accouch_r_cemment_"].replace([0,1],["Non","Oui"])
data["_92_Fumez_vous_du_tabac_"]=data["_92_Fumez_vous_du_tabac_"].replace([0,1],["Non","Oui"])
data["_93_tes_vous_un_consommateur_"]=data["_93_tes_vous_un_consommateur_"].replace([0,1],["Non","Oui"])
data["_94_Si_oui_en_avez_v_consommer_en_groupe_"]=data["_94_Si_oui_en_avez_v_consommer_en_groupe_"].replace([0,1],["Non","Oui"])

data["_96_quelle_fr_quence_vous_ar"]=data["_96_quelle_fr_quence_vous_ar"].replace([0,1,2,3,4,999,888],["jamais","une fois par mois ou moins","deux à quatre fois par mois","deux à trois fois par semaine","quatre fois ou plus par semaine ","Refus","Reponse Non applicable"])
data["_97_Combien_de_verre_talopk_mi_de_sodabi"]=data["_97_Combien_de_verre_talopk_mi_de_sodabi"].replace([0,1,2,3,4,999],["un ou deux ","trois ou quatre ","cinq ou six","sept à neuf","dix ou plus","Refus"])

data["_98_Au_cours_d_une_m_s_standards_ou_plus_"]=data["_98_Au_cours_d_une_m_s_standards_ou_plus_"].replace([0,1,2,3,4,999],["jamais ","moins d’une fois par mois","une fois par mois ","une fois par semaine ","chaque jour ou presque","Refus"])

data["_99_Avez_vous_consta_derni_res_semaines_"]=data["_99_Avez_vous_consta_derni_res_semaines_"].replace([0,1],["Non","Oui"])
data["_100_Avez_vous_const_derni_res_semaines_"]=data["_100_Avez_vous_const_derni_res_semaines_"].replace([0,1],["Non","Oui"])

# data.iloc[1:20,295:].head()
data["_60_A_quelle_fr_quen_urs_du_dernier_mois_"]=data["_60_A_quelle_fr_quen_urs_du_dernier_mois_"].replace([0,1,2,3],["jamais","rarement","souvent","Très souvent"])

# data.iloc[1:20,295:].head()


# In[4]:


import math
db=myclient.testrestoredb
donnee_Identification=db["donnee_Identification"]
donnee_Geographique=db["donnee_Geographique"]
donnee_consultation=db["donnee_consultation"]
condition_Logement=db["condition_Logement"]
condition_Professsionnelle=db["condition_Professionnelle"]
connaissance_covid19=db["connaissance_covid19"]
donnee_socioDemographique=db["donnee_socioDemographique"]
pratique_covid19=db["pratique_covid19"]
perception_covid19=db["perception_covid19"]
historique_sante=db["historique_sante"]
nutrition=db["nutrition"]




for  i in range(0,len(data.iloc[0:,0])): 
    
    # chargement de la dimension donnee identification
    
    donnee_Ident=dict()
    donnee_Ident["_id"]=i
    donnee_Ident["axe_recherche"]="WP2"
    donnee_Ident["site"]=data.loc[i,"Site_"] 
    donnee_Ident["initiales"]=data.loc[i,"Initiales_"]
    donnee_Ident["code_enquete"]=int(data.loc[i,"Code_enqu_t_"])
    donnee_Ident["code_interview"]=data.loc[i,"Code_interviewer_"]
    donnee_Ident["heure_de_debut"]=data.loc[i,"Heure_de_d_but_interview"]
    donnee_Ident["heure_de_fin"]=data.loc[i,"Heure_de_fin_interview"]
    #donnee_Identification.insert_one(donnee_Ident);
    
    # # chargement de la dimension donnee socio_demographique
    donnee_Socio=dict()
    donnee_Socio["_id"]=i
    donnee_Socio["sexe"]=data.loc[i,"_1_Sexe_"]
    donnee_Socio["statut_matrimonial"]=data.loc[i,"_1_1_Quel_est_votre_statut_mat"]
    donnee_Socio["age"]=int(data.loc[i,"_2_Quel_est_votre_ge_r_volu_"])
    donnee_Socio["groupe_socio_culturel"]=data.loc[i,"_3_De_quel_groupe_socio_cultur"] if str(data.loc[i,"_3_De_quel_groupe_socio_cultur"]) != "0" else data.loc[i,"_3_11_Autre_pr_ciser"] 
    donnee_Socio["religion"]=data.loc[i,"_3_1_De_confession_religieuse_"] if str(data.loc[i,"_3_1_De_confession_religieuse_"]) !="0" else data.loc[i,"_3_1_7_Autres_pr_ciser"]
    donnee_Socio["profession"]=data.loc[i,"_4_Quel_est_votre_statut_profe"] if str(data.loc[i,"_4_Quel_est_votre_statut_profe"]) != "0" else data.loc[i,"_4_10_Autre_pr_ciser"]
    donnee_Socio["secteur_activite"]=data.loc[i,"_5_Quel_est_votre_principal_se"] if str(data.loc[i,"_5_Quel_est_votre_principal_se"]) != "0" else data.loc[i,"_5_18_Autre_pr_ciser"]
    donnee_Socio["revenu_mensuel"]=data.loc[i,"_6_Dans_quelle_tranc_otre_revenu_mensuel_"]
    donnee_Socio["niveau_etude"]=data.loc[i,"_7_Quel_est_votre_pl_haut_niveau_d_tude_"]
    #donnee_socioDemographique.insert_one(donnee_Socio)
    
    # # chargement de la dimension donnee Geographique
    donnee_Geo=dict()
    donnee_Geo["_id"]=i
    donnee_Geo["sejour_benin_6_dernier_mois"]=data.loc[i,"_8_Avez_vous_effectu_des_voya"]
    zone=[]
    afrique=dict()
    afrique["quartier"]=""
    afrique["commune"]=""
    afrique["ville"]=""
    afrique["continent"]=data.loc[i,"_9_Si_oui_la_question_8_veu/1"]
    afrique["pays"]=data.loc[i,"_9_1_Pays_visit_s_en_Afrique"]
    asie=dict()
    asie["quartier"]=""
    asie["commune"]=""
    asie["ville"]=""
    asie["continent"]=data.loc[i,"_9_Si_oui_la_question_8_veu/2"]
    asie["pays"]=data.loc[i,"_9_2_Pays_visit_s_en_Asie"]
    europe=dict()
    europe["quartier"]=""
    europe["commune"]=""
    europe["ville"]=""
    europe["continent"]=data.loc[i,"_9_Si_oui_la_question_8_veu/3"]
    europe["pays"]=data.loc[i,"_9_3_Pays_visit_s_en_Europe"]
    amerique_nord=dict()
    amerique_nord["quartier"]=""
    amerique_nord["commune"]=""
    amerique_nord["ville"]=""
    amerique_nord["continent"]=data.loc[i,"_9_Si_oui_la_question_8_veu/4"]
    amerique_nord["pays"]=data.loc[i,"_9_4_Pays_visit_s_en_Am_rique_du_Nord"]
    amerique_sud=dict()
    amerique_sud["quartier"]=""
    amerique_sud["commune"]=""
    amerique_sud["ville"]=""
    amerique_sud["continent"]=data.loc[i,"_9_Si_oui_la_question_8_veu/5"]
    amerique_sud["pays"]=data.loc[i,"_9_5_Pays_visit_s_en_Am_rique_du_Sud"]
    if str(afrique["continent"])!="nan":
        zone.append(afrique)
    if str(asie["continent"])!="nan":
        zone.append(asie)
    if str(europe["continent"])!="nan":
        zone.append(europe)
    if str(amerique_nord["continent"])!="nan":
        zone.append(amerique_nord)
    if str(amerique_sud["continent"])!="nan":
        zone.append(amerique_sud)
    donnee_Geo["zone_externe_visite"]=zone
    
    
    communes_int=[data.loc[i,"_11_Au_cours_du_dern_s_avez_vous_visit_e_/1"],
                  data.loc[i,"_11_Au_cours_du_dern_s_avez_vous_visit_e_/2"],
                  data.loc[i,"_11_Au_cours_du_dern_s_avez_vous_visit_e_/3"],
                  data.loc[i,"_11_Au_cours_du_dern_s_avez_vous_visit_e_/4"]]
    zone_interne=[]
    for j in communes_int:
        if str(j)!="0":
            afrique_int=dict()
            afrique_int["quartier"]=""
            afrique_int["commune"]=j
            afrique_int["ville"]=""
            afrique_int["continent"]="Afrique"
            afrique_int["pays"]="Benin"
            zone_interne.append(afrique_int)
    donnee_Geo["zone_interne_visite"]=zone_interne
    
    afrique_res=dict()
    afrique_res["quartier"]=""
    afrique_res["commune"]=data.loc[i,"_10_Dans_quelle_commune_habite"] if str(data.loc[i,"_10_Dans_quelle_commune_habite"]) != "0" else data.loc[i,"_10_15_Autre_pr_ciser"]
    afrique_res["ville"]=""
    afrique_res["continent"]="Afrique"
    afrique_res["pays"]="Benin"
    donnee_Geo["zone_de_residence"]=afrique_res
    moyen_depla=[]
    if str(data.loc[i,"_12_Quel_s_sont_le_s_moyen_s/1"]) != "0":
        depl = dict()
        depl["moyen_depl"]= data.loc[i,"_12_Quel_s_sont_le_s_moyen_s/1"]
        depl["duree"]=int(data.loc[i,"_12_1_Dur_e_moyenne_transport_individuel"])
        moyen_depla.append(depl)
    if str(data.loc[i,"_12_Quel_s_sont_le_s_moyen_s/2"]) != "0":
        depl = dict()
        depl["moyen_depl"]= data.loc[i,"_12_Quel_s_sont_le_s_moyen_s/2"]
        depl["duree"]=""
        moyen_depla.append(depl)
    if str(data.loc[i,"_12_Quel_s_sont_le_s_moyen_s/3"]) != "0":
        depl = dict()
        depl["moyen_depl"]= data.loc[i,"_12_Quel_s_sont_le_s_moyen_s/3"]
        depl["duree"]=int(data.loc[i,"_12_2_Dur_e_moyenne_transport_en_commun"])
        moyen_depla.append(depl)
    donnee_Geo["moyen_deplacement"]=moyen_depla
    #donnee_Geographique.insert_one(donnee_Geo)
    
    # # chargement de la dimension condition de logement
    condition_Loge=dict()
    condition_Loge["_id"]=i
    condition_Loge["type_logement"]=data.loc[i,"_13_Dans_quel_type_d_habitatio"]
    condition_Loge["nombre_resident"]=int(data.loc[i,"_14_Combien_de_perso_pal_vous_y_compris_"])
    condition_Loge["nombre_personne_chambre"]=int(data.loc[i,"_15_Dans_votre_chambre_couch"])
    condition_Loge["frequence_aeration_logement"]=data.loc[i,"_16_Est_ce_que_vous_oment_de_la_journ_e_"]
    condition_Loge["frequence_brasseur"]=data.loc[i,"_17_Utilisez_vous_un_oment_de_la_journ_e_"]
    condition_Loge["frequence_climatiseur"]=data.loc[i,"_18_Utilisez_vous_un_oment_de_la_journ_e_"]
    condition_Loge["source_eau"]=data.loc[i,"_19_Quelle_source_d_approvisio"] if str(data.loc[i,"_19_Quelle_source_d_approvisio"])!="0" else data.loc[i,"_19_4_Autre_p_ciser"]
    condition_Loge["dispositif_lavage_mains"]=data.loc[i,"_20_Quel_dispositif_logement_principal_"]
    #condition_Logement.insert_one(condition_Loge)
    
    # # chargement de la dimension condition professionnelle
    condition_pro=dict()
    condition_pro["_id"]=i
    condition_pro["lieu_travail"]=data.loc[i,"_21_Dans_quelle_commune_se_tro"]
    condition_pro["type_batiment"]=data.loc[i,"_22_Dans_quel_type_de_b_timent"]
    condition_pro["nombre_par_piece"]=float(data.loc[i,"_23_Combien_tes_vous_dans_la_"])
    condition_pro["distance_approximative_min"]=data.loc[i,"_24_Quelle_distance_e_travail_principal_"]
    condition_pro["frequence_brasseur"]=data.loc[i,"_25_Utilisez_vous_un_oment_de_la_journ_e_"]
    condition_pro["frequence_climatiseur"]=data.loc[i,"_26_Utilisez_vous_un_ent_dans_la_journ_e_"]
    condition_pro["source_eau"]=data.loc[i,"_27_Quelle_source_d_approvisio"] if str(data.loc[i,"_27_Quelle_source_d_approvisio"])!="0" else data.loc[i,"_27_4_Autre_pr_ciser"]
    condition_pro["dispositif_lavage_mains"]=data.loc[i,"_28_Quel_dispositif_e_travail_principal_"]
    #condition_Professsionnelle.insert_one(condition_pro)
    
    # # chargement de la dimension pratique covid
    pratique_covid=dict()
    pratique_covid["_id"]=i
    pratique_covid["nbr_ceremonies_recente"]=int(data.loc[i,"_29_A_combien_de_c_r_monies_r"])

    lieu_celeb=[]
    if str(data.loc[i,"_30_O_avaient_elles_lieu_et_q/1"])!='nan':
        celebration=dict()
        celebration["lieu"]=data.loc[i,"_30_O_avaient_elles_lieu_et_q/1"]
        celebration["nombre_participants"]=float(data.loc[i,"_30_1_A_domicile_nombre_approximatif"])
        lieu_celeb.append(celebration)
        
    if str(data.loc[i,"_30_O_avaient_elles_lieu_et_q/2"])!='nan':
        celebration=dict()
        celebration["lieu"]=data.loc[i,"_30_O_avaient_elles_lieu_et_q/2"]
        celebration["nombre_participants"]=float(data.loc[i,"_30_2_Chez_un_proche_nombre_approximatif"])
        lieu_celeb.append(celebration)
        
    if str(data.loc[i,"_30_O_avaient_elles_lieu_et_q/3"])!='nan':
        celebration=dict()
        celebration["lieu"]=data.loc[i,"_30_O_avaient_elles_lieu_et_q/3"]
        celebration["nombre_participants"]=float(data.loc[i,"_30_3_Dans_un_lieu_p_nombre_approximatif"])
        lieu_celeb.append(celebration)
    pratique_covid["lieu_celebration"]=lieu_celeb
    pratique_covid["frequence_visite_hopitaux"]=float(data.loc[i,"_31_Combien_de_fois_0_si_pas_fr_quent"])
    pratique_covid["frequence_visite_lieu_rejouissance"]=float(data.loc[i,"_32_Combien_de_fois_0_si_pas_fr_quent"])
    pratique_covid["frequence_visite_lieu_religieux"]=float(data.loc[i,"_33_Combien_de_fois_0_si_pas_fr_quent"])
    pratique_covid["frequence_visite_manifestation_sportive"]=float(data.loc[i,"_34_Combien_de_fois_0_si_pas_fr_quent"])
    pratique_covid["frequence_visite_lieu_rejouissance"]=float(data.loc[i,"_32_Combien_de_fois_0_si_pas_fr_quent"])
    
    contact_physique=dict()
    contact_physique["frequence"]=float(data.loc[i,"_35_Combien_de_fois_avez_vous_"])
    contact_physique["nombre_participants"]=float(data.loc[i,"_36_Quel_tait_le_no_ticip_ces_matchs_"])
    
    sans_physique=dict()
    sans_physique["frequence"]=float(data.loc[i,"_37_Combien_de_fois_avez_vous_"])
    sans_physique["nombre_participants"]=float(data.loc[i,"_37_2_Quel_tait_le_ticip_ces_matchs_"])
    
    mes_physiques=dict()
    mes_physiques["avec_contact_physique"]=contact_physique
    mes_physiques["sans_contact_physique"]=sans_physique
    
    pratique_covid["frequence_participation_match"]=mes_physiques
    
    plage=dict()
    plage["frequence"]=float(data.loc[i,"_38_Combien_de_fois_avez_vous_"])
    plage["nombre_participants"]=float(data.loc[i,"_39_Quel_tait_le_no_ge_non_loin_de_vous_"])
    pratique_covid["frequence_visiste_plage"]=plage
    
    education=dict()
    education["frequence"]=float(data.loc[i,"_40_Combien_de_fois_avez_vous_"])
    education["nombre_participants"]=float(data.loc[i,"_41_Quel_tait_le_no_que_vous_aux_cours_"])
    education["mesure_barriere_respecte"]=data.loc[i,"_41_1_Aviez_vous_l_i_taient_respect_es_"]
    pratique_covid["frequence_visite_lieu_educatif"]=education
    
    banque=dict()
    banque["frequence"]=float(data.loc[i,"_42_Combien_de_fois_vous_tes_"])
    banque["nombre_participants"]=float(data.loc[i,"_43_Quel_tait_le_no_d_attente_que_vous_"])
    banque["mesure_barriere_respecte"]=data.loc[i,"_43_1_Aviez_vous_l_i_taient_respect_es_"]
    pratique_covid["frequence_visite_banque"]=banque
    
    boutique=dict()
    boutique["frequence"]=float(data.loc[i,"_44_Combien_de_fois_vous_tes_"])
    boutique["nombre_participants"]=float(data.loc[i,"_45_Quel_tait_le_no_e_boutique_que_vous_"])
    boutique["mesure_barriere_respecte"]=data.loc[i,"_45_1_Aviez_vous_l_i_taient_respect_es_"]
    pratique_covid["frequence_visite_boutique"]=boutique
    
    superette=dict()
    superette["frequence"]=float(data.loc[i,"_45_bis1_Combien_de_fois_vous_"])
    superette["nombre_participants"]=float(data.loc[i,"_45_bis1_1_Quel_tai_sup_rette_que_vous_"])
    superette["mesure_barriere_respecte"]=data.loc[i,"_45_bis_1_2_Aviez_vo_taient_respect_es_"]
    pratique_covid["frequence_visite_superette"]=superette
    
    hypermarche=dict()
    hypermarche["frequence"]=float(data.loc[i,"_45_bis2_Combien_de_fois_vous_"])
    hypermarche["nombre_participants"]=float(data.loc[i,"_45_bis2_1_Quel_tai_ypermarch_que_vous_"])
    hypermarche["mesure_barriere_respecte"]=data.loc[i,"_45_bis_2_2_Aviez_vo_taient_respect_es_"]
    pratique_covid["frequence_visite_hypermarche"]=hypermarche
    
    marche=dict()
    marche["frequence"]=float(data.loc[i,"_46_Combien_de_fois_vous_tes_"])
    marche["mesure_barriere_respecte"]=data.loc[i,"_46_1_Aviez_vous_l_i_taient_respect_es_"]
    pratique_covid["frequence_visite_marche"]=marche
    
    infos_masque=dict()
    infos_masque["type_de_masque"]=data.loc[i,"_47_Quel_type_de_masque_avez_v"]
    raisons=[]
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/0"])!="0":
        raisons.append(data.loc[i,"_47_1_Quelles_sont_les_raisons/0"])
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/1"])!="0":
        raisons.append(data.loc[i,"_47_1_Quelles_sont_les_raisons/1"])
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/2"])!="0":
        raisons.append(data.loc[i,"_47_1_Quelles_sont_les_raisons/2"])
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/3"])!="0":
        raisons.append(data.loc[i,"_47_1_Quelles_sont_les_raisons/3"])
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/4"])!="0":
        raisons.append(data.loc[i,"_47_1_Quelles_sont_les_raisons/4"])
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/5"])!="0":
        raisons.append(data.loc[i,"_47_1_Quelles_sont_les_raisons/5"])
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/6"])!="0":
        raisons.append(data.loc[i,"_47_1_Quelles_sont_les_raisons/6"])
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/7"])!="0":
        raisons.append(data.loc[i,"_47_1_7_Autre_pr_ciser"])
    infos_masque["raison_choix_type"]=raisons
    
    contre_raisons=[]
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/0"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/0"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/1"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/2"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/2"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/2"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/3"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/3"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/4"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/4"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/5"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/5"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/6"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/6"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/7"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/7"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/8"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_7_Autre_pr_ciser"])
    infos_masque["contre_raison"]=contre_raisons
    frequence_port=dict()
    frequence_port["dans_logement_principal"]=data.loc[i,"_48_1_A_quelle_fr_qu_d_autres_personnes_"]
    frequence_port["dans_rue"]=data.loc[i,"_48_2_A_quelle_fr_qu_masque_dans_la_rue_"]
    frequence_port["dans_transport_individuel"]=data.loc[i,"_48_3_A_quelle_fr_qu_uel_voiture_moto_"]
    frequence_port["dans_transport_commun"]=data.loc[i,"_48_4_A_quelle_fr_qu_voiture_moto_bus_"]
    frequence_port["dans_lieu_travail"]=data.loc[i,"_48_5_A_quelle_fr_qu_d_autres_personnes_"]
    frequence_port["dans_lieu_public"]=data.loc[i,"_48_6_A_quelle_fr_qu_ch_lieu_religieux_"]
    infos_masque["frequence_port_masque"]=frequence_port
    infos_masque["freqence_entretient_masque"]=data.loc[i,"_49_A_quelle_fr_quen_viez_vous_ce_masque_"]
    infos_masque["abaisser_masque"]=data.loc[i,"_49_1_Vous_arrivait_du_menton_ou_du_cou_"]
    pratique_covid["information_masque"]=infos_masque
    
    entretien_main=dict()
    entretien_main["entrer_lieu_public"]=data.loc[i,"_50_A_quelle_fr_quen_ch_lieu_religieux_"]
    entretien_main["sorti_lieu_public"]=data.loc[i,"_51_A_quelle_fr_quen_ch_lieu_religieux_"]
    entretien_main["sorti_logement"]=data.loc[i,"_52_A_quelle_fr_quen_r_de_votre_logement_"]
    entretien_main["retour_logement"]=data.loc[i,"_53_A_quelle_fr_quen_ur_votre_logement_"]
    entretien_main["touche_instrument_non_desinfecter"]=data.loc[i,"_54_A_quelle_fr_quen_n_e_de_porte_cl_s_"]
    pratique_covid["frequence_entretient_main"]=entretien_main
    
    pratique_covid["frequence_utilisation_gel"]=data.loc[i,"_55_A_quelle_fr_quen_de_mani_re_g_n_rale_"]
    pratique_covid["frequence_desinfection_objet_logement"]=data.loc[i,"_56_A_quelle_fr_quen_urs_du_dernier_mois_"]
    pratique_covid["frequence_desinfection_objet_travail"]=data.loc[i,"_57_A_quelle_fr_quen_urs_du_dernier_mois_"]
    pratique_covid["frequence_touche_visage"]=data.loc[i,"_58_A_quelle_fr_quen_urs_du_dernier_mois_"]
    pratique_covid["frequence_serrer_mains"]=data.loc[i,"_59_A_quelle_fr_quen_urs_du_dernier_mois_"]
    pratique_covid["frequence_rapport_sexuel"]=data.loc[i,"_60_A_quelle_fr_quen_urs_du_dernier_mois_"]
    pratique_covid["malade_dernier_mois"]=data.loc[i,"_61_Avez_vous_t_malade_au_co"]
    lieu_guerision=list()
    
    if str(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/1"])!="0":
        lieu_guerision.append(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/1"])
    if str(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/2"])!="0":
        lieu_guerision.append(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/2"])
    if str(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/3"])!="0":
        lieu_guerision.append(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/3"])
    if str(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/4"])!="0":
        lieu_guerision.append(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/4"])
    if str(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/5"])!="0":
        lieu_guerision.append(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/5"])
    if str(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/6"])!="0":
        lieu_guerision.append(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/6"])
    if str(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/7"])!="0":
        lieu_guerision.append(data.loc[i,"_62_7_Autres_pr_ciser"])
    pratique_covid["lieu_preferentiel_guerison"]=lieu_guerision

    #pratique_covid19.insert_one(pratique_covid)
    
     # # chargement de la dimension perception covid19
    perception_cov=dict()
    perception_cov["_id"]=i
    def_cov=list()
    if str(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/1"])!="0":
        def_cov.append(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/1"])
    if str(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/2"])!="0":
        def_cov.append(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/2"])
    if str(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/3"])!="0":
        def_cov.append(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/3"])
    if str(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/4"])!="0":
        def_cov.append(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/4"])
    if str(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/5"])!="0":
        def_cov.append(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/5"])
    if str(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/6"])!="0":
        def_cov.append(data.loc[i,"_63_6_Autre_pr_ciser"])
    #if str(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/999"])!="0":
        #def_cov.append(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/999"])
    perception_cov["definition_covid19"]=def_cov
    peur_cov=dict()
    peur_cov["reponse"]=data.loc[i,"_64_Avez_vous_peur_de_la_COVID_19_"]
    raison_peur=list()
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/0"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/0"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/1"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/1"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/2"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/2"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/3"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/3"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/4"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/4"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/5"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/5"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/6"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/6"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/7"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/7"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/8"])!="0":
        raison_peur.append(data.loc[i,"_65_9_Autre_pr_ciser"])
    peur_cov["raisons"]=raison_peur
    perception_cov["peur_covid19"]=peur_cov
    consequence_covid=dict()
    plan_social=list()
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/0"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/0"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/1"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/1"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/2"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/2"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/3"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/3"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/4"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/4"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/5"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/5"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/6"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/6"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/7"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/7"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/8"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/8"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/9"])!="0":
        plan_social.append(data.loc[i,"_66_9_Autre_pr_ciser"])
    consequence_covid["plan_social"]=plan_social
    sur_comportement=list()
    if str(data.loc[i,"_67_Quelles_ont_t_les_cons_q/0"])!="0":
        sur_comportement.append(data.loc[i,"_67_Quelles_ont_t_les_cons_q/0"])
    if str(data.loc[i,"_67_Quelles_ont_t_les_cons_q/1"])!="0":
        sur_comportement.append(data.loc[i,"_67_Quelles_ont_t_les_cons_q/1"])
    if str(data.loc[i,"_67_Quelles_ont_t_les_cons_q/2"])!="0":
        sur_comportement.append(data.loc[i,"_67_Quelles_ont_t_les_cons_q/2"])
    if str(data.loc[i,"_67_Quelles_ont_t_les_cons_q/3"])!="0":
        sur_comportement.append(data.loc[i,"_67_Quelles_ont_t_les_cons_q/3"])
    if str(data.loc[i,"_67_Quelles_ont_t_les_cons_q/4"])!="0":
        sur_comportement.append(data.loc[i,"_67_Quelles_ont_t_les_cons_q/4"])
    if str(data.loc[i,"_67_Quelles_ont_t_les_cons_q/5"])!="0":
        sur_comportement.append(data.loc[i,"_67_Quelles_ont_t_les_cons_q/5"])
    if str(data.loc[i,"_67_Quelles_ont_t_les_cons_q/9"])!="0":
        sur_comportement.append(data.loc[i,"_67_9_Autre_pr_ciser"])
    consequence_covid["sur_comportement"]=sur_comportement
    plan_economique=list()
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/0"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/0"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/1"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/1"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/2"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/2"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/3"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/3"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/4"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/4"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/5"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/5"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/6"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/6"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/7"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/7"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/8"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/8"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/9"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/9"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/10"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/10"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/11"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/11"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/12"])!="0":
        plan_economique.append(data.loc[i,"_68_12_Autre_pr_ciser"])
    consequence_covid["plan_economique"]=plan_economique
    plan_sanitaire=list()
    if str(data.loc[i,"_69_Quelles_ont_t_les_cons_q/0"])!="0":
        plan_sanitaire.append(data.loc[i,"_69_Quelles_ont_t_les_cons_q/0"])
    if str(data.loc[i,"_69_Quelles_ont_t_les_cons_q/1"])!="0":
        plan_sanitaire.append(data.loc[i,"_69_Quelles_ont_t_les_cons_q/1"])
    if str(data.loc[i,"_69_Quelles_ont_t_les_cons_q/2"])!="0":
        plan_sanitaire.append(data.loc[i,"_69_Quelles_ont_t_les_cons_q/2"])
    if str(data.loc[i,"_69_Quelles_ont_t_les_cons_q/3"])!="0":
        plan_sanitaire.append(data.loc[i,"_69_Quelles_ont_t_les_cons_q/3"])
    if str(data.loc[i,"_69_Quelles_ont_t_les_cons_q/4"])!="0":
        plan_sanitaire.append(data.loc[i,"_69_Quelles_ont_t_les_cons_q/4"])
    if str(data.loc[i,"_69_Quelles_ont_t_les_cons_q/5"])!="0":
        plan_sanitaire.append(data.loc[i,"_69_5_Autre_pr_ciser"])
    consequence_covid["plan_sanitaire"]=plan_sanitaire
    perception_cov["consequence_covid19"]=consequence_covid
    
    cause_covid=list()
    if str(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/0"])!="0":
        cause_covid.append(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/0"])
    if str(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/1"])!="0":
        cause_covid.append(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/1"])
    if str(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/2"])!="0":
        cause_covid.append(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/2"])
    if str(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/3"])!="0":
        cause_covid.append(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/3"])
    if str(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/4"])!="0":
        cause_covid.append(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/4"])
    if str(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/5"])!="0":
        cause_covid.append(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/5"])
    if str(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/6"])!="0":
        cause_covid.append(data.loc[i,"_70_6_Autre_pr_ciser"])
    perception_cov["cause_covid19"]=cause_covid
    provoque_cov=dict()
    provoque_cov["reponse"]=data.loc[i,"_71_Est_ce_que_la_Covid_19_peu"]
    provoque_cov["justification"]=data.loc[i,"_72_Si_oui_comment_"] if str(data.loc[i,"_71_Est_ce_que_la_Covid_19_peu"])=="Oui" else  data.loc[i,"_73_Si_non_pourquoi_"]
    perception_cov["provoquer_covid19"]=provoque_cov
    #perception_covid19.insert_one(perception_cov)
    
    ## Chargement dans la dimension connaissance covid
    connaissance_covid=dict()
    connaissance_covid["_id"]=i
    transmission_cov=list()
    if str(data.loc[i,"_74_Comment_la_COVID_19_se_tra/0"])!="0":
        transmission_cov.append(data.loc[i,"_74_Comment_la_COVID_19_se_tra/0"])
    if str(data.loc[i,"_74_Comment_la_COVID_19_se_tra/1"])!="0":
        transmission_cov.append(data.loc[i,"_74_Comment_la_COVID_19_se_tra/1"])
    if str(data.loc[i,"_74_Comment_la_COVID_19_se_tra/2"])!="0":
        transmission_cov.append(data.loc[i,"_74_Comment_la_COVID_19_se_tra/2"])
    if str(data.loc[i,"_74_Comment_la_COVID_19_se_tra/3"])!="0":
        transmission_cov.append(data.loc[i,"_74_Comment_la_COVID_19_se_tra/3"])
    if str(data.loc[i,"_74_Comment_la_COVID_19_se_tra/4"])!="0":
        transmission_cov.append(data.loc[i,"_74_Comment_la_COVID_19_se_tra/4"])
    if str(data.loc[i,"_74_Comment_la_COVID_19_se_tra/5"])!="0":
        transmission_cov.append(data.loc[i,"_74_Comment_la_COVID_19_se_tra/5"])
    if str(data.loc[i,"_74_Comment_la_COVID_19_se_tra/6"])!="0":
        transmission_cov.append(data.loc[i,"_74_6_Autres"])
    connaissance_covid["tranmission_covid"]=transmission_cov
    symptome_cov=list()
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/0"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/0"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/1"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/1"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/2"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/2"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/3"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/3"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/4"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/4"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/5"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/5"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/6"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/6"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/7"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/7"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/8"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/8"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/9"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/9"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/10"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/10"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/11"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/11"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/12"])!="0":
        symptome_cov.append(data.loc[i,"_75_12_Autres"])
    connaissance_covid["symptome_covid19"]=symptome_cov
    personne_a_risque=list()
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/0"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/0"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/1"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/1"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/2"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/2"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/3"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/3"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/4"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/4"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/5"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/5"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/6"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/6"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/7"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/7"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/8"])!="0":
        personne_a_risque.append(data.loc[i,"_76_8_Autres"])
    connaissance_covid["personne_a_risque"]=personne_a_risque
    connaissance_covid["periode_incubation"]=str(data.loc[i,"_77_Quelle_est_la_p_tion_du_coronavirus_"])
    info_infection=dict()
    info_infection["infecter_sans_presenter_symptome"]=data.loc[i,"_78_Des_personnes_pe_senter_de_sympt_mes_"]
    info_infection["pas_symptome_mais_transmet_covid"]=data.loc[i,"_79_Une_personne_qui_une_autre_personne_"]
    connaissance_covid["information_infection"]=info_infection
    mesure_prev=list()
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/0"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/0"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/1"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/1"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/2"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/2"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/3"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/3"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/4"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/4"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/5"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/5"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/6"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/6"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/7"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/7"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/8"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/8"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/9"])!="0":
        mesure_prev.append(data.loc[i,"_80_9_Autres"])
    connaissance_covid["mesure_protection_covid"]=mesure_prev
    source_info_covid=dict()
    source=list()
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/1"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/1"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/2"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/2"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/3"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/3"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/4"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/4"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/5"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/5"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/6"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/6"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/7"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/7"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/8"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/8"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/9"])!="0":
        source.append(data.loc[i,"_81_9_Autres"])
    source_info_covid["source"]=source
    source_info_covid["source_confiance"]=data.loc[i,"_82_En_laquelle_de_ces_sources"] if str(data.loc[i,"_82_En_laquelle_de_ces_sources"])!="0" else data.loc[i,"_82_8_Autres"]
    source_info_covid["suffisament_informe"]=data.loc[i,"_83_Pensez_vous_que_les_inform"]
    degre_confiance=dict()
    degre_confiance["information_gouvernementales"]=data.loc[i,"_84_Quel_est_votre_d_es_gouvernementales_"]
    degre_confiance["information_internationales"]=data.loc[i,"_84_1_Quel_est_votre_ons_internationales_"]
    degre_confiance["information_medias"]=data.loc[i,"_84_2_Quel_est_votre_es_dans_les_m_dias_"]
    source_info_covid["degre_confiance_info_officielles"]=degre_confiance
    connaissance_covid["source_info_covid"]=source_info_covid
    #connaissance_covid19.insert_one(connaissance_covid)
    # chargement dans la dimension historique sante
    historique=dict()
    historique["_id"]=i
    historique["statut_covid"]=data.loc[i,"_85_Quel_est_votre_s_terme_du_d_pistage_"]
    raison_dep=list()
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/0"])!="0":
        raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/0"])
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/1"])!="0":
        raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/1"])
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/2"])!="0":
        raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/2"])
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/3"])!="0":
        raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/3"])
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/4"])!="0":
        raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/4"])
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/5"])!="0":
        raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/5"])
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/6"])!="0":
        raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/6"])
    #if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/999"])!="0":
        #raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/999"])
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/7"])!="0":
        raison_dep.append(data.loc[i,"_86_7_Autres_pr_ciser"])
    historique["raison_depistage"]= raison_dep
    nombre_cas=dict()
    nombre_cas["dans_la_famille"]=float(data.loc[i,"_87_Combien_de_cas_o_le_0_si_aucun_cas"])
    nombre_cas["parmi_proche"]=str(data.loc[i,"_88_Combien_de_cas_o_es_0_si_aucun_cas"])
    historique["nombre_cas_confirme"]=nombre_cas
    historique["nutriment_preventifs_consommer"]=data.loc[i,"_89_Quels_sont_les_m_contre_la_COVID_19_"]
    antecedent_med=list()
    if str(data.loc[i,"_90_Souffrez_vous_de_/0"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/0"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/1"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/1"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/2"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/2"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/3"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/3"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/4"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/4"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/5"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/5"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/6"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/6"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/7"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/7"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/8"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/8"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/9"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/9"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/10"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/10"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/11"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/11"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/12"])!="0":
        antecedent_med.append(data.loc[i,"_90_12_Autres_pr_ciser"])
    
    historique["antecedent_medical"]= antecedent_med
    historique["enceinte_accoucher_recemment"]=data.loc[i,"_91_tes_vous_encein_accouch_r_cemment_"]
    consomme_stupefiant=dict()
    consomme_stupefiant["tabac"]=data.loc[i,"_92_Fumez_vous_du_tabac_"]
    consomme_stupefiant["chicha"]=data.loc[i,"_93_tes_vous_un_consommateur_"]
    consomme_stupefiant["consomme_en_groupe"]=data.loc[i,"_94_Si_oui_en_avez_v_consommer_en_groupe_"]
    historique["consomme_stupefiant"]=consomme_stupefiant
    #historique_sante.insert_one(historique)
    # chargement dans la dimension nutrition
    nutrit=dict()
    nutrit["_id"]=i
    consommation=dict()
    cereale=dict()
    cereale["poids"]=1
    cereale["score"]=int(data.loc[i,"Ma_s_riz_sorgho_mil_pain_e"])
    consommation["cereale"]=cereale
    tubercule=dict()
    tubercule["poids"]=1
    tubercule["score"]=int(data.loc[i,"Manioc_pommes_de_terre_et_pat"])
    consommation["tubercule"]=tubercule
    legumineuse_noix=dict()
    legumineuse_noix["poids"]=3
    legumineuse_noix["score"]=int(data.loc[i,"Haricots_pois_arachides_en_c"])
    consommation["legumineuse_noix"]=legumineuse_noix
    legume=dict()
    legume["poids"]=1
    legume["score"]=int(data.loc[i,"L_gumes_condiments_et_l_gumes"])
    consommation["legume"]=legume
    fruit=dict()
    fruit["poids"]=1
    fruit["score"]=int(data.loc[i,"Fruits_mangue_orange_ananas"])
    consommation["fruit"]=fruit
    viande_poisson=dict()
    viande_poisson["poids"]=4
    viande_poisson["score"]=int(data.loc[i,"B_uf_ch_vre_volailles_porc_"])
    consommation["viande_poisson"]=viande_poisson
    lait=dict()
    lait["poids"]=4
    lait["score"]=int(data.loc[i,"Lait_yaourt_et_autres_produit"])
    consommation["lait"]=lait
    sucre=dict()
    sucre["poids"]=0.5
    sucre["score"]=int(data.loc[i,"Sucre_et_produits_sucr_s"])
    consommation["sucre"]=sucre
    huile=dict()
    huile["poids"]=0.5
    huile["score"]=int(data.loc[i,"Huiles_mati_res_grasses_frit"])
    consommation["huile"]=huile
    nutrit["consommation_alimentaire"]=consommation
    consommation_alcool=dict()
    consommation_alcool["frequence"]=data.loc[i,"_96_quelle_fr_quence_vous_ar"]
    consommation_alcool["nbr_verres_jours_ordinaire"]=data.loc[i,"_97_Combien_de_verre_talopk_mi_de_sodabi"]
    consommation_alcool["frequence_6_verres_aumoins"]=data.loc[i,"_98_Au_cours_d_une_m_s_standards_ou_plus_"]
    nutrit["consommation_alcool"]=consommation_alcool
    nutrit["perte_poids"]=data.loc[i,"_99_Avez_vous_consta_derni_res_semaines_"]
    nutrit["prise_poids"]=data.loc[i,"_100_Avez_vous_const_derni_res_semaines_"]
    #nutrition.insert_one(nutrit)


# In[30]:


# Chargement dans la table de fait (objet)
db=myclient.testrestoredb
donnee_Identification=db["donnee_Identification"]
donnee_Geographique=db["donnee_Geographique"]
donnee_consultation=db["donnee_consultation"]
condition_Logement=db["condition_Logement"]
condition_Professsionnelle=db["condition_Professionnelle"]
connaissance_covid19=db["connaissance_covid19"]
donnee_socioDemographique=db["donnee_socioDemographique"]
pratique_covid19=db["pratique_covid19"]
perception_covid19=db["perception_covid19"]
historique_sante=db["historique_sante"]
nutrition=db["nutrition"]
dim_objet=db["objet"]
for  i in range(0,len(data.iloc[0:,0])):
    objects=dict()
    objects["_id"]=i
    objects["type"]="personnes"
    
    #recuperation de ID itineraire_therapeutique
    objects["ID_itineraire_therapeutique"]=""
    
    #recuperation de ID logement
    
    type_logement=data.loc[i,"_13_Dans_quel_type_d_habitatio"]
    nombre_resident=int(data.loc[i,"_14_Combien_de_perso_pal_vous_y_compris_"])
    nombre_personne_chambre=int(data.loc[i,"_15_Dans_votre_chambre_couch"])
    frequence_aeration_logement=data.loc[i,"_16_Est_ce_que_vous_oment_de_la_journ_e_"]
    frequence_brasseur=data.loc[i,"_17_Utilisez_vous_un_oment_de_la_journ_e_"]
    frequence_climatiseur=data.loc[i,"_18_Utilisez_vous_un_oment_de_la_journ_e_"]
    source_eau=data.loc[i,"_19_Quelle_source_d_approvisio"] if str(data.loc[i,"_19_Quelle_source_d_approvisio"])!="0" else data.loc[i,"_19_4_Autre_p_ciser"]
    dispositif_lavage_mains=data.loc[i,"_20_Quel_dispositif_logement_principal_"]

    cursorL = condition_Logement.find_one({"type_logement" :type_logement,"nombre_resident" :nombre_resident,"nombre_personne_chambre" :nombre_personne_chambre,"frequence_aeration_logement" :frequence_aeration_logement,"frequence_brasseur" :frequence_brasseur,"frequence_climatiseur" :frequence_climatiseur,"source_eau" :source_eau,"dispositif_lavage_mains" :dispositif_lavage_mains},{'_id': 1}) 
    objects["ID_condition_logement"]=cursorL['_id']
    
    #recuperation de ID condition professionnelle
    
    lieu_travail=data.loc[i,"_21_Dans_quelle_commune_se_tro"]
    type_batiment=data.loc[i,"_22_Dans_quel_type_de_b_timent"]
    nombre_par_piece=float(data.loc[i,"_23_Combien_tes_vous_dans_la_"])
    distance_approximative_min=data.loc[i,"_24_Quelle_distance_e_travail_principal_"]
    frequence_brasseur=data.loc[i,"_25_Utilisez_vous_un_oment_de_la_journ_e_"]
    frequence_climatiseur=data.loc[i,"_26_Utilisez_vous_un_ent_dans_la_journ_e_"]
    source_eau=data.loc[i,"_27_Quelle_source_d_approvisio"] if str(data.loc[i,"_27_Quelle_source_d_approvisio"])!="0" else data.loc[i,"_27_4_Autre_pr_ciser"]
    dispositif_lavage_mains=data.loc[i,"_28_Quel_dispositif_e_travail_principal_"]
   
    cursorP = condition_Professsionnelle.find_one({"lieu_travail" :lieu_travail,"type_batiment" :type_batiment,"nombre_par_piece" :nombre_par_piece,"distance_approximative_min" :distance_approximative_min,"frequence_brasseur" :frequence_brasseur,"frequence_climatiseur" :frequence_climatiseur,"source_eau" :source_eau,"dispositif_lavage_mains" :dispositif_lavage_mains},{'_id': 1})
    objects["ID_professionnelle"]=cursorP['_id']
    
    #recuperation de ID connaissance covid
    
    connaissance_covid=dict()
    connaissance_covid["_id"]=i
    transmission_cov=list()
    if str(data.loc[i,"_74_Comment_la_COVID_19_se_tra/0"])!="0":
        transmission_cov.append(data.loc[i,"_74_Comment_la_COVID_19_se_tra/0"])
    if str(data.loc[i,"_74_Comment_la_COVID_19_se_tra/1"])!="0":
        transmission_cov.append(data.loc[i,"_74_Comment_la_COVID_19_se_tra/1"])
    if str(data.loc[i,"_74_Comment_la_COVID_19_se_tra/2"])!="0":
        transmission_cov.append(data.loc[i,"_74_Comment_la_COVID_19_se_tra/2"])
    if str(data.loc[i,"_74_Comment_la_COVID_19_se_tra/3"])!="0":
        transmission_cov.append(data.loc[i,"_74_Comment_la_COVID_19_se_tra/3"])
    if str(data.loc[i,"_74_Comment_la_COVID_19_se_tra/4"])!="0":
        transmission_cov.append(data.loc[i,"_74_Comment_la_COVID_19_se_tra/4"])
    if str(data.loc[i,"_74_Comment_la_COVID_19_se_tra/5"])!="0":
        transmission_cov.append(data.loc[i,"_74_Comment_la_COVID_19_se_tra/5"])
    if str(data.loc[i,"_74_Comment_la_COVID_19_se_tra/6"])!="0":
        transmission_cov.append(data.loc[i,"_74_6_Autres"])
    connaissance_covid["tranmission_covid"]=transmission_cov
    symptome_cov=list()
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/0"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/0"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/1"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/1"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/2"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/2"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/3"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/3"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/4"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/4"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/5"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/5"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/6"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/6"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/7"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/7"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/8"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/8"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/9"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/9"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/10"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/10"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/11"])!="0":
        symptome_cov.append(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/11"])
    if str(data.loc[i,"_75_Quels_sont_les_sympt_mes_d/12"])!="0":
        symptome_cov.append(data.loc[i,"_75_12_Autres"])
    connaissance_covid["symptome_covid19"]=symptome_cov
    personne_a_risque=list()
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/0"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/0"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/1"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/1"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/2"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/2"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/3"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/3"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/4"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/4"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/5"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/5"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/6"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/6"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/7"])!="0":
        personne_a_risque.append(data.loc[i,"_76_Quelles_sont_les_personnes/7"])
    if str(data.loc[i,"_76_Quelles_sont_les_personnes/8"])!="0":
        personne_a_risque.append(data.loc[i,"_76_8_Autres"])
    connaissance_covid["personne_a_risque"]=personne_a_risque
    connaissance_covid["periode_incubation"]=str(data.loc[i,"_77_Quelle_est_la_p_tion_du_coronavirus_"])
    info_infection=dict()
    info_infection["infecter_sans_presenter_symptome"]=data.loc[i,"_78_Des_personnes_pe_senter_de_sympt_mes_"]
    info_infection["pas_symptome_mais_transmet_covid"]=data.loc[i,"_79_Une_personne_qui_une_autre_personne_"]
    connaissance_covid["information_infection"]=info_infection
    mesure_prev=list()
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/0"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/0"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/1"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/1"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/2"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/2"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/3"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/3"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/4"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/4"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/5"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/5"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/6"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/6"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/7"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/7"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/8"])!="0":
        mesure_prev.append(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/8"])
    if str(data.loc[i,"_80_Pour_se_prot_ger_de_la_COV/9"])!="0":
        mesure_prev.append(data.loc[i,"_80_9_Autres"])
    connaissance_covid["mesure_protection_covid"]=mesure_prev
    source_info_covid=dict()
    source=list()
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/1"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/1"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/2"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/2"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/3"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/3"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/4"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/4"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/5"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/5"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/6"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/6"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/7"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/7"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/8"])!="0":
        source.append(data.loc[i,"_81_Quelles_sont_vos_sources_d/8"])
    if str(data.loc[i,"_81_Quelles_sont_vos_sources_d/9"])!="0":
        source.append(data.loc[i,"_81_9_Autres"])
    source_info_covid["source"]=source
    source_info_covid["source_confiance"]=data.loc[i,"_82_En_laquelle_de_ces_sources"] if str(data.loc[i,"_82_En_laquelle_de_ces_sources"])!="0" else data.loc[i,"_82_8_Autres"]
    source_info_covid["suffisament_informe"]=data.loc[i,"_83_Pensez_vous_que_les_inform"]
    degre_confiance=dict()
    degre_confiance["information_gouvernementales"]=data.loc[i,"_84_Quel_est_votre_d_es_gouvernementales_"]
    degre_confiance["information_internationales"]=data.loc[i,"_84_1_Quel_est_votre_ons_internationales_"]
    degre_confiance["information_medias"]=data.loc[i,"_84_2_Quel_est_votre_es_dans_les_m_dias_"]
    source_info_covid["degre_confiance_info_officielles"]=degre_confiance
    connaissance_covid["source_info_covid"]=source_info_covid
    
    
    cursorCon = connaissance_covid19.find_one({"tranmission_covid" :connaissance_covid["tranmission_covid"],"symptome_covid19" : connaissance_covid["symptome_covid19"],"personne_a_risque" :connaissance_covid["personne_a_risque"],"periode_incubation" :connaissance_covid["periode_incubation"],"information_infection" :connaissance_covid["information_infection"],"mesure_protection_covid" :connaissance_covid["mesure_protection_covid"],"source_info_covid" :connaissance_covid["source_info_covid"]},{'_id': 1})
    
    
    objects["ID_connaissance_covid"]=cursorCon['_id']
    
    #recuperation de ID donnee identification
    
    axe_recherche="WP2"
    site=data.loc[i,"Site_"] 
    initiales=data.loc[i,"Initiales_"]
    code_enquete=int(data.loc[i,"Code_enqu_t_"])
    code_interview=data.loc[i,"Code_interviewer_"]
    heure_de_debut=data.loc[i,"Heure_de_d_but_interview"]
    heure_de_fin=data.loc[i,"Heure_de_fin_interview"]
   
    cursorIdent = donnee_Identification.find_one({"axe_recherche" :axe_recherche,"site" :site,"initiales" :initiales,"code_enquete" :code_enquete,"code_interview" :code_interview,"heure_de_debut" :heure_de_debut,"heure_de_fin" :heure_de_fin},{'_id': 1})
    
    
    objects["ID_identification"]=cursorIdent['_id']
    
    #recuperation de ID donnee consultation
    objects["ID_consultation"]=""
    
    #recuperation de ID donnee socio demographique
    
    sexe=data.loc[i,"_1_Sexe_"]
    statut_matrimonial=data.loc[i,"_1_1_Quel_est_votre_statut_mat"]
    age=int(data.loc[i,"_2_Quel_est_votre_ge_r_volu_"])
    groupe_socio_culturel=data.loc[i,"_3_De_quel_groupe_socio_cultur"] if str(data.loc[i,"_3_De_quel_groupe_socio_cultur"]) != "0" else data.loc[i,"_3_11_Autre_pr_ciser"] 
    religion=data.loc[i,"_3_1_De_confession_religieuse_"] if str(data.loc[i,"_3_1_De_confession_religieuse_"]) !="0" else data.loc[i,"_3_1_7_Autres_pr_ciser"]
    profession=data.loc[i,"_4_Quel_est_votre_statut_profe"] if str(data.loc[i,"_4_Quel_est_votre_statut_profe"]) != "0" else data.loc[i,"_4_10_Autre_pr_ciser"]
    secteur_activite=data.loc[i,"_5_Quel_est_votre_principal_se"] if str(data.loc[i,"_5_Quel_est_votre_principal_se"]) != "0" else data.loc[i,"_5_18_Autre_pr_ciser"]
    revenu_mensuel=data.loc[i,"_6_Dans_quelle_tranc_otre_revenu_mensuel_"]
    niveau_etude=data.loc[i,"_7_Quel_est_votre_pl_haut_niveau_d_tude_"]
    cursorSocio = donnee_socioDemographique.find_one({"sexe" :sexe,"statut_matrimonial" :statut_matrimonial,"age" :age,"groupe_socio_culturel" :groupe_socio_culturel,"religion" :religion,"profession" :profession,"secteur_activite" :secteur_activite,"revenu_mensuel" :revenu_mensuel,"niveau_etude" :niveau_etude},{'_id': 1})
    
    
    objects["ID_socioDEmographique"]=cursorSocio['_id']
    
    #recuperation de ID donnee geographique
    
    donnee_Geo=dict()
    donnee_Geo["sejour_benin_6_dernier_mois"]=data.loc[i,"_8_Avez_vous_effectu_des_voya"]
    zone=[]
    afrique=dict()
    afrique["quartier"]=""
    afrique["commune"]=""
    afrique["ville"]=""
    afrique["continent"]=data.loc[i,"_9_Si_oui_la_question_8_veu/1"]
    afrique["pays"]=data.loc[i,"_9_1_Pays_visit_s_en_Afrique"]
    asie=dict()
    asie["quartier"]=""
    asie["commune"]=""
    asie["ville"]=""
    asie["continent"]=data.loc[i,"_9_Si_oui_la_question_8_veu/2"]
    asie["pays"]=data.loc[i,"_9_2_Pays_visit_s_en_Asie"]
    europe=dict()
    europe["quartier"]=""
    europe["commune"]=""
    europe["ville"]=""
    europe["continent"]=data.loc[i,"_9_Si_oui_la_question_8_veu/3"]
    europe["pays"]=data.loc[i,"_9_3_Pays_visit_s_en_Europe"]
    amerique_nord=dict()
    amerique_nord["quartier"]=""
    amerique_nord["commune"]=""
    amerique_nord["ville"]=""
    amerique_nord["continent"]=data.loc[i,"_9_Si_oui_la_question_8_veu/4"]
    amerique_nord["pays"]=data.loc[i,"_9_4_Pays_visit_s_en_Am_rique_du_Nord"]
    amerique_sud=dict()
    amerique_sud["quartier"]=""
    amerique_sud["commune"]=""
    amerique_sud["ville"]=""
    amerique_sud["continent"]=data.loc[i,"_9_Si_oui_la_question_8_veu/5"]
    amerique_sud["pays"]=data.loc[i,"_9_5_Pays_visit_s_en_Am_rique_du_Sud"]
    if str(afrique["continent"])!="nan":
        zone.append(afrique)
    if str(asie["continent"])!="nan":
        zone.append(asie)
    if str(europe["continent"])!="nan":
        zone.append(europe)
    if str(amerique_nord["continent"])!="nan":
        zone.append(amerique_nord)
    if str(amerique_sud["continent"])!="nan":
        zone.append(amerique_sud)
    donnee_Geo["zone_externe_visite"]=zone
    
    
    communes_int=[data.loc[i,"_11_Au_cours_du_dern_s_avez_vous_visit_e_/1"],
                  data.loc[i,"_11_Au_cours_du_dern_s_avez_vous_visit_e_/2"],
                  data.loc[i,"_11_Au_cours_du_dern_s_avez_vous_visit_e_/3"],
                  data.loc[i,"_11_Au_cours_du_dern_s_avez_vous_visit_e_/4"]]
    zone_interne=[]
    for j in communes_int:
        if str(j)!="0":
            afrique_int=dict()
            afrique_int["quartier"]=""
            afrique_int["commune"]=j
            afrique_int["ville"]=""
            afrique_int["continent"]="Afrique"
            afrique_int["pays"]="Benin"
            zone_interne.append(afrique_int)
    donnee_Geo["zone_interne_visite"]=zone_interne
    
    afrique_res=dict()
    afrique_res["quartier"]=""
    afrique_res["commune"]=data.loc[i,"_10_Dans_quelle_commune_habite"] if str(data.loc[i,"_10_Dans_quelle_commune_habite"]) != "0" else data.loc[i,"_10_15_Autre_pr_ciser"]
    afrique_res["ville"]=""
    afrique_res["continent"]="Afrique"
    afrique_res["pays"]="Benin"
    donnee_Geo["zone_de_residence"]=afrique_res
    moyen_depla=[]
    if str(data.loc[i,"_12_Quel_s_sont_le_s_moyen_s/1"]) != "0":
        depl = dict()
        depl["moyen_depl"]= data.loc[i,"_12_Quel_s_sont_le_s_moyen_s/1"]
        depl["duree"]=int(data.loc[i,"_12_1_Dur_e_moyenne_transport_individuel"])
        moyen_depla.append(depl)
    if str(data.loc[i,"_12_Quel_s_sont_le_s_moyen_s/2"]) != "0":
        depl = dict()
        depl["moyen_depl"]= data.loc[i,"_12_Quel_s_sont_le_s_moyen_s/2"]
        depl["duree"]=""
        moyen_depla.append(depl)
    if str(data.loc[i,"_12_Quel_s_sont_le_s_moyen_s/3"]) != "0":
        depl = dict()
        depl["moyen_depl"]= data.loc[i,"_12_Quel_s_sont_le_s_moyen_s/3"]
        depl["duree"]=int(data.loc[i,"_12_2_Dur_e_moyenne_transport_en_commun"])
        moyen_depla.append(depl)
    donnee_Geo["moyen_deplacement"]=moyen_depla
    
    
    cursorGeo = donnee_Geographique.find_one({"sejour_benin_6_dernier_mois" :donnee_Geo["sejour_benin_6_dernier_mois"],"zone_externe_visite" : donnee_Geo["zone_externe_visite"],"zone_interne_visite" :donnee_Geo["zone_interne_visite"],"zone_de_residence" :donnee_Geo["zone_de_residence"]},{'_id': 1}) 
    objects["ID_donneeGeographique"]=cursorGeo['_id']
    
    #recuperation de ID historique sante
    
    
    historique=dict()
    historique["_id"]=i
    historique["statut_covid"]=data.loc[i,"_85_Quel_est_votre_s_terme_du_d_pistage_"]
    raison_dep=list()
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/0"])!="0":
        raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/0"])
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/1"])!="0":
        raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/1"])
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/2"])!="0":
        raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/2"])
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/3"])!="0":
        raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/3"])
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/4"])!="0":
        raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/4"])
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/5"])!="0":
        raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/5"])
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/6"])!="0":
        raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/6"])
    #if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/999"])!="0":
        #raison_dep.append(data.loc[i,"_86_Quelles_raisons_vous_ont_m/999"])
    if str(data.loc[i,"_86_Quelles_raisons_vous_ont_m/7"])!="0":
        raison_dep.append(data.loc[i,"_86_7_Autres_pr_ciser"])
    historique["raison_depistage"]= raison_dep
    nombre_cas=dict()
    nombre_cas["dans_la_famille"]=float(data.loc[i,"_87_Combien_de_cas_o_le_0_si_aucun_cas"])
    nombre_cas["parmi_proche"]=str(data.loc[i,"_88_Combien_de_cas_o_es_0_si_aucun_cas"])
    historique["nombre_cas_confirme"]=nombre_cas
    historique["nutriment_preventifs_consommer"]=data.loc[i,"_89_Quels_sont_les_m_contre_la_COVID_19_"]
    antecedent_med=list()
    if str(data.loc[i,"_90_Souffrez_vous_de_/0"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/0"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/1"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/1"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/2"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/2"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/3"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/3"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/4"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/4"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/5"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/5"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/6"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/6"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/7"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/7"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/8"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/8"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/9"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/9"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/10"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/10"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/11"])!="0":
        antecedent_med.append(data.loc[i,"_90_Souffrez_vous_de_/11"])
    if str(data.loc[i,"_90_Souffrez_vous_de_/12"])!="0":
        antecedent_med.append(data.loc[i,"_90_12_Autres_pr_ciser"])
    
    historique["antecedent_medical"]= antecedent_med
    historique["enceinte_accoucher_recemment"]=data.loc[i,"_91_tes_vous_encein_accouch_r_cemment_"]
    consomme_stupefiant=dict()
    consomme_stupefiant["tabac"]=data.loc[i,"_92_Fumez_vous_du_tabac_"]
    consomme_stupefiant["chicha"]=data.loc[i,"_93_tes_vous_un_consommateur_"]
    consomme_stupefiant["consomme_en_groupe"]=data.loc[i,"_94_Si_oui_en_avez_v_consommer_en_groupe_"]
    historique["consomme_stupefiant"]=consomme_stupefiant
    
    
    cursorHist =historique_sante.find_one({"statut_covid" :historique["statut_covid"],"raison_depistage" : historique["raison_depistage"],"nombre_cas_confirme" :historique["nombre_cas_confirme"],"nutriment_preventifs_consommer" :historique["nutriment_preventifs_consommer"],"antecedent_medical" :historique["antecedent_medical"],"enceinte_accoucher_recemment" :historique["enceinte_accoucher_recemment"],"consomme_stupefiant" :historique["consomme_stupefiant"]},{'_id': 1})
    objects["ID_historiqueSante"]=cursorHist['_id']
    
    #recuperation de ID nutrition
    
    
    nutrit=dict()
    nutrit["_id"]=i
    consommation=dict()
    cereale=dict()
    cereale["poids"]=1
    cereale["score"]=int(data.loc[i,"Ma_s_riz_sorgho_mil_pain_e"])
    consommation["cereale"]=cereale
    tubercule=dict()
    tubercule["poids"]=1
    tubercule["score"]=int(data.loc[i,"Manioc_pommes_de_terre_et_pat"])
    consommation["tubercule"]=tubercule
    legumineuse_noix=dict()
    legumineuse_noix["poids"]=3
    legumineuse_noix["score"]=int(data.loc[i,"Haricots_pois_arachides_en_c"])
    consommation["legumineuse_noix"]=legumineuse_noix
    legume=dict()
    legume["poids"]=1
    legume["score"]=int(data.loc[i,"L_gumes_condiments_et_l_gumes"])
    consommation["legume"]=legume
    fruit=dict()
    fruit["poids"]=1
    fruit["score"]=int(data.loc[i,"Fruits_mangue_orange_ananas"])
    consommation["fruit"]=fruit
    viande_poisson=dict()
    viande_poisson["poids"]=4
    viande_poisson["score"]=int(data.loc[i,"B_uf_ch_vre_volailles_porc_"])
    consommation["viande_poisson"]=viande_poisson
    lait=dict()
    lait["poids"]=4
    lait["score"]=int(data.loc[i,"Lait_yaourt_et_autres_produit"])
    consommation["lait"]=lait
    sucre=dict()
    sucre["poids"]=0.5
    sucre["score"]=int(data.loc[i,"Sucre_et_produits_sucr_s"])
    consommation["sucre"]=sucre
    huile=dict()
    huile["poids"]=0.5
    huile["score"]=int(data.loc[i,"Huiles_mati_res_grasses_frit"])
    consommation["huile"]=huile
    nutrit["consommation_alimentaire"]=consommation
    consommation_alcool=dict()
    consommation_alcool["frequence"]=data.loc[i,"_96_quelle_fr_quence_vous_ar"]
    consommation_alcool["nbr_verres_jours_ordinaire"]=data.loc[i,"_97_Combien_de_verre_talopk_mi_de_sodabi"]
    consommation_alcool["frequence_6_verres_aumoins"]=data.loc[i,"_98_Au_cours_d_une_m_s_standards_ou_plus_"]
    nutrit["consommation_alcool"]=consommation_alcool
    nutrit["perte_poids"]=data.loc[i,"_99_Avez_vous_consta_derni_res_semaines_"]
    nutrit["prise_poids"]=data.loc[i,"_100_Avez_vous_const_derni_res_semaines_"]
    
    
    cursorNut =nutrition.find_one({"consommation_alimentaire":nutrit["consommation_alimentaire"],"consommation_alcool" : nutrit["consommation_alcool"],"perte_poids" :nutrit["perte_poids"],"prise_poids" :nutrit["prise_poids"]},{'_id': 1})
    objects["ID_nutrition"]=cursorNut['_id']
    
    #recuperation de ID perception covid
    
    
    perception_cov=dict()
    perception_cov["_id"]=i
    def_cov=list()
    if str(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/1"])!="0":
        def_cov.append(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/1"])
    if str(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/2"])!="0":
        def_cov.append(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/2"])
    if str(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/3"])!="0":
        def_cov.append(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/3"])
    if str(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/4"])!="0":
        def_cov.append(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/4"])
    if str(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/5"])!="0":
        def_cov.append(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/5"])
    if str(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/6"])!="0":
        def_cov.append(data.loc[i,"_63_6_Autre_pr_ciser"])
    #if str(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/999"])!="0":
        #def_cov.append(data.loc[i,"_63_Selon_vous_qu_est_ce_que_/999"])
    perception_cov["definition_covid19"]=def_cov
    peur_cov=dict()
    peur_cov["reponse"]=data.loc[i,"_64_Avez_vous_peur_de_la_COVID_19_"]
    raison_peur=list()
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/0"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/0"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/1"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/1"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/2"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/2"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/3"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/3"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/4"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/4"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/5"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/5"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/6"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/6"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/7"])!="0":
        raison_peur.append(data.loc[i,"_65_Pour_quelles_raisons_pense/7"])
    if str(data.loc[i,"_65_Pour_quelles_raisons_pense/8"])!="0":
        raison_peur.append(data.loc[i,"_65_9_Autre_pr_ciser"])
    peur_cov["raisons"]=raison_peur
    perception_cov["peur_covid19"]=peur_cov
    consequence_covid=dict()
    plan_social=list()
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/0"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/0"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/1"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/1"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/2"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/2"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/3"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/3"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/4"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/4"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/5"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/5"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/6"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/6"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/7"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/7"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/8"])!="0":
        plan_social.append(data.loc[i,"_66_Quelles_ont_t_les_cons_q/8"])
    if str(data.loc[i,"_66_Quelles_ont_t_les_cons_q/9"])!="0":
        plan_social.append(data.loc[i,"_66_9_Autre_pr_ciser"])
    consequence_covid["plan_social"]=plan_social
    sur_comportement=list()
    if str(data.loc[i,"_67_Quelles_ont_t_les_cons_q/0"])!="0":
        sur_comportement.append(data.loc[i,"_67_Quelles_ont_t_les_cons_q/0"])
    if str(data.loc[i,"_67_Quelles_ont_t_les_cons_q/1"])!="0":
        sur_comportement.append(data.loc[i,"_67_Quelles_ont_t_les_cons_q/1"])
    if str(data.loc[i,"_67_Quelles_ont_t_les_cons_q/2"])!="0":
        sur_comportement.append(data.loc[i,"_67_Quelles_ont_t_les_cons_q/2"])
    if str(data.loc[i,"_67_Quelles_ont_t_les_cons_q/3"])!="0":
        sur_comportement.append(data.loc[i,"_67_Quelles_ont_t_les_cons_q/3"])
    if str(data.loc[i,"_67_Quelles_ont_t_les_cons_q/4"])!="0":
        sur_comportement.append(data.loc[i,"_67_Quelles_ont_t_les_cons_q/4"])
    if str(data.loc[i,"_67_Quelles_ont_t_les_cons_q/5"])!="0":
        sur_comportement.append(data.loc[i,"_67_Quelles_ont_t_les_cons_q/5"])
    if str(data.loc[i,"_67_Quelles_ont_t_les_cons_q/9"])!="0":
        sur_comportement.append(data.loc[i,"_67_9_Autre_pr_ciser"])
    consequence_covid["sur_comportement"]=sur_comportement
    plan_economique=list()
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/0"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/0"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/1"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/1"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/2"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/2"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/3"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/3"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/4"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/4"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/5"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/5"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/6"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/6"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/7"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/7"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/8"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/8"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/9"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/9"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/10"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/10"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/11"])!="0":
        plan_economique.append(data.loc[i,"_68_Quelles_ont_t_les_cons_q/11"])
    if str(data.loc[i,"_68_Quelles_ont_t_les_cons_q/12"])!="0":
        plan_economique.append(data.loc[i,"_68_12_Autre_pr_ciser"])
    consequence_covid["plan_economique"]=plan_economique
    plan_sanitaire=list()
    if str(data.loc[i,"_69_Quelles_ont_t_les_cons_q/0"])!="0":
        plan_sanitaire.append(data.loc[i,"_69_Quelles_ont_t_les_cons_q/0"])
    if str(data.loc[i,"_69_Quelles_ont_t_les_cons_q/1"])!="0":
        plan_sanitaire.append(data.loc[i,"_69_Quelles_ont_t_les_cons_q/1"])
    if str(data.loc[i,"_69_Quelles_ont_t_les_cons_q/2"])!="0":
        plan_sanitaire.append(data.loc[i,"_69_Quelles_ont_t_les_cons_q/2"])
    if str(data.loc[i,"_69_Quelles_ont_t_les_cons_q/3"])!="0":
        plan_sanitaire.append(data.loc[i,"_69_Quelles_ont_t_les_cons_q/3"])
    if str(data.loc[i,"_69_Quelles_ont_t_les_cons_q/4"])!="0":
        plan_sanitaire.append(data.loc[i,"_69_Quelles_ont_t_les_cons_q/4"])
    if str(data.loc[i,"_69_Quelles_ont_t_les_cons_q/5"])!="0":
        plan_sanitaire.append(data.loc[i,"_69_5_Autre_pr_ciser"])
    consequence_covid["plan_sanitaire"]=plan_sanitaire
    perception_cov["consequence_covid19"]=consequence_covid
    
    cause_covid=list()
    if str(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/0"])!="0":
        cause_covid.append(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/0"])
    if str(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/1"])!="0":
        cause_covid.append(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/1"])
    if str(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/2"])!="0":
        cause_covid.append(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/2"])
    if str(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/3"])!="0":
        cause_covid.append(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/3"])
    if str(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/4"])!="0":
        cause_covid.append(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/4"])
    if str(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/5"])!="0":
        cause_covid.append(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/5"])
    if str(data.loc[i,"_70_Selon_vous_y_a_t_il_des_c/6"])!="0":
        cause_covid.append(data.loc[i,"_70_6_Autre_pr_ciser"])
    perception_cov["cause_covid19"]=cause_covid
    provoque_cov=dict()
    provoque_cov["reponse"]=data.loc[i,"_71_Est_ce_que_la_Covid_19_peu"]
    provoque_cov["justification"]=data.loc[i,"_72_Si_oui_comment_"] if str(data.loc[i,"_71_Est_ce_que_la_Covid_19_peu"])=="Oui" else  data.loc[i,"_73_Si_non_pourquoi_"]
    perception_cov["provoquer_covid19"]=provoque_cov
    
    
    cursorPer =perception_covid19.find_one({"definition_covid19" :perception_cov["definition_covid19"],"peur_covid19" : perception_cov["peur_covid19"],"consequence_covid19" :perception_cov["consequence_covid19"],"cause_covid19" :perception_cov["cause_covid19"],"provoquer_covid19" :perception_cov["provoquer_covid19"]},{'_id': 1}) 
    objects["ID_perceptionCocid"]=cursorPer['_id']
    
    #recuperation de ID pratique covid
    
    
    pratique_covid=dict()
    pratique_covid["_id"]=i
    pratique_covid["nbr_ceremonies_recente"]=int(data.loc[i,"_29_A_combien_de_c_r_monies_r"])

    lieu_celeb=[]
    if str(data.loc[i,"_30_O_avaient_elles_lieu_et_q/1"])!='nan':
        celebration=dict()
        celebration["lieu"]=data.loc[i,"_30_O_avaient_elles_lieu_et_q/1"]
        celebration["nombre_participants"]=float(data.loc[i,"_30_1_A_domicile_nombre_approximatif"])
        lieu_celeb.append(celebration)
        
    if str(data.loc[i,"_30_O_avaient_elles_lieu_et_q/2"])!='nan':
        celebration=dict()
        celebration["lieu"]=data.loc[i,"_30_O_avaient_elles_lieu_et_q/2"]
        celebration["nombre_participants"]=float(data.loc[i,"_30_2_Chez_un_proche_nombre_approximatif"])
        lieu_celeb.append(celebration)
        
    if str(data.loc[i,"_30_O_avaient_elles_lieu_et_q/3"])!='nan':
        celebration=dict()
        celebration["lieu"]=data.loc[i,"_30_O_avaient_elles_lieu_et_q/3"]
        celebration["nombre_participants"]=float(data.loc[i,"_30_3_Dans_un_lieu_p_nombre_approximatif"])
        lieu_celeb.append(celebration)
    pratique_covid["lieu_celebration"]=lieu_celeb
    pratique_covid["frequence_visite_hopitaux"]=float(data.loc[i,"_31_Combien_de_fois_0_si_pas_fr_quent"])
    pratique_covid["frequence_visite_lieu_rejouissance"]=float(data.loc[i,"_32_Combien_de_fois_0_si_pas_fr_quent"])
    pratique_covid["frequence_visite_lieu_religieux"]=float(data.loc[i,"_33_Combien_de_fois_0_si_pas_fr_quent"])
    pratique_covid["frequence_visite_manifestation_sportive"]=float(data.loc[i,"_34_Combien_de_fois_0_si_pas_fr_quent"])
    pratique_covid["frequence_visite_lieu_rejouissance"]=float(data.loc[i,"_32_Combien_de_fois_0_si_pas_fr_quent"])
    
    contact_physique=dict()
    contact_physique["frequence"]=float(data.loc[i,"_35_Combien_de_fois_avez_vous_"])
    contact_physique["nombre_participants"]=float(data.loc[i,"_36_Quel_tait_le_no_ticip_ces_matchs_"])
    
    sans_physique=dict()
    sans_physique["frequence"]=float(data.loc[i,"_37_Combien_de_fois_avez_vous_"])
    sans_physique["nombre_participants"]=float(data.loc[i,"_37_2_Quel_tait_le_ticip_ces_matchs_"])
    
    mes_physiques=dict()
    mes_physiques["avec_contact_physique"]=contact_physique
    mes_physiques["sans_contact_physique"]=sans_physique
    
    pratique_covid["frequence_participation_match"]=mes_physiques
    
    plage=dict()
    plage["frequence"]=float(data.loc[i,"_38_Combien_de_fois_avez_vous_"])
    plage["nombre_participants"]=float(data.loc[i,"_39_Quel_tait_le_no_ge_non_loin_de_vous_"])
    pratique_covid["frequence_visiste_plage"]=plage
    
    education=dict()
    education["frequence"]=float(data.loc[i,"_40_Combien_de_fois_avez_vous_"])
    education["nombre_participants"]=float(data.loc[i,"_41_Quel_tait_le_no_que_vous_aux_cours_"])
    education["mesure_barriere_respecte"]=data.loc[i,"_41_1_Aviez_vous_l_i_taient_respect_es_"]
    pratique_covid["frequence_visite_lieu_educatif"]=education
    
    banque=dict()
    banque["frequence"]=float(data.loc[i,"_42_Combien_de_fois_vous_tes_"])
    banque["nombre_participants"]=float(data.loc[i,"_43_Quel_tait_le_no_d_attente_que_vous_"])
    banque["mesure_barriere_respecte"]=data.loc[i,"_43_1_Aviez_vous_l_i_taient_respect_es_"]
    pratique_covid["frequence_visite_banque"]=banque
    
    boutique=dict()
    boutique["frequence"]=float(data.loc[i,"_44_Combien_de_fois_vous_tes_"])
    boutique["nombre_participants"]=float(data.loc[i,"_45_Quel_tait_le_no_e_boutique_que_vous_"])
    boutique["mesure_barriere_respecte"]=data.loc[i,"_45_1_Aviez_vous_l_i_taient_respect_es_"]
    pratique_covid["frequence_visite_boutique"]=boutique
    
    superette=dict()
    superette["frequence"]=float(data.loc[i,"_45_bis1_Combien_de_fois_vous_"])
    superette["nombre_participants"]=float(data.loc[i,"_45_bis1_1_Quel_tai_sup_rette_que_vous_"])
    superette["mesure_barriere_respecte"]=data.loc[i,"_45_bis_1_2_Aviez_vo_taient_respect_es_"]
    pratique_covid["frequence_visite_superette"]=superette
    
    hypermarche=dict()
    hypermarche["frequence"]=float(data.loc[i,"_45_bis2_Combien_de_fois_vous_"])
    hypermarche["nombre_participants"]=float(data.loc[i,"_45_bis2_1_Quel_tai_ypermarch_que_vous_"])
    hypermarche["mesure_barriere_respecte"]=data.loc[i,"_45_bis_2_2_Aviez_vo_taient_respect_es_"]
    pratique_covid["frequence_visite_hypermarche"]=hypermarche
    
    marche=dict()
    marche["frequence"]=float(data.loc[i,"_46_Combien_de_fois_vous_tes_"])
    marche["mesure_barriere_respecte"]=data.loc[i,"_46_1_Aviez_vous_l_i_taient_respect_es_"]
    pratique_covid["frequence_visite_marche"]=marche
    
    infos_masque=dict()
    infos_masque["type_de_masque"]=data.loc[i,"_47_Quel_type_de_masque_avez_v"]
    raisons=[]
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/0"])!="0":
        raisons.append(data.loc[i,"_47_1_Quelles_sont_les_raisons/0"])
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/1"])!="0":
        raisons.append(data.loc[i,"_47_1_Quelles_sont_les_raisons/1"])
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/2"])!="0":
        raisons.append(data.loc[i,"_47_1_Quelles_sont_les_raisons/2"])
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/3"])!="0":
        raisons.append(data.loc[i,"_47_1_Quelles_sont_les_raisons/3"])
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/4"])!="0":
        raisons.append(data.loc[i,"_47_1_Quelles_sont_les_raisons/4"])
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/5"])!="0":
        raisons.append(data.loc[i,"_47_1_Quelles_sont_les_raisons/5"])
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/6"])!="0":
        raisons.append(data.loc[i,"_47_1_Quelles_sont_les_raisons/6"])
    if str(data.loc[i,"_47_1_Quelles_sont_les_raisons/7"])!="0":
        raisons.append(data.loc[i,"_47_1_7_Autre_pr_ciser"])
    infos_masque["raison_choix_type"]=raisons
    
    contre_raisons=[]
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/0"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/0"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/1"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/2"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/2"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/2"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/3"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/3"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/4"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/4"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/5"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/5"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/6"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/6"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/7"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_Si_pas_de_port_du_masque/7"])
    if str(data.loc[i,"_47_2_Si_pas_de_port_du_masque/8"])!="0":
        contre_raisons.append(data.loc[i,"_47_2_7_Autre_pr_ciser"])
    infos_masque["contre_raison"]=contre_raisons
    frequence_port=dict()
    frequence_port["dans_logement_principal"]=data.loc[i,"_48_1_A_quelle_fr_qu_d_autres_personnes_"]
    frequence_port["dans_rue"]=data.loc[i,"_48_2_A_quelle_fr_qu_masque_dans_la_rue_"]
    frequence_port["dans_transport_individuel"]=data.loc[i,"_48_3_A_quelle_fr_qu_uel_voiture_moto_"]
    frequence_port["dans_transport_commun"]=data.loc[i,"_48_4_A_quelle_fr_qu_voiture_moto_bus_"]
    frequence_port["dans_lieu_travail"]=data.loc[i,"_48_5_A_quelle_fr_qu_d_autres_personnes_"]
    frequence_port["dans_lieu_public"]=data.loc[i,"_48_6_A_quelle_fr_qu_ch_lieu_religieux_"]
    infos_masque["frequence_port_masque"]=frequence_port
    infos_masque["freqence_entretient_masque"]=data.loc[i,"_49_A_quelle_fr_quen_viez_vous_ce_masque_"]
    infos_masque["abaisser_masque"]=data.loc[i,"_49_1_Vous_arrivait_du_menton_ou_du_cou_"]
    pratique_covid["information_masque"]=infos_masque
    
    entretien_main=dict()
    entretien_main["entrer_lieu_public"]=data.loc[i,"_50_A_quelle_fr_quen_ch_lieu_religieux_"]
    entretien_main["sorti_lieu_public"]=data.loc[i,"_51_A_quelle_fr_quen_ch_lieu_religieux_"]
    entretien_main["sorti_logement"]=data.loc[i,"_52_A_quelle_fr_quen_r_de_votre_logement_"]
    entretien_main["retour_logement"]=data.loc[i,"_53_A_quelle_fr_quen_ur_votre_logement_"]
    entretien_main["touche_instrument_non_desinfecter"]=data.loc[i,"_54_A_quelle_fr_quen_n_e_de_porte_cl_s_"]
    pratique_covid["frequence_entretient_main"]=entretien_main
    
    pratique_covid["frequence_utilisation_gel"]=data.loc[i,"_55_A_quelle_fr_quen_de_mani_re_g_n_rale_"]
    pratique_covid["frequence_desinfection_objet_logement"]=data.loc[i,"_56_A_quelle_fr_quen_urs_du_dernier_mois_"]
    pratique_covid["frequence_desinfection_objet_travail"]=data.loc[i,"_57_A_quelle_fr_quen_urs_du_dernier_mois_"]
    pratique_covid["frequence_touche_visage"]=data.loc[i,"_58_A_quelle_fr_quen_urs_du_dernier_mois_"]
    pratique_covid["frequence_serrer_mains"]=data.loc[i,"_59_A_quelle_fr_quen_urs_du_dernier_mois_"]
    pratique_covid["frequence_rapport_sexuel"]=data.loc[i,"_60_A_quelle_fr_quen_urs_du_dernier_mois_"]
    pratique_covid["malade_dernier_mois"]=data.loc[i,"_61_Avez_vous_t_malade_au_co"]
    lieu_guerision=list()
    
    if str(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/1"])!="0":
        lieu_guerision.append(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/1"])
    if str(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/2"])!="0":
        lieu_guerision.append(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/2"])
    if str(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/3"])!="0":
        lieu_guerision.append(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/3"])
    if str(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/4"])!="0":
        lieu_guerision.append(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/4"])
    if str(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/5"])!="0":
        lieu_guerision.append(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/5"])
    if str(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/6"])!="0":
        lieu_guerision.append(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/6"])
    if str(data.loc[i,"_62_Si_oui_avez_vous_eu_recou/7"])!="0":
        lieu_guerision.append(data.loc[i,"_62_7_Autres_pr_ciser"])
    pratique_covid["lieu_preferentiel_guerison"]=lieu_guerision

    
    cursorPra =pratique_covid19.find_one({"nbr_ceremonies_recente" :pratique_covid["nbr_ceremonies_recente"],"lieu_celebration" : pratique_covid["lieu_celebration"],"frequence_visite_hopitaux" :pratique_covid["frequence_visite_hopitaux"],"frequence_visite_lieu_rejouissance" :pratique_covid["frequence_visite_lieu_rejouissance"],"frequence_visite_lieu_religieux" :pratique_covid["frequence_visite_lieu_religieux"],"frequence_visite_manifestation_sportive" :pratique_covid["frequence_visite_manifestation_sportive"],"frequence_participation_match" :pratique_covid["frequence_participation_match"],"frequence_visiste_plage" :pratique_covid["frequence_visiste_plage"],"frequence_visite_lieu_educatif" :pratique_covid["frequence_visite_lieu_educatif"],"frequence_visite_banque" :pratique_covid["frequence_visite_banque"],"frequence_visite_boutique" :pratique_covid["frequence_visite_boutique"],"frequence_visite_superette" :pratique_covid["frequence_visite_superette"],"frequence_visite_hypermarche" :pratique_covid["frequence_visite_hypermarche"],"frequence_visite_marche" :pratique_covid["frequence_visite_marche"],"information_masque" :pratique_covid["information_masque"],"frequence_entretient_main" :pratique_covid["frequence_entretient_main"],"frequence_utilisation_gel" :pratique_covid["frequence_utilisation_gel"],"frequence_desinfection_objet_logement" :pratique_covid["frequence_desinfection_objet_logement"],"frequence_desinfection_objet_travail" :pratique_covid["frequence_desinfection_objet_travail"],"frequence_touche_visage" :pratique_covid["frequence_touche_visage"],"frequence_serrer_mains" :pratique_covid["frequence_serrer_mains"],"frequence_rapport_sexuel" :pratique_covid["frequence_rapport_sexuel"],"malade_dernier_mois" :pratique_covid["malade_dernier_mois"],"lieu_preferentiel_guerison" :pratique_covid["lieu_preferentiel_guerison"]},{'_id': 1})
    objects["ID_pratiqueCovid"]=cursorPra['_id']
    
    
    dim_objet.insert_one(objects)
    
    
    


# In[29]:


ID_list = list()
for  i in range(0,len(data.iloc[0:,0])):
    
print(ID_list)


# In[ ]:




