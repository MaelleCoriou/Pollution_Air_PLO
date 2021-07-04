# SQLite-functions
import os
import sqlite3


def ouvrir_connection(nom_bdd):
    conn = None
    try:
        conn = sqlite3.connect(dbname=nom_bdd)
        print('Connexion ok')
        print(sqlite3.version)
    except sqlite3.Error as e:
        print("Erreur lors de la connection à la base de données")
        print(e)
        return None   
    return conn


def supprimer_table(conn, sql_suppression_table):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_suppression_table)
        conn.commit()
    except sqlite3.Error as e:
        print("Erreur lors de la suppression de la table")
        print(e)
        return
    cursor.close()
    print("La table a été supprimée avec succès")

    
def creer_table(conn, sql_creation_table):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_creation_table)
        conn.commit()
    except sqlite3.Error as e:
        print("Erreur lors de la création de la table")
        print(e)
        return
    cursor.close()
    print("La table a été crée avec succès")

    
def inserer_donnees(conn, sql_insertion_table, donnees):
    try:
        cursor = conn.cursor()
        for d in donnees:
            cursor.execute(sql_insertion_table, d)
        conn.commit()
    except sqlite3.Error as e:
        print("Erreur lors de l'insertion des données")
        print(e)
        return
    cursor.close()
    print("Les données ont été insérées avec succès")

    
def lire_donnees(conn, sql_requete):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_requete)
        conn.commit()
    except sqlite3.Error as e:
        print("Erreur lors de la lecture des données")
        print(e)
        return None
    
    print("Les données ont été lues avec succès")
    data = []
    for row in cursor:
        data.append(row)

    cursor.close()
    
    return data

def modification_table(conn, modification_table):
    try:
        cursor = conn.cursor()
        cursor.execute(modification_table)
        conn.commit()
    except sqlite3.Error as e:
        print("Erreur lors de la création de la modification")
        print(e)
        return
    cursor.close()
    print("Modification effectuée avec succès")

def modif_table(conn, modif_table):
    cursor = conn.cursor()
    cursor.execute(modif_table)
    conn.commit()
    cursor.close()
