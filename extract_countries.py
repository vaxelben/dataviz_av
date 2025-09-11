#!/usr/bin/env python3
"""
Script pour extraire la liste distincte des pays du fichier world_atlas.json
"""

import json
import sys
from pathlib import Path

def extract_countries_from_atlas(json_file_path):
    """
    Extrait la liste distincte des pays du fichier world_atlas.json
    
    Args:
        json_file_path (str): Chemin vers le fichier JSON
        
    Returns:
        list: Liste triée des noms de pays distincts
    """
    try:
        # Charger le fichier JSON
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # Extraire les pays
        countries = set()
        
        # Naviguer dans la structure TopoJSON
        if 'objects' in data and 'countries' in data['objects']:
            geometries = data['objects']['countries'].get('geometries', [])
            
            for geometry in geometries:
                if 'properties' in geometry and 'name' in geometry['properties']:
                    country_name = geometry['properties']['name']
                    if country_name:  # Vérifier que le nom n'est pas vide
                        countries.add(country_name.strip())
        
        # Convertir en liste triée
        sorted_countries = sorted(list(countries))
        
        return sorted_countries
    
    except FileNotFoundError:
        print(f"Erreur: Le fichier '{json_file_path}' n'a pas été trouvé.")
        return []
    except json.JSONDecodeError as e:
        print(f"Erreur: Impossible de décoder le fichier JSON - {e}")
        return []
    except Exception as e:
        print(f"Erreur inattendue: {e}")
        return []

def save_countries_to_file(countries, output_file):
    """
    Sauvegarde la liste des pays dans un fichier texte
    
    Args:
        countries (list): Liste des pays
        output_file (str): Chemin du fichier de sortie
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as file:
            for country in countries:
                file.write(f"{country}\n")
        print(f"Liste des pays sauvegardée dans '{output_file}'")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde: {e}")

def main():
    """Fonction principale"""
    # Chemin vers le fichier JSON
    json_file = "brief/world_atlas.json"
    
    # Vérifier que le fichier existe
    if not Path(json_file).exists():
        print(f"Erreur: Le fichier '{json_file}' n'existe pas.")
        sys.exit(1)
    
    print("Extraction des pays du fichier world_atlas.json...")
    
    # Extraire les pays
    countries = extract_countries_from_atlas(json_file)
    
    if not countries:
        print("Aucun pays trouvé ou erreur lors de l'extraction.")
        sys.exit(1)
    
    # Afficher les résultats
    print(f"\n{len(countries)} pays distincts trouvés:")
    print("-" * 40)
    
    for i, country in enumerate(countries, 1):
        print(f"{i:3d}. {country}")
    
    # Proposer de sauvegarder dans un fichier
    save_to_file = input("\nVoulez-vous sauvegarder la liste dans un fichier? (o/n): ").lower().strip()
    
    if save_to_file in ['o', 'oui', 'y', 'yes']:
        output_file = input("Nom du fichier de sortie (par défaut: pays_extraits.txt): ").strip()
        if not output_file:
            output_file = "pays_extraits.txt"
        
        save_countries_to_file(countries, output_file)
    
    print(f"\nExtraction terminée. Total: {len(countries)} pays.")

if __name__ == "__main__":
    main()
