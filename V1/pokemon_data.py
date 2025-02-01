# Importing the required libraries
import requests
import pandas as pd
import duckdb

# Api url
api_url = "https://pokeapi.co/api/v2/pokemon/"

# Extract ID data
def extract_id(url):
    return url.rstrip('/').split('/')[-1]

# Function to collect all Pokémon data
def fetch_all_pokemon(api_url):
    pokemon_data = []
    offset = 0
    limit = 20  # Número de resultados por página

    while True:
        # URL with pagination
        response = requests.get(api_url, params={"offset": offset, "limit": limit})
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            
            # Add the page data to our dataset
            pokemon_data.extend(
                {"name": item["name"], "id": extract_id(item["url"])} for item in results
            )
            
            # Check for more results
            if data.get('next') is None:  # 'next' will be None when there are no more pages
                break
            
            # Next page
            offset += limit
        else:
            print(f"Erro ao acessar a API: {response.status_code}")
            break

    return pokemon_data

# collect data
pokemon_data = fetch_all_pokemon(api_url)

# Create DataFrame
df_pokemon = pd.DataFrame(pokemon_data)

# Create DuckDB database
conn = duckdb.connect("poke_api_data.duckdb")
conn.execute("""
    CREATE TABLE IF NOT EXISTS pokemon ( 
      name TEXT,
      id INT,
    )
""")
conn.execute("""INSERT INTO pokemon SELECT * FROM df_pokemon""")
conn.close()