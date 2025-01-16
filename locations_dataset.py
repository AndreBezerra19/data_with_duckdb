import requests
import pandas as pd
import duckdb

# Listar todos os IDs
def get_all_pokemon_ids():
    # URL para listar todos os Pokémon
    url = "https://pokeapi.co/api/v2/pokemon?limit=10000"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        # Extrair IDs dos URLs
        pokemon_ids = [
            int(pokemon["url"].split("/")[-2]) for pokemon in data["results"]
        ]
        return pokemon_ids
    else:
        raise Exception(f"Erro ao acessar a API: {response.status_code}")
    
# Função para buscar a main_region de um Pokémon pelo ID
def get_gen(extract_id):
    url = f"https://pokeapi.co/api/v2/pokemon-species/{extract_id}/" 
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        return data["generation"]["name"]
    else:
        return None 
    
# Função para buscar o nome da regiao de cada geracao
def get_main_region(gen_name):
    url = f"https://pokeapi.co/api/v2/generation/{gen_name}/"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        return data["main_region"]["name"]
    else:
        return None
    
# Obter todos os IDs de Pokémon
pokemon_ids = get_all_pokemon_ids()

# Lista para armazenar os resultados
data = []

# Buscar a main_region para cada ID
for pokemon_id in pokemon_ids:
    generation = get_gen(pokemon_id)
    data.append({"id": pokemon_id, "generation": generation})

# Criar o DataFrame
df_locations = pd.DataFrame(data)

# Adiciona o nome da regiao de origem de cada pokemon
df_locations["main_region"] = df_locations["generation"].apply(get_main_region)

# Create DataFrame
df_regions = pd.DataFrame(df_locations)

# Create DuckDB database
conn = duckdb.connect("poke_api_data.duckdb")
conn.execute("""
    CREATE TABLE IF NOT EXISTS regions ( 
      id INT,
      generation TEXT,
      main_region TEXT
    )
""")
conn.execute("""INSERT INTO regions SELECT * FROM df_regions""")
conn.close()

print(df_locations.head())