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
        # Monta a URL com paginação
        response = requests.get(api_url, params={"offset": offset, "limit": limit})
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            
            # Adiciona os dados da página ao nosso dataset
            pokemon_data.extend(
                {"name": item["name"], "id": extract_id(item["url"])} for item in results
            )
            
            # Verifica se há mais resultados
            if data.get('next') is None:  # 'next' será None quando não houver mais páginas
                break
            
            # Avança para a próxima página
            offset += limit
        else:
            print(f"Erro ao acessar a API: {response.status_code}")
            break

    return pokemon_data

# Função para buscar os tipos de cada Pokémon
def fetch_pokemon_types(pokemon_data):
    detailed_data = []

    for pokemon in pokemon_data:
        pokemon_id = pokemon["id"]
        response = requests.get(f"{api_url}{pokemon_id}")
        if response.status_code == 200:
            data = response.json()
            types = data.get("types", [])
            
            # Inicializando os tipos
            type_1 = None
            type_2 = None
            
            # Iterando nos tipos para extrair conforme o slot
            for type_info in types:
                if type_info["slot"] == 1:
                    type_1 = type_info["type"]["name"]
                elif type_info["slot"] == 2:
                    type_2 = type_info["type"]["name"]
            
            # Adiciona ao dataset detalhado
            detailed_data.append({
                "id": pokemon_id,
                "type_1": type_1,
                "type_2": type_2
            })
        else:
            print(f"Erro ao acessar detalhes do Pokémon ID {pokemon_id}: {response.status_code}")

    return detailed_data

# Coletando os dados iniciais
pokemon_data = fetch_all_pokemon(api_url)

# Buscando os detalhes de tipo
detailed_data = fetch_pokemon_types(pokemon_data)

# Criando o DataFrame detalhado
df_types = pd.DataFrame(detailed_data)

# Create DuckDB database
conn = duckdb.connect("poke_api_data.duckdb")
conn.execute("""
    CREATE TABLE IF NOT EXISTS types ( 
      id INT,
      type_1 TEXT,
      type_2 TEXT
    )
""")
conn.execute("""INSERT INTO types SELECT * FROM df_types""")
conn.close()