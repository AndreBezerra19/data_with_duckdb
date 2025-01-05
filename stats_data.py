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

# Função para buscar os stats de cada Pokémon
def fetch_pokemon_stats_expanded(pokemon_data):
    stats_data = []

    for pokemon in pokemon_data:
        pokemon_id = pokemon["id"]
        response = requests.get(f"{api_url}{pokemon_id}")
        if response.status_code == 200:
            data = response.json()
            stats = data.get("stats", [])
            
            # Iterando pelas estatísticas
            for stat_info in stats:
                stats_data.append({
                    "id": pokemon_id,
                    "stat": stat_info["stat"]["name"],
                    "effort": stat_info["effort"],
                    "base_stat": stat_info["base_stat"]
                })
        else:
            print(f"Erro ao acessar detalhes do Pokémon ID {pokemon_id}: {response.status_code}")

    return stats_data

# Collect pokemon data
pokemon_data = fetch_all_pokemon(api_url)

# Collect pokemons stats
stats_data_expanded = fetch_pokemon_stats_expanded(pokemon_data)

# Create stats dataframe
df_stats_expanded = pd.DataFrame(stats_data_expanded)

# Create DuckDB database
conn = duckdb.connect("poke_api_data.duckdb")
conn.execute("""
    CREATE TABLE IF NOT EXISTS stats ( 
      id INT,
      stat TEXT,
      effort TEXT,
      base_stat INT
    )
""")
conn.execute("""INSERT INTO stats SELECT * FROM df_stats_expanded""")
conn.close()