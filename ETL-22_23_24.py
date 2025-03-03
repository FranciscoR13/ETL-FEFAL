from rapidfuzz import process, fuzz
from unidecode import unidecode
import pandas as pd
import dataframe_image as dfi
from openpyxl import load_workbook
from openpyxl.styles import Alignment
from spellchecker import SpellChecker  
import psutil
import json
import re
import os

#================================================ FUNÇÕES ===============================================#

# Função para fechar o ficheiro de destino caso esteja aberto
def close_excel():
    for process in psutil.process_iter(attrs=["pid", "name"]):
        if "EXCEL.EXE" in process.info["name"]:                     # Verifica se o Excel está aberto
            os.system(f"taskkill /PID {process.info['pid']} /F")    # Força o encerramento
            return

# Função para corrigir erros ortográficos
def correct_text(texto):
    palavras = texto.split()
    palavras_corrigidas = [spell.correction(palavra) if spell.correction(palavra) else palavra for palavra in palavras]
    return " ".join(palavras_corrigidas).upper()                                                                         

# Função para normalizar os dados
def normalize_text(texto):
    if not isinstance(texto, str) or not texto.strip():  
        return ""  
    
    texto = texto.strip()                # Remover espaços no início e no fim
    texto = unidecode(texto)             # Remover acentos
    texto = re.sub(r"\s+", " ", texto)   # Substituir múltiplos espaços por um único espaço
    texto = texto.upper()                # Converter para maiúsculas
    
    return texto
    
# Função para normalizar texto e remover prefixos
def clean_text(text):
    if not isinstance(text, str):
        return text
    text = unidecode(text).strip().upper()
    for prefix in remover_prefixos:
        text = re.sub(prefix, "", text)
    return text

# Função para encontrar a melhor correspondência para um nome de coluna
def find_best_match(nome_coluna, lista_colunas, score_minimo=80):
    nome_coluna_norm = normalize_text(nome_coluna)  # Normalizar o nome da coluna alvo
    
    correspondencias = [
        (col, fuzz.partial_ratio(nome_coluna_norm, normalize_text(col))) 
        for col in lista_colunas
    ]
    
    correspondencias.sort(key=lambda x: x[1], reverse=True)  # Ordenar por melhor score
    
    melhor_correspondencia, melhor_score = correspondencias[0]


    return melhor_correspondencia if melhor_score >= score_minimo else None

# Função para encontrar a melhor correspondência de coluna
def get_best_column(df, column_name, aliases, score_minimo=80):
    # Primeiro, verifica se a coluna principal existe diretamente
    if column_name in df.columns:
        return column_name

    # Se a coluna não existir diretamente, verificar os aliases
    for alias in aliases.get(column_name, []):
        # Tentar encontrar a melhor correspondência de fuzzy matching para cada alias
        best_match_column = find_best_match(alias, df.columns, score_minimo)

        if best_match_column:
            return list(aliases.keys())[0]

    return None

# Função de fuzzy matching genérica para concelhos e freguesias
def fuzzy_match_local(valor, local_list, limite_score=95):
    # Verifica se o valor tem uma correspondência fuzzy suficiente com qualquer localidade da lista
    for local in local_list:
        if fuzz.partial_ratio(clean_text(valor), clean_text(local)) > limite_score:
            return local
    return None


#========================================================================================================#

#================================================ INICIO ================================================#

# Carregar JSON
with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

# Aceder às variáveis do JSON
ano = config["ano"]
file_path_in = config["file_paths"]["input"].format(ano=ano)
file_path_out = config["file_paths"]["output"].format(ano=ano)
file_path_concelhos = config["file_paths"]["concelhos"]
file_path_freguesias = config["file_paths"]["freguesias"]

cols_targets = config["columns"]["targets"]
aliases = config["columns"].get("aliases", {})
entidade_type = config["columns"]["entity_type"]
col_nformandos = config["columns"]["num_formandos"]

# Ler o ficheiro Excel
df = pd.read_excel(file_path_in)

# Normalizar os nomes das colunas
# Aplicar a normalização a todas as colunas
df.columns = [normalize_text(col) for col in df.columns]

# Identificar a melhor coluna para verificar duplicados
coluna_verificar = get_best_column(df, config["columns"]["check_duplicates"], aliases)
if not coluna_verificar:
    raise ValueError("Erro: Nenhuma correspondência encontrada para 'check_duplicates'.")

# Verificar e selecionar colunas existentes
# cols_existentes = [col for col in cols_targets if col in df.columns]
# if not cols_existentes:
#     raise ValueError("Erro: Nenhuma das colunas alvo foi encontrada no DataFrame.")

# df = df[cols_existentes]  

valores_invalidos = config["invalid_values"]
ws_title = config["ws_title"]
limite_fuzzy = config["fuzzy_limit"]

municipio_keywords = config["keywords"]["municipio"]
freguesia_keywords = config["keywords"]["freguesia"]
tipos_entidade_validos = config["keywords"]["entity_types"]

palavras_chave_formacao = config["keywords"]["training"]
palavras_chave_comentario = config["keywords"]["comment"]
palavras_chave_tempo_grupo = config["keywords"]["group_time"]
palavras_chave_areas_tematicas = config["keywords"]["thematic_areas"]
palavras_chave_continua = config["keywords"]["continuous_training"]
palavras_chave_preferencia = config["keywords"]["preference"]

descricao_comentario = config["descriptions"]["comment"]
descricao_tempo_grupo = config["descriptions"]["group_time"]
descricao_interesse = config["descriptions"]["interest"]
descricao_continua = config["descriptions"]["continuous_training"]
descricao_preferencia = config["descriptions"]["preference"]
descricao_formacao_curso = config["descriptions"]["training_course"]

data_sub_key = config["data_keys"]["submission_date"]
data_inicio_key = config["data_keys"]["start_date"]
data_fim_key = config["data_keys"]["end_date"]
col_sub = config["data_keys"]["submitted"]
col_temp = config["data_keys"]["completion_time"]

VALOR_VAZIO = config["default_values"]["empty"]
VALOR_NAO = config["default_values"]["no"]

trainings = config["trainings"]
interests = config["interests"]

remover_prefixos = config["prefixs"]

# Criar um dicionário de mapeamento {nome_original: nome_modificado}
mapeamento_colunas = {}
cols_comentarios_fc = []
cols_interesses_fc = []
cols_continua_fc = []
cols_sumformados = []

# Lista de Concelhos
with open(file_path_concelhos, 'r', encoding='utf-8') as file:
    concelhos = file.read()
concelhos = unidecode(concelhos)
concelhos_list = concelhos.split('\n')

# Lista de Freguesias
with open(file_path_freguesias, 'r', encoding='utf-8') as file:
    freguesias = file.read()
freguesias = unidecode(freguesias)
freguesias_list = freguesias.split('\n')

spell = SpellChecker(language="pt")  

#========================================================================================================#

#=========================================== PREPARAR DATASET ===========================================#

# Fechar o excl de destino caso aberto
close_excel()

# Ler o ficheiro Excel
df = pd.read_excel(file_path_in)

# Remover colunas vazias
df = df.dropna(axis=1, how='all')

# Normalizar os nomes das colunas do DataFrame
df.columns = [normalize_text(col) for col in df.columns]

df = df.applymap(lambda x: normalize_text(x) if isinstance(x, str) else x)

# Dicionário para armazenar a melhor correspondência de cada coluna-alvo
best_matches = {}

# Encontrar a melhor correspondência para cada coluna-alvo
for target in cols_targets:
    # Verifica a correspondência direta
    best_match = find_best_match(target, df.columns)

    # Se uma correspondência direta for encontrada, adiciona ao dicionário e pula para a próxima iteração
    if best_match:
        best_matches[best_match] = target
        print(f"{target} <-> {best_match}")
        continue  

    # Se o target tiver aliases, verificar também as colunas correspondentes
    if target in aliases:
        for alias in aliases[target]:
            alias_match = find_best_match(alias, df.columns)
            if alias_match and alias_match not in best_matches:
                best_matches[alias_match] = target
                print(f"{alias} <-> {alias_match}")

# Renomear as colunas do DataFrame
df = df.rename(columns=best_matches)

# Verificar se a coluna existe antes de tentar remover valores inválidos
if coluna_verificar in df.columns:
    # Filtrar valores inválidos, se houver
    valores_invalidos_encontrados = df[coluna_verificar].isin(valores_invalidos)

    if len(valores_invalidos_encontrados) > 0:
        dados_removidos = df[valores_invalidos_encontrados]
        df = df[~valores_invalidos_encontrados]  # Remover valores inválidos

        # Imprimir os valores removidos
        # print("\nValores removidos por serem inválidos:")
        # print("\n".join(map(str, dados_removidos[coluna_verificar].tolist())))
        # print("\n")


    # Remover linhas com valores nulos na coluna de verificação
    df = df.dropna(subset=[coluna_verificar])

else:
    print(f"Aviso: A coluna '{coluna_verificar}' não existe no DataFrame. Nenhuma remoção foi feita.")
    

#========================================================================================================#

#========================================== COLUNA DE SUBMISSÃO =========================================#

# Identificar as colunas corretas usando match flexível
col_submissao = find_best_match(normalize_text(data_sub_key), df)
col_ultima_acao = find_best_match(normalize_text(data_fim_key), df)
col_inicio = find_best_match(normalize_text(data_inicio_key), df)

# Verificar se ambas as colunas foram encontradas
if col_submissao and col_ultima_acao:
    # Encontrar o índice da coluna "DATA DA ULTIMA ACCAO"
    col_index = df.columns.get_loc(col_ultima_acao) + 1

    # Criar a nova coluna com 'SIM' ou 'NÃO'
    df.insert(col_index, col_sub, df[col_submissao].notna().map({True: "SIM", False: "NAO"}))

    # Inserir na posição correta em cols_targets
    index_pos = cols_targets.index(col_ultima_acao) + 1
    if col_sub not in cols_targets:
        cols_targets.insert(index_pos, col_sub)
else:
    print(f"Erro COLUNA DE SUBMISSÃO: As colunas '{data_sub_key}' ou '{data_fim_key}' não foram encontradas.")

#========================================================================================================#

#======================================== COLUNA TEMPO DE RESPOSTA ======================================#

# Verificar se as colunas foram encontradas
if col_inicio and col_ultima_acao:
    # Converter as colunas para datetime
    df[col_inicio] = pd.to_datetime(df[col_inicio], errors="coerce")
    df[col_ultima_acao] = pd.to_datetime(df[col_ultima_acao], errors="coerce")

    # Calcular a diferença entre as datas (em segundos)
    df["diferença segundos"] = (df[col_ultima_acao] - df[col_inicio]).dt.total_seconds()

    # Converter para o formato adequado (h:mm:ss ou mm:ss)
    df[col_temp] = df["diferença segundos"].apply(
        lambda x: f"{int(x // 3600):02}:{int((x % 3600) // 60):02}:{int(x % 60):02}" if pd.notna(x) and x >= 3600 else 
                  f"{int(x // 60):02}:{int(x % 60):02}" if pd.notna(x) else "00:00"
    )

    # Eliminar as linhas onde o TEMPO DE REALIZAÇÃO é "00:00"
    df = df[df[col_temp] != "00:00"]

    index = cols_targets.index(col_ultima_acao) + 1

    # Inserir a nova coluna col_temp na posição correta em cols_targets
    cols_targets.insert(index, col_temp)

else:
    print("Erro COLUNA TEMPO DE RESPOSTA: As colunas 'DATA DE INICIO' ou 'DATA DA ULTIMA ACCAO' não foram encontradas.")

#========================================================================================================#

#=========================================== COLUNAS DE CURSOS ==========================================#

# Dicionário para mapear as colunas
mapeamento_colunas = {}

# Identificar todas as colunas que contêm "Formação" ou "Curso" no nome
cols_formacao_curso = [col for col in df.columns if col and (any(normalize_text(palavra) in normalize_text(col) for palavra in palavras_chave_formacao))]

for col in cols_formacao_curso:
    col_normalizado = normalize_text(col)
    
    # Extrair o nome dentro de [ ]
    nome_encontrado = re.findall(r'\[(.*?)\]', col_normalizado)
    nome_final = " - ".join(nome_encontrado) if nome_encontrado else col_normalizado

    # Verificar se a coluna contém "comentário" no nome original
    if normalize_text(palavras_chave_comentario) in col_normalizado:
        nome_final = re.sub(r'(?i) - comentário', '', nome_final).strip()
        nome_final = f"{descricao_comentario}: {nome_final}"
        cols_comentarios_fc.append(col)

    # Verificar se a coluna contém "tempo do grupo"
    elif normalize_text(palavras_chave_tempo_grupo) in nome_final:
        nome_final = re.sub(r'(?i)\btempo do grupo:\s*', '', nome_final).strip()
        nome_final = f"{descricao_tempo_grupo}: {nome_final}"

    # Verificar se a coluna contém "áreas temáticas" no nome
    elif normalize_text(palavras_chave_areas_tematicas) in col_normalizado:  
        nome_final = f"{descricao_interesse}: {nome_final}"
        cols_interesses_fc.append(col)

    # Verificar se a coluna contém "contínua"
    elif normalize_text(palavras_chave_continua) in col_normalizado:
        nome_final = f"{descricao_continua}: {nome_final}"
        cols_continua_fc.append(col)

    # Verificar se a coluna contém "preferência"
    elif normalize_text(palavras_chave_preferencia) in col_normalizado:
        nome_final = f"{descricao_preferencia}: {nome_final}"

    else:
        nome_final = f"{descricao_formacao_curso}: {nome_final}"
        cols_sumformados.append(col)

    # Guardar no dicionário de mapeamento
    mapeamento_colunas[col] = nome_final

# Adicionar os nomes originais das colunas a cols_targets (mantendo os nomes no df)
if len(mapeamento_colunas) > 0:
    cols_targets += list(mapeamento_colunas.keys())

#========================================================================================================#

#====================================== PREENCHER CELULAS INVÁLIDAS =====================================#

# Preencher os NaN com 0 para as colunas de tipo inteiro e float
df[df.select_dtypes(include=['int64', 'float64']).columns] = df.select_dtypes(include=['int64', 'float64']).fillna(0)

# Filtrar apenas colunas de texto
colunas_texto = df.select_dtypes(include=['object', 'string']).columns

# Identificar colunas de Sim/Não (poucos valores únicos, ≤ 3)
colunas_sim_nao = [col for col in colunas_texto if df[col].nunique(dropna=True) <= 3]

# Aplicar preenchimento
for col in colunas_texto:
    if col in colunas_sim_nao:
        df[col] = df[col].fillna("NAO")  # Para colunas Sim/Não
    else:
        df[col] = df[col].apply(lambda x: "VAZIO" if pd.isna(x) or isinstance(x, (int, float)) else x)  # Outras colunas

# Preencher NaN apenas nas colunas que não são datetime
df[df.select_dtypes(exclude=['datetime64']).columns] = df[df.select_dtypes(exclude=['datetime64']).columns].fillna(VALOR_NAO)

#========================================================================================================#

#========================================== COLUNA Nº FORMANDOS =========================================#

# Criar uma nova coluna com a soma apenas dos valores inteiros
df[col_nformandos] = df[cols_sumformados].apply(
    lambda row: sum(x for x in pd.to_numeric(row, errors='coerce').fillna(0) if x.is_integer()), 
    axis=1
)

# Encontrar a última coluna numérica em cols_sumformados
ultima_coluna_numerica = None
for coluna in cols_sumformados:
    if pd.api.types.is_numeric_dtype(df[coluna]):  
        ultima_coluna_numerica = coluna
        #print(coluna)

# Inserir o nome da nova coluna após a última coluna numérica
if ultima_coluna_numerica:
    index = cols_targets.index(ultima_coluna_numerica) + 1  
    cols_targets.insert(index, col_nformandos)
    #print(index)


#========================================================================================================#

#=========================================== GUARDAR DATAFRAME ==========================================#

# Verificar quais colunas existem no DataFrame
cols_existentes = [col for col in cols_targets if col in df.columns]
cols_faltantes = set(cols_targets) - set(cols_existentes)

# Avisar sobre colunas em falta
if cols_faltantes:
    print(f"Aviso: As seguintes colunas não estão no DataFrame e serão ignoradas: {cols_faltantes}")

# Selecionar apenas as colunas que existem
if cols_existentes:
    df = df[cols_existentes]
else:
    print("Erro: Nenhuma das colunas alvo foi encontrada no DataFrame. O ETL pode falhar.")

# Aplicar o rename para substituir os nomes no DataFrame sem quebrar a estrutura
df.rename(columns=mapeamento_colunas, inplace=True)

# Guardar
df.to_excel(file_path_out, index=False)

# Abrir o ficheiro com openpyxl para modificar
wb = load_workbook(file_path_out)  
ws = wb.active  

# Alterar o título da aba ativa
ws.title = ws_title

# Ajustar o tamanho das colunas com base no tamanho dos cabeçalhos
for col in ws.columns:
    max_length = 0
    col_letter = col[0].column_letter 

    # Calcular a maior largura da coluna em uma única iteração
    for cell in col:
        if cell.value:
            max_length = max(max_length, len(str(cell.value)))

    # Ajustar a largura da coluna
    ws.column_dimensions[col_letter].width = max_length + 2  

# Ajustar a altura da linha de cabeçalho (primeira linha)
ws.row_dimensions[1].height = 70  

# Centralizar o conteúdo de todas as células
for row in ws.iter_rows():
    for cell in row:
        cell.alignment = Alignment(horizontal="center", vertical="center")  

# Salvar o ficheiro após as modificações
wb.save(file_path_out)

#dfi.export(df.head(50), "tabela-2023.png")  

# Abrir o ficheiro salvo
if os.name == 'nt':  # Para sistemas Windows
    os.startfile(file_path_out)
elif os.name == 'posix':  # Para sistemas Unix (Linux/MacOS)
    if 'darwin' in subprocess.os.uname().sysname.lower():  # MacOS
        subprocess.call(['open', file_path_out])
    else:  # Linux
        subprocess.call(['xdg-open', file_path_out])

#================================================= FIM ==================================================#
