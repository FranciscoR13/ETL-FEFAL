import pandas as pd
import numpy as np
import re
from unidecode import unidecode
import pandas as pd
import numpy as np

prefixes = [
    r"^\s*(municipio|município|camara municipal|cm|c m)(\s+(de|do|da|dos|das))?\s+",
    r"^\s*(freguesia|junta de freguesia|uniao de freguesias|uniao das freguesias)(\s+(de|do|da|dos|das))?\s+"
]

def normalize_text(text):
    if not isinstance(text, str):
        return ""
    text = text.strip()
    text = unidecode(text)
    text = re.sub(r"\s+", " ", text)
    return text.lower()
def create_map(list, old_key, new_key):
    return {
        normalize_text(i[old_key]): i[new_key]
        for i in list
        if old_key in i and new_key in i
    }
def rename_cols(df, map, strict=False):
    renamed_cols = {}
    for col in df.columns:
        col_norm = normalize_text(col)
        if col_norm in map:
            renamed_cols[col] = map[col_norm]
    df_renomeado = df.rename(columns=renamed_cols)
    if strict:
        df_renomeado = df_renomeado[list(renamed_cols.values())]

    return df_renomeado
def remove_prefixes(text, prefixes):
    text = normalize_text(text)
    for prefix in prefixes:
        text = re.sub(prefix, "", text)
    return text.strip()
def query_to_df(cur, sql: str) -> pd.DataFrame:
    cur.execute(sql)
    data = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    return pd.DataFrame(data, columns=column_names)

def run_etl(year: int, df: pd.DataFrame, mongo_db, cur_sii) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, pd.DataFrame]:
    configs = load_mongo_configs(mongo_db, year)
    group_dfs = split_column_groups(df, configs["groups"])
    group_dfs = normalize_column_names(group_dfs)

    # Process identification
    df_id = group_dfs["identificacao"]
    df_id.columns = [normalize_text(col) for col in df_id.columns]
    df_id = rename_cols(df_id, configs["map_ren_col"], True)
    df_id = df_id[~df_id["nome_entidade"].apply(normalize_text).isin(["", "nd", "nan", "n/a", "na", "não definido", "sem dados", None])]

    if "tipo_entidade" in df_id:
        df_id["tipo_entidade"] = df_id["tipo_entidade"].apply(lambda x: configs["map_ent"].get(normalize_text(x), x))
    else:
        df_id["tipo_entidade"] = "Municípios"

    df_id["nome_entidade_norm"] = df_id["nome_entidade"].apply(lambda x: remove_prefixes(normalize_text(x), prefixes))

    df_sii = query_to_df(cur_sii, "SELECT id_entidades, ent_nome, ent_tipo FROM entidades")
    df_sii["ent_nome"] = df_sii["ent_nome"].apply(lambda x: remove_prefixes(x, prefixes))
    df_sii["ent_tipo"] = df_sii["ent_tipo"].apply(normalize_text)

    df_id["entity_key"] = df_id["nome_entidade_norm"] + "||" + df_id["tipo_entidade"].apply(normalize_text)
    df_sii["entity_key"] = df_sii["ent_nome"] + "||" + df_sii["ent_tipo"]
    map_entity = dict(zip(df_sii["entity_key"], df_sii["id_entidades"]))
    df_id["id_entidade"] = df_id["entity_key"].map(map_entity)
    group_dfs["identificacao"] = df_id

    group_dfs = process_completion_percentage(group_dfs)
    group_dfs = initialize_time_fields(group_dfs)
    group_dfs = process_additional_fields(group_dfs, year)
    group_dfs = process_formations(group_dfs)
    group_dfs = process_interests(group_dfs)
    group_dfs = process_availability(group_dfs)
    group_dfs = validate_preferences(group_dfs)

    full_data = pd.concat(group_dfs.values(), axis=1).reset_index(drop=True)
    df_id_full = group_dfs["identificacao"].reset_index(drop=True)

    valid_id_mask = df_id_full["id_entidade"].notna()
    duplicate_mask_partial = df_id_full[valid_id_mask].duplicated(subset="id_entidade", keep="first")
    duplicate_mask = pd.Series(False, index=df_id_full.index)
    duplicate_mask[duplicate_mask_partial.index] = duplicate_mask_partial

    unmatched_mask = df_id_full["id_entidade"].isna()

    duplicate_df = full_data.iloc[duplicate_mask[duplicate_mask].index].reset_index(drop=True)
    unmatched_df = full_data.iloc[unmatched_mask[unmatched_mask].index].reset_index(drop=True)

    cols_to_remove = ["nome_entidade_norm", "entity_key", "data_inicio", "data_fim", "__pct", "__tempo"]
    duplicate_df.drop(columns=[col for col in cols_to_remove if col in duplicate_df.columns], inplace=True, errors="ignore")
    unmatched_df.drop(columns=[col for col in cols_to_remove if col in unmatched_df.columns], inplace=True, errors="ignore")

    valid_idxs = df_id_full[~(duplicate_mask | unmatched_mask)].index
    for group in group_dfs:
        group_dfs[group] = group_dfs[group].reset_index(drop=True).loc[valid_idxs].reset_index(drop=True)

    group_dfs["identificacao"].drop(columns=cols_to_remove, inplace=True, errors="ignore")

    return group_dfs, duplicate_df, unmatched_df


def load_mongo_configs(mongo_db, year: int) -> dict:
    ren_col = list(mongo_db["ConfigRenCol"].find({}, {"_id": 0}))
    col_map = mongo_db["ConfigColMap"].find_one({"year": year})
    ent_map = list(mongo_db["ConfigMapEnt"].find({}, {"_id": 0}))
    return {
        "map_ren_col": create_map(ren_col, "original_name", "new_name"),
        "map_ent": create_map(ent_map, "tipo_entidade_inq", "tipo_entidade_norm"),
        "groups": col_map["groups"]
    }

def split_column_groups(df: pd.DataFrame, groups: dict) -> dict[str, pd.DataFrame]:
    return {
        name: df.iloc[:, lims["start"] - 1:lims["end"]]
        for name, lims in groups.items()
    }

def normalize_column_names(group_dfs):
    for name, df in group_dfs.items():
        df.columns = [normalize_text(col).strip() for col in df.columns]
    return group_dfs

def process_identification(group_dfs, configs, cur_sii):
    df_id = group_dfs["identificacao"]
    df_id.columns = [normalize_text(col) for col in df_id.columns]
    df_id = rename_cols(df_id, configs["map_ren_col"], True)
    if "nome_entidade" not in df_id:
        return {}, pd.DataFrame()
    df_id = df_id[~df_id["nome_entidade"].apply(normalize_text).isin(["", "nd", "nan", "n/a", "na", "não definido", "sem dados", None])]
    if "tipo_entidade" in df_id:
        df_id["tipo_entidade"] = df_id["tipo_entidade"].apply(lambda x: configs["map_ent"].get(normalize_text(x), x))
    else:
        df_id["tipo_entidade"] = "Municípios"
    df_id["nome_entidade_norm"] = df_id["nome_entidade"].apply(lambda x: remove_prefixes(normalize_text(x), prefixes))
    df_sii = query_to_df(cur_sii, "SELECT id_entidades, ent_nome, ent_tipo FROM entidades")
    df_sii["ent_nome"] = df_sii["ent_nome"].apply(lambda x: remove_prefixes(x, prefixes))
    df_sii["ent_tipo"] = df_sii["ent_tipo"].apply(normalize_text)
    df_id["entity_key"] = df_id["nome_entidade_norm"] + "||" + df_id["tipo_entidade"].apply(normalize_text)
    df_sii["entity_key"] = df_sii["ent_nome"] + "||" + df_sii["ent_tipo"]
    map_entity = dict(zip(df_sii["entity_key"], df_sii["id_entidades"]))
    df_id["id_entidade"] = df_id["entity_key"].map(map_entity)
    valid_idxs = df_id[df_id["id_entidade"].notna()].index
    invalid_idxs = df_id[df_id["id_entidade"].isna()].index
    unmatched_df = df_id.loc[invalid_idxs].copy()
    for group in group_dfs:
        group_dfs[group] = group_dfs[group].loc[valid_idxs].reset_index(drop=True)
    group_dfs["identificacao"] = df_id.loc[valid_idxs].reset_index(drop=True)
    return group_dfs, unmatched_df

def process_completion_percentage(group_dfs):
    df = group_dfs["identificacao"]
    if "percentagem_preenchido" in df.columns:
        df["percentagem_preenchido"] = pd.to_numeric(df["percentagem_preenchido"], errors="coerce")
        df["percentagem_preenchido"] = df["percentagem_preenchido"].apply(
            lambda x: x if pd.notna(x) and x >= 0 else np.nan
        )
        max_pct = df["percentagem_preenchido"].max()
        if pd.notna(max_pct) and max_pct > 0:
            df["percentagem_preenchido"] = (df["percentagem_preenchido"] / max_pct * 100).round().astype("Int64")
    else:
        df["percentagem_preenchido"] = pd.NA
    group_dfs["identificacao"] = df
    return group_dfs


def initialize_time_fields(group_dfs):
    df = group_dfs["identificacao"]
    if "data_inicio" in df.columns and "data_fim" in df.columns:
        df["data_inicio"] = pd.to_datetime(df["data_inicio"], errors="coerce")
        df["data_fim"] = pd.to_datetime(df["data_fim"], errors="coerce")
        valid_mask = df["data_inicio"].notna() & df["data_fim"].notna()
        df["tempo_realizacao"] = pd.NA
        df.loc[valid_mask, "tempo_realizacao"] = (df.loc[valid_mask, "data_fim"] - df.loc[valid_mask, "data_inicio"]).dt.total_seconds()
        df["tempo_realizacao"] = df["tempo_realizacao"].apply(lambda x: x if pd.notna(x) and x > 0 else pd.NA).astype("Int64")
    elif "tempo_realizacao" not in df.columns:
        df["tempo_realizacao"] = pd.Series(dtype="Int64")
    group_dfs["identificacao"] = df
    return group_dfs

def remove_entity_duplicates(group_dfs):
    df = group_dfs["identificacao"].copy()
    df["__pct"] = df["percentagem_preenchido"].fillna(-1)
    df["__tempo"] = df["tempo_realizacao"].fillna(-1)
    df.sort_values(by=["id_entidade", "__pct", "__tempo"], ascending=[True, False, False], inplace=True)
    dup_mask = df.duplicated(subset="id_entidade", keep="first")
    valid_idxs = df[~dup_mask].index
    duplicate_df = df[dup_mask].drop(columns=["__pct", "__tempo"]).reset_index(drop=True)
    for group in group_dfs:
        group_dfs[group] = group_dfs[group].loc[valid_idxs].reset_index(drop=True)
    return group_dfs, duplicate_df

def process_additional_fields(group_dfs, year):
    df = group_dfs["identificacao"]
    df["ano"] = year
    if "nome_responsavel" not in df.columns:
        df["nome_responsavel"] = pd.NA
    if "data_submissao" in df.columns and "data_fim" in df.columns:
        df["data_submissao"] = pd.to_datetime(df["data_submissao"], errors="coerce")
        df["data_fim"] = pd.to_datetime(df["data_fim"], errors="coerce")
        df["data_submissao"] = df["data_submissao"].fillna(df["data_fim"])
    else:
        df["data_submissao"] = pd.NaT
    # group_dfs["identificacao"] = df[["id_entidade", "ano", "data_submissao", "existe_responsavel", "nome_responsavel", "percentagem_preenchido", "tempo_realizacao"]]
    return group_dfs

def process_formations(group_dfs):
    if "formacoes" not in group_dfs or group_dfs["formacoes"].empty:
        return group_dfs
    
    def clean_column_names(text, invalids):
        if pd.isna(text):
            return ""
        for inval in invalids:
            if inval:
                text = str(text).replace(str(inval), "")
        return text.strip()
    def validate_numeric(v):
        try:
            num = int(v)
            return num if num >= 0 else 0
        except:
            return 0
    group_dfs["formacoes"].columns = [clean_column_names(col, prefixes) for col in group_dfs["formacoes"].columns]
    for col in group_dfs["formacoes"].columns:
        group_dfs["formacoes"][col] = group_dfs["formacoes"][col].apply(validate_numeric)
    return group_dfs

def process_interests(group_dfs):
    if "interesses" not in group_dfs or group_dfs["interesses"].empty:
        return group_dfs
    for col in group_dfs["interesses"].columns:
        col_normalized = normalize_text(col)
        if "comentario" not in col_normalized:
            group_dfs["interesses"][col] = group_dfs["interesses"][col].apply(
                lambda v: 1 if (nv := normalize_text(str(v))) == "sim"
                else 0 if nv == "nao" else None
            )
    return group_dfs

def process_availability(group_dfs):
    if "disponibilidade" not in group_dfs or group_dfs["disponibilidade"].empty:
        return group_dfs
    for col in group_dfs["disponibilidade"].columns:
        group_dfs["disponibilidade"][col] = group_dfs["disponibilidade"][col].apply(
            lambda v: 1 if (nv := normalize_text(str(v))) == "sim"
            else 0 if nv == "nao" else None
        )
    return group_dfs

def validate_preferences(group_dfs):
    if "tipo de ensino" not in group_dfs or group_dfs["tipo de ensino"].empty:
        return group_dfs
    for col in group_dfs["tipo de ensino"].columns:
        group_dfs["tipo de ensino"][col] = pd.to_numeric(group_dfs["tipo de ensino"][col], errors='coerce').astype("Int64")
    return group_dfs




