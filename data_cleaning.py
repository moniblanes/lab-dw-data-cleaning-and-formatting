# data_cleaning.py
import pandas as pd
import numpy as np


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )

    # OJO: tu .str.replace('st','state') puede afectar a palabras que contienen "st"
    # Mejor: reemplazar solo si la columna es exactamente "st"
    df = df.rename(columns={"st": "state"})

    # corrección typo específico que viste
    df.columns = df.columns.str.replace("custateomer", "customer", regex=False)

    return df


def clean_gender(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # convertimos a string para poder usar .str aunque haya NaN
    s = df["gender"].astype("string")

    # normalizaciones
    s = (
        s.str.replace("Male", "M", regex=False)
         .str.replace("Femal", "F", regex=False)
         .str.replace("female", "F", regex=False)
    )
    df["gender"] = s
    return df


def clean_state(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    s = df["state"].astype("string")

    s = (
        s.str.replace("AZ", "Arizona", regex=False)
         .str.replace("Cali", "California", regex=False)
         .str.replace("WA", "Washington", regex=False)
         .str.replace("Californiafornia", "California", regex=False)
    )
    df["state"] = s
    return df


def clean_education(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    s = df["education"].astype("string")
    df["education"] = s.str.replace("Bachelors", "Bachelor", regex=False)
    return df


def clean_customer_lifetime_value(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Ejemplo: "12.34%" -> 0.1234
    s = df["customer_lifetime_value"].astype("string")
    s = s.str.replace("%", "", regex=False)

    # to_numeric por si hay algún valor raro
    clv = pd.to_numeric(s, errors="coerce") / 100
    df["customer_lifetime_value"] = clv

    return df


def format_number_of_open_complaints(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    s = df["number_of_open_complaints"].astype("string")

    # En tu enfoque: "1/5/00" -> "5" y "1/0/00" -> "0"
    # Esto es una versión más robusta: coge el número central si hay "/"
    # y si no, intenta convertir directamente.
    def extract_middle(val: str):
        if pd.isna(val):
            return np.nan
        val = str(val)
        if "/" in val:
            parts = val.split("/")
            if len(parts) >= 2:
                return parts[1]
        return val

    extracted = s.map(extract_middle)
    df["number_of_open_complaints"] = pd.to_numeric(extracted, errors="coerce")

    return df


def drop_first_row_slice(df: pd.DataFrame) -> pd.DataFrame:
    """
    En tu notebook hiciste: df = df.iloc[1:1070]
    Mantengo la misma lógica para que te salga igual el resultado.
    """
    df = df.copy()
    return df.iloc[1:1070].copy()


def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # gender: NaN -> 'U'
    df["gender"] = df["gender"].fillna("U")

    # customer_lifetime_value: NaN -> media
    mean_clv = df["customer_lifetime_value"].mean()
    df["customer_lifetime_value"] = df["customer_lifetime_value"].fillna(mean_clv)

    return df


def drop_duplicates_reset_index(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    return df


def main(df: pd.DataFrame) -> pd.DataFrame:
    """
    Orquesta todo el cleaning/formatting en el orden lógico.
    """
    df = standardize_columns(df)
    df = clean_gender(df)
    df = clean_state(df)
    df = clean_education(df)
    df = clean_customer_lifetime_value(df)
    df = format_number_of_open_complaints(df)

    # tu recorte
    df = drop_first_row_slice(df)

    # nulos + duplicados
    df = handle_nulls(df)
    df = drop_duplicates_reset_index(df)

    return df