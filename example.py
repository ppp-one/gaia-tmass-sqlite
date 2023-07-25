import sqlite3
import numpy as np
from datetime import datetime
import pandas as pd

def db_query(db: str, min_dec: float, max_dec: float, min_ra: float, max_ra: float) -> pd.DataFrame:
    """
    Queries a federated database for astronomical data within a specified range of declination and right ascension.

    Args:
        db (str): The path to the SQLite database file.
        min_dec (float): The minimum declination value to query.
        max_dec (float): The maximum declination value to query.
        min_ra (float): The minimum right ascension value to query.
        max_ra (float): The maximum right ascension value to query.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the queried astronomical data.
    """

    conn = sqlite3.connect(db)

    # Determine the relevant shard(s) based on the query parameters.
    arr = np.arange(np.floor(min_dec), np.ceil(max_dec) + 1, 1)
    relevant_shard_ids = set()
    for i in range(len(arr) - 1):
        shard_id = f"{arr[i]:.0f}_{arr[i+1]:.0f}"
        relevant_shard_ids.add(shard_id)

    # Execute the federated query across the relevant shard(s).
    df_total = pd.DataFrame()
    for shard_id in relevant_shard_ids:
        shard_table_name = f'{shard_id}'
        q = f'SELECT * FROM `{shard_table_name}` WHERE dec BETWEEN {min_dec} AND {max_dec} AND ra BETWEEN {min_ra} AND {max_ra}'
        df = pd.read_sql_query(q, conn)
        df_total = pd.concat([df, df_total], axis=0)

    # Close the conn and return the results.
    conn.close()
    return df_total.sort_values(by=['j_m']).reset_index(drop=True)


if __name__ == '__main__':

    min_dec = -50.2
    max_dec = -49.2
    min_ra = 23.3
    max_ra = 24.4

    t0 = datetime.now()
    df = db_query('./db/gaia_tmass_16_jm_cut.db', min_dec, max_dec, min_ra, max_ra)
    print("Executed query", datetime.now() - t0)
    print(df)