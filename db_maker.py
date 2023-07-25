import csv
import sqlite3
import numpy as np

def many_tables(db_path: str, j_cut: float) -> None:
    """
    Create multiple tables in a SQLite database from CSV files.

    Args:
        db_path (str): The path to the SQLite database file.
        j_cut (float): The cutoff value for the 'j_m' column.

    Returns:
        None
    """

    arr_small = np.arange(-90,90.01, 0.01)
    arr_big = np.arange(-90,91, 1)

    arr_small_round = []
    for i in arr_small:
        arr_small_round.append(round(i,2))

    files = {}
    for i in range(len(arr_big) - 1):
        # filter arr_small
        arr_filt = arr_small[(arr_small_round >= arr_big[i]) & (arr_small_round <= arr_big[i+1])]

        files[f"{arr_big[i]}_{arr_big[i+1]}"] = []

        for j in range(len(arr_filt) - 1):
            filename = f"./gaia_archive_bulk_query/csv/gaia_tmass_{arr_filt[j]:.2f}_{arr_filt[j+1]:.2f}.csv"
            files[f"{arr_big[i]}_{arr_big[i+1]}"].append(filename)


    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    columns = ['ra', 'dec', 'pmra', 'pmdec', 'phot_g_mean_mag', 'j_m']

    column_names = ', '.join(columns)
    for k, filenames in files.items():
        # Create table
        com = f"CREATE TABLE IF NOT EXISTS '{k}' (ra REAL, dec REAL, pmra REAL, pmdec REAL, phot_g_mean_mag REAL, j_m REAL)"
        c.execute(com)

        # Read CSVs and store columns
        counter = 0
        for filename in filenames:
            counter += 1

            if counter % 100 == 0:
                print(f"Read {counter} files")

            if filename.endswith('.csv'):
                with open(filename, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            if float(row['j_m']) < j_cut:
                                values = [row[column] for column in columns]
                                c.execute(f"INSERT INTO '{k}' ({column_names}) VALUES ({', '.join(['?']*len(columns))})", values)
                        except Exception as e:
                            print(f"Error with {filename} {row['designation']}: {e}")

    conn.commit()
    conn.close()

if __name__ == '__main__':
    many_tables('./db/gaia_tmass_16_jm_cut.db', 16)