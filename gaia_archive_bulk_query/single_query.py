from astroquery.gaia import Gaia
import os.path

def single_query(dec0: float, dec1: float) -> None:
    """
    Downloads Gaia and 2MASS data for a given range of declinations and saves it to a CSV file.

    Args:
        dec0 (float): The lower bound of the declination range to query.
        dec1 (float): The upper bound of the declination range to query.

    Returns:
        None: The function does not return anything, but saves the query results to a CSV file.
    """

    try:
        filename = f"./csv/gaia_tmass_{dec0:.2f}_{dec1:.2f}.csv"
        if os.path.exists(filename):
            pass
        else:
            print(f"Writing {filename}")
            limit = 100000000000
            job = Gaia.launch_job(
                        f"""
                        SELECT TOP {limit} gaia.designation, tmass.designation, gaia.ra, gaia.dec, gaia.pmra, gaia.pmdec, phot_g_mean_mag, phot_bp_mean_mag, phot_rp_mean_mag, tmass.j_m, tmass.h_m, tmass.ks_m
                        FROM gaiadr2.gaia_source AS gaia
                        INNER JOIN gaiadr2.tmass_best_neighbour AS tmass_match ON tmass_match.source_id = gaia.source_id
                        INNER JOIN gaiadr1.tmass_original_valid AS tmass ON tmass.tmass_oid = tmass_match.tmass_oid
                        WHERE gaia.dec BETWEEN {dec0} AND {dec1}
                        """
                    )
            r = job.get_results()
            r.write(filename, overwrite=True)
    except Exception as e:
        print(f"Error with {dec0:.2f} to {dec1:.2f}: {str(e)}")
