from pandas import DataFrame, read_stata
from requests import HTTPError, Timeout, JSONDecodeError
from numpy import ceil
import requests

from multiprocessing import Pool, cpu_count
from typing import Dict, List
from time import sleep
import warnings
from json import dump, dumps
import traceback


def read_file(file_path: str, num_lines: int) -> List[str]:
    """
    Read n lines of a text file.
    """
    with open(file_path, "r") as file:
        lines = []
        for i, line in enumerate(file):
            if i == num_lines:
                break
            lines.append(line.strip())
        return lines


def write_file(file_path: str, lines: List[str]) -> None:
    """
    Write n lines of a text file.
    """
    with open(file_path, "w") as file:
        for line in lines:
            file.write(f"{line}\n")


def read_file_test(file_path: str, num_lines: int) -> List[str]:
    """
    Test .readlines() method for parsing .txt.
    """
    with open(file_path, "r") as file:
        lines = file.readlines()[:num_lines]
    return lines


def convert_stata_to_csv(file_path: str) -> None:
    """
    Read Stata .dta format and write to .csv.
    """
    df = read_stata(file_path)
    df.to_csv(file_path.replace(".dta", ".csv"), index=False)
    print("Data saved.\n")


def parse_line(args) -> Dict[str, str]:
    """
    Parse line of .txt file according to character indices.
    This lower-level function is called in parse_data().
    """
    line, columns = args
    result = {name: line[start - 1 : end].strip() for (name, start, end) in columns}
    return result


def parse_data(
    read_file_path: str, write_file_path_csv: str, write_file_path_stata: str
) -> None:
    """
    Parses D&B text file according to columns object,
    which is a list of tuples where first element is the name of the column,
    the second element is the starting line index and last is the ending line index.

    This function uses all available cores and each worker in the pool processes a batch of lines.
    """
    columns = [
        ("DUNS", 1, 9),
        ("DCOMP", 10, 39),
        ("DTRADE", 40, 69),
        ("DSTREET", 70, 94),
        ("DCITY", 95, 114),
        ("DSTATEAB", 115, 116),
        ("DZIP5", 117, 121),
        ("DZIP4EXT", 122, 125),
        ("DMAILADD", 126, 150),
        ("DMAILCIT", 151, 170),
        ("DMAILSTA", 171, 172),
        ("DMAILZIP", 173, 177),
        ("DMAILZP4", 178, 181),
        ("DCARRRTC", 182, 185),
        ("FILLER0", 186, 187),
        ("DNATLCOD", 188, 190),
        ("DSTATECO", 191, 192),
        ("DCOUNTYC", 193, 195),
        ("DCITYCOD", 196, 199),
        ("DSMSACOD", 200, 202),
        ("DTELEPHO", 203, 212),
        ("DCEONAME", 213, 242),
        ("DCEOTITT", 243, 272),
        ("DSALESVO", 273, 287),
        ("DSLSVOLC", 288, 288),
        ("DEMTLTOT", 289, 297),
        ("DEMTOTC", 298, 298),
        ("DEMTLHER", 299, 307),
        ("DEMPHRCDC", 308, 308),
        ("DYRSTART", 309, 312),
        ("DSTATUSI", 313, 313),
        ("DSUBSIDI", 314, 314),
        ("DMANUFIN", 315, 315),
        ("DULTDUN", 316, 324),
        ("DHDQDUN", 325, 333),
        ("DPARDUN", 334, 342),
        ("DPRHQCT", 343, 362),
        ("DPRHQST", 363, 364),
        ("FILLER1", 365, 372),
        ("FILLER2", 373, 382),
        ("DHIER", 383, 384),
        ("DDIAS", 385, 393),
        ("DPOPLCD", 394, 394),
        ("DTRANCD", 395, 395),
        ("DRPTDAT", 396, 401),
        ("FILLER3", 402, 420),
        ("DRCRDCL", 421, 421),
        ("DLINEBU", 422, 440),
        ("DPRIMSI", 441, 444),
        ("DSICEXT1", 445, 448),
        ("DSICEXT2", 449, 452),
        ("DSICEXT3", 453, 456),
        ("DSICEXT4", 457, 460),
        ("DSIC2", 461, 480),
        ("DSIC3", 481, 500),
        ("DSIC4", 501, 520),
        ("DSIC5", 521, 540),
        ("DSIC6", 541, 560),
    ]

    with open(read_file_path, "r") as file, Pool(cpu_count()) as pool:
        lines = [line.rstrip("\n") for line in file.readlines()]
        results = pool.map(parse_line, [(line, columns) for line in lines])

    print("Data parsed.\n")

    df = DataFrame(results)

    df.to_csv(write_file_path_csv, index=False)
    df.to_stata(write_file_path_stata, write_index=False)

    print("Data saved.\n")


def fill_leading_zeros(df: DataFrame) -> DataFrame:
    """
    Fills leading zeros for D&B ID, ZIP code, and SIC code.
    This is necessary since leading zeros are dropped once the data is imported,
    since the columns are treated as integers, which can't have leading zeros.
    """
    column_to_length = {
        "DUNS": 9,
        "DZIP5": 5,
        # "DZIP4EXT": 4,
        # "DMAILZIP": 5,
        # "DMAILZP4": 4,
        # "DNATLCOD": 3,
        # "DSTATECO": 2,
        # "DCOUNTYC": 3,
        # "DCITYCOD": 4,
        # "DSMSACOD": 3,
        # "DTELEPHO": 10,
        # "DULTDUN": 9,
        # "DHDQDUN": 9,
        # "DPARDUN": 9,
        # "DHIER": 2,
        # "DDIAS": 9,
        # "DPOPLCD": 1,
        # "DTRANCD": 1,
        # "DRPTDAT": 6,
        "DPRIMSI": 4,
        # "DSICEXT1": 4,
        # "DSICEXT2": 4,
        # "DSICEXT3": 4,
        # "DSICEXT4": 4,
        # "DSIC2": 20,
        # "DSIC3": 20,
        # "DSIC4": 20,
        # "DSIC5": 20,
        # "DSIC6": 20,
    }
    with warnings.catch_warnings():  # NB: Could not cast to string type from list of dicts. Suppressing warnings instead.
        warnings.simplefilter("ignore")
        for column, length in column_to_length.items():
            df.loc[:, column] = df.loc[:, column].astype(str)
            mask = (
                (df.loc[:, column].notna())
                & (df.loc[:, column] != "nan")
                & (df.loc[:, column] != 0)
            )
            df.loc[mask, column] = df.loc[mask, column].str.zfill(length)

    print("Filled leading zeros.\n")

    return df


def fill_leading_zeros_zip(df: DataFrame) -> DataFrame:
    """
    Fills leading zeros for ZIP code, and FIPS code.
    This is necessary since leading zeros are dropped once the data is imported,
    since the columns are treated as integers, which can't have leading zeros.
    """
    column_to_length = {
        "zipcode": 5,
        "statefips": 2,
        "countyfips": 3,
    }
    with warnings.catch_warnings():  # NB: Could not cast to string type from list of dicts. Suppressing warnings instead.
        warnings.simplefilter("ignore")
        for column, length in column_to_length.items():
            df.loc[:, column] = df.loc[:, column].fillna(0).astype(int).astype(str)
            mask = (df.loc[:, column].notna()) & (df.loc[:, column] != "nan")
            df.loc[mask, column] = df.loc[mask, column].str.zfill(length)

    print("Filled leading zeros.\n")

    return df


def zip_combine_state_and_county(df: DataFrame) -> DataFrame:
    """
    Combine state and county codes into 5 digit FIPS code for ZIP data.
    Rename zipcode to DZIP5 for matching.
    """
    df["FIPS"] = df["statefips"] + df["countyfips"]
    return df.drop(columns=["statefips", "countyfips"]).rename(
        columns={"zipcode": "DZIP5"}
    )


def keep_required_columns(df: DataFrame) -> DataFrame:
    """
    Keep only required columns from D&B dataset.
    """
    columns_to_keep = [
        "DUNS",
        "DCOMP",
        "DSTREET",
        "DCITY",
        "DSTATEAB",
        "DZIP5",
        "DSALESVO",
        "DEMTLHER",
        "DPRIMSI",
    ]
    print("Kept required columns.\n")
    return df[columns_to_keep]


def df_to_dict(df: DataFrame) -> List[Dict[str, str]]:
    """
    Convert DataFrame to dictionary to be used as input for geocoding.
    """
    print("Converted processed DataFrame to dictionary for geocoding.\n")
    return df.to_dict(orient="records")


def get_data(url: str) -> List[Dict[str, str]]:
    """
    Send HTTP GET request.
    This lower-level function is called by geocode_data().
    """
    for _ in range(10):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except (HTTPError, Timeout, JSONDecodeError) as e:  # Catch JSONDecodeError
            print(f"Attempt failed with exception: {str(e)}. Retrying...\n")
            sleep(1)  # wait for 1 second before trying again
    return {"error": "Unable to fetch data after 10 attempts."}


def geocode_data(data: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    """
    Send GET requests to US Census Geocoder for each street address, city, state abbreviation, and ZIP code in D&B dataset.
    This lower-level function is called in multi_geocode_data().
    -------------------------------------
    API Parameters:
    benchmark=Public_AR_Census2020
    vintage=Census2020_Census2020
    layers=82
    format=json
    """
    results = {}
    for record in data:
        if record["DSTREET"] is not None:
            url = f"https://geocoding.geo.census.gov/geocoder/geographies/address?street={record['DSTREET']}&city={record['DCITY']}&state={record['DSTATEAB']}&zip={record['DZIP5']}&benchmark=Public_AR_Census2020&vintage=Census2020_Census2020&layers=82&format=json"
            response = get_data(url)
            results[record["DUNS"]] = response
    return results


def get_chunks(data: List[Dict[str, str]], n):
    """
    Generator function which yields successive n-sized chunks from list of dicts.
    Called in multi_geocode_data() to generate inner lists of chunks to feed each worker.
    """
    for i in range(0, len(data), n):
        yield data[i : i + n]


def multi_geocode_data(data: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    """
    Initilizes pool of workers which each send a batch of requests to geocoding endpoint.
    Uses all available cores, but not at a 100% processing power.
    --------------------------------
    TODO: In future adding multithreading may significantly speed up processes,
    since each core only uses from around 3 to 10% at any given time.
    """
    num_processes = cpu_count()
    data_chunks = list(get_chunks(data, int(ceil(len(data) / num_processes))))

    with Pool(num_processes) as pool:
        results = pool.map(geocode_data, data_chunks)

    results = {k: v for sublist in results for k, v in sublist.items()}

    print(f"Geocoding complete for {len(results)} observations.\n")

    return results


def save_geocoded_responses(
    responses: Dict[str, Dict[str, str]], file_json: str, file_txt: str
) -> None:
    """
    Write geocoded responses to JSON file unless raises error,
    since some of the JSON responses may be malformed, then save to .txt file.
    """
    try:
        with open(file_json, "w") as file:
            dump(responses, file, indent=4)
        print("Saved geocoded responses as JSON.\n")
    except Exception as json_error:
        error_message = {"error": str(json_error), "traceback": traceback.format_exc()}
        with open("log/error_log.jsonl", "a") as error_file:
            error_file.write(f"{dumps(error_message)}\n")
        print(
            f"Error: {str(json_error)} occurred while saving geocoded responses as JSON. Check log/error_log.jsonl"
        )
        try:
            with open(file_txt, "w") as file:
                file.write(responses)
            print("Saved geocoded responses as text.\n")
        except Exception as txt_error:
            print(
                f"Error: {str(txt_error)} occurred while saving geocoded responses as text."
            )


def extract_dandbid_fips(results: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Extract D&B IDs and state and county FIPS codes into list of dictonaries,
    each with keys DUNS and FIPS from combining state and county code into 5 digit FIPS.
    """
    outer_results = []
    for id, result in results.items():
        if "result" in result:
            if result["result"]["addressMatches"]:
                inner_results = {}
                inner_results["DUNS"] = id
                inner_results["FIPS"] = (
                    f"{result['result']['addressMatches'][0]['geographies']['Counties'][0]['STATE']}{result['result']['addressMatches'][0]['geographies']['Counties'][0]['COUNTY']}"
                )
                outer_results.append(inner_results)
    print(
        f"Extracting complete.\n\t{len(outer_results)} observations mapped to FIPS.\n"
    )
    return outer_results


def process_crosswalk(df: DataFrame) -> DataFrame:
    """
    Process crosswalk data by first filling leading zeros and then renaming columns.
    """
    df["cty_fips"] = df["cty_fips"].astype(int).astype(str).str.zfill(5)
    df["czone"] = df["czone"].astype(int).astype(str).str.zfill(5)
    print(f"Crosswalk processing complete.\n\tDataFrame shape:{df.shape}.\n")
    return df.rename(columns={"cty_fips": "FIPS", "czone": "CZONE"})


def get_aggregates_and_market_share(df: DataFrame) -> DataFrame:
    """
    1. Compute total firms by commuting zone and industry.
    2. Compute total employees by commuting zone,
        aggregate all employees across industries for each commuting zone for labour share.
    3. Compute total sales by commuting zone and industry,
        doesn't make sense to mix production from diffrent industries for sales, hence by industry as well.
    4. Compute market share for employees on location by commuting zone.
    5. Compute market share for sales by commuting zone and industry.
    -----------------------------
    NB: DEMTLHER, employees here on location, variable is used instead of DEMPLTOT, total employees of market participant,
        since interest in labour market share of employees in commuting zone not firms total employees.
    """
    aggregate_columns, mkt_share_emp_columns, mkt_share_sales_columns = (
        [
            "DUNS",
            "DPRIMSI",
            "CZONE",
        ],
        [
            "DEMTLHER",
            "CZONE",
        ],
        [
            "DSALESVO",
            "DPRIMSI",
            "CZONE",
        ],
    )
    df_groupby_dprimsi_czone = (
        df[aggregate_columns]
        .groupby(by=["CZONE", "DPRIMSI"])
        .size()
        .reset_index(name="FIRMSTOTCZI")
    )
    df_aggregate = df.merge(df_groupby_dprimsi_czone, on=["CZONE", "DPRIMSI"])
    df_groupby_emps = (
        df[mkt_share_emp_columns].groupby(by=["CZONE"]).sum().reset_index()
    )
    df_mkt_share_emps = df_aggregate.merge(
        df_groupby_emps, on="CZONE", suffixes=("", "TOTCZ")
    )
    df_groupby_sales = (
        df[mkt_share_sales_columns].groupby(by=["CZONE", "DPRIMSI"]).sum().reset_index()
    )
    df_complete = df_mkt_share_emps.merge(
        df_groupby_sales, on=["CZONE", "DPRIMSI"], suffixes=("", "TOTCZI")
    )
    df_complete["EMPMKTSHAREZ"] = df_complete["DEMTLHER"] / df_complete["DEMTLHERTOTCZ"]
    df_complete["SALESMKTSHAREZI"] = (
        df_complete["DSALESVO"] / df_complete["DSALESVOTOTCZI"]
    )
    print(
        f"Computing aggregates and market shares complete.\n\tDataFrame shape:{df_complete.shape}.\n"
    )
    return df_complete


def get_herfindahl_index(df: DataFrame) -> DataFrame:
    """
    1. Compute weighted Herfindahl Index for labour market concentration by commuting zone and industry.
    2. Compute weighted Herfindahl Index for sales concentration by commuting zone and industry.
    """
    herfindahl_columns = [
        "DPRIMSI",
        "CZONE",
        "HHI_EMP",
        "HHI_SALES",
    ]
    df["HHI_EMP"] = df["EMPMKTSHAREZ"] * (1 / df["FIRMSTOTCZI"])
    df["HHI_SALES"] = df["SALESMKTSHAREZI"] * (1 / df["FIRMSTOTCZI"])
    df_herfindahl = (
        df[herfindahl_columns]
        .groupby(by=["CZONE", "DPRIMSI"])
        .sum()
        .reset_index()
        .rename(columns={"DPRIMSI": "SIC"})
    )
    print(
        f"Computing Herfindahl Index by commuting zone and industry complete.\n\tDataFrame shape:{df_herfindahl.shape}.\n"
    )
    return df_herfindahl
