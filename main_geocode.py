from pandas import read_csv, DataFrame

from time import perf_counter
from datetime import timedelta

from utils import (
    parse_data,
    keep_required_columns,
    fill_leading_zeros,
    df_to_dict,
    multi_geocode_data,
    save_geocoded_responses,
    extract_dandbid_fips,
    process_crosswalk,
    get_aggregates_and_market_share,
    get_herfindahl_index,
)
from config import (
    D_AND_B_TEXT,
    D_AND_B_CSV,
    D_AND_B_STATA,
    GEOCODED_RESPONSES_JSON,
    GEOCODED_RESPONSES_TEXT,
    D_AND_B_FIPS_CSV,
    D_AND_B_FIPS_STATA,
    CZONE_CSV,
    D_AND_B_CZONE_CSV,
    D_AND_B_CZONE_STATA,
    D_AND_B_ANALYSIS_CSV,
    D_AND_B_ANALYSIS_STATA,
    D_AND_B_COMPLETE_CSV,
    D_AND_B_COMPLETE_STATA,
    D_AND_B_HERFINDAHL_CSV,
    D_AND_B_HERFINDAHL_STATA,
)


def main() -> None:

    # Parse D&B data according to guideline to .csv and .dta, uses all available cores
    # parse_data(D_AND_B_TEXT, D_AND_B_CSV, D_AND_B_STATA)

    # Read D&B data
    df = read_csv(D_AND_B_CSV)

    # Keep columns for geo-locating addresses to FIPS codes
    df_dropped = keep_required_columns(df)

    # Fill leading zeros for D&B IDs and ZIP columns
    df_processed = fill_leading_zeros(df_dropped)

    # Convert DataFrame to list of dictionaries each representing a row
    data_geolocating = df_to_dict(df_processed)

    # Use multiprocessing to send requests to geo-coder to get FIPS codes
    # Uses all available cores to send requests in parallel
    geocoder_responses = multi_geocode_data(data_geolocating)

    # Write API responses to JSON file, outer keys are D&B IDs
    save_geocoded_responses(
        geocoder_responses, GEOCODED_RESPONSES_JSON, GEOCODED_RESPONSES_TEXT
    )

    # Extract outer D&B IDs key as D&B column and,
    # state and county codes then combined into FIPS column
    extracted_result = extract_dandbid_fips(geocoder_responses)

    # Covert list of dictionaries containing D&B IDs and FIPS codes into DataFrame
    df_mapping = DataFrame(extracted_result)

    # Save geocoded responses data
    df_mapping.to_csv(D_AND_B_FIPS_CSV, index=False)
    df_mapping.to_stata(D_AND_B_FIPS_STATA, write_index=False)

    # Read FIPS commuting zone crosswalk data
    df_crosswalk = read_csv(CZONE_CSV)

    # Process crosswalk file
    df_crosswalk_processed = process_crosswalk(df_crosswalk)

    # Merge D&B IDs and commuting zones on FIPS
    df_mapped = df_mapping.merge(df_crosswalk_processed, how="inner", on=["FIPS"])

    # Save D&B to commuting zone mapping
    df_mapped.to_csv(D_AND_B_CZONE_CSV, index=False)
    df_mapped.to_stata(D_AND_B_CZONE_STATA, write_index=False)

    # Merge D&B processed and D&B commuting zone mapped on DUNS
    df_ready = df_processed.merge(df_mapped, how="inner", on=["DUNS"])

    # Save D&B data with FIPS and CZONE
    df_ready.to_csv(D_AND_B_ANALYSIS_CSV, index=False)
    df_ready.to_stata(D_AND_B_ANALYSIS_STATA, write_index=False)

    # Compute total firms by commuting zone and industry,
    # and market shares of participants
    df_complete = get_aggregates_and_market_share(df_ready)

    # Save D&B data complete with market shares
    df_complete.to_csv(D_AND_B_COMPLETE_CSV, index=False)
    df_complete.to_stata(D_AND_B_COMPLETE_STATA, write_index=False)

    # Compute Herfindahl Index by commuting zone and industry,
    # for employees and sales
    df_herfindahl = get_herfindahl_index(df_complete)

    # Save Herfindahl dataset: CZONE, DPRIMSI, HHI_EMP, HHI_SALES
    df_herfindahl.to_csv(D_AND_B_HERFINDAHL_CSV, index=False)
    df_herfindahl.to_stata(D_AND_B_HERFINDAHL_STATA, write_index=False)


if __name__ == "__main__":

    start_time = perf_counter()

    main()

    end_time = perf_counter()

    print(f"Time: {str(timedelta(seconds=end_time - start_time))}\n")
