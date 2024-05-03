from pandas import read_csv

from time import perf_counter
from datetime import timedelta

from utils import (
    parse_data,
    keep_required_columns,
    fill_leading_zeros,
    fill_leading_zeros_zip,
    zip_combine_state_and_county,
    process_crosswalk,
    get_aggregates_and_market_share,
    get_herfindahl_index,
)
from config import (
    D_AND_B_TEXT,
    D_AND_B_CSV,
    D_AND_B_STATA,
    CZONE_CSV,
    D_AND_B_CZONE_CSV,
    D_AND_B_CZONE_STATA,
    D_AND_B_ANALYSIS_CSV,
    D_AND_B_ANALYSIS_STATA,
    D_AND_B_COMPLETE_CSV,
    D_AND_B_COMPLETE_STATA,
    D_AND_B_HERFINDAHL_CSV,
    D_AND_B_HERFINDAHL_STATA,
    ZIP_CODE_CSV,
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

    # Read Sampsa's ZIP code data
    df_zip = read_csv(
        ZIP_CODE_CSV,
        usecols=["zipcode", "statefips", "countyfips"],
    )

    # Fill leading zeros for ZIPs
    df_zip_filled = fill_leading_zeros_zip(df_zip)

    # Combine state and county codes into FIPS
    df_zip_processed = zip_combine_state_and_county(df_zip_filled)

    # Read FIPS commuting zone crosswalk data
    df_crosswalk = read_csv(CZONE_CSV)

    # Process crosswalk file
    df_crosswalk_processed = process_crosswalk(df_crosswalk)

    # Merge ZIP and crosswalk on FIPS
    df_mapped = df_zip_processed.merge(df_crosswalk_processed, how="inner", on=["FIPS"])

    # Drop duplicate rows using key DZIP5, multiple ZIPs in FIPS and CZONE
    df_mapped.drop_duplicates(subset="DZIP5", inplace=True)

    # Save mapping dataset: DZIP5, FIPS, CZONE
    df_mapped.to_csv(D_AND_B_CZONE_CSV, index=False)
    df_mapped.to_stata(D_AND_B_CZONE_STATA, write_index=False)

    # Merge D&B processed and ZIP to commuting zone mapping
    df_ready = df_processed.merge(df_mapped, how="inner", on=["DZIP5"])

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
