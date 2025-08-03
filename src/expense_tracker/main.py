from typing import Literal, cast

from expense_tracker.utils.logger import LOGGER
from expense_tracker.utils.processing import ProcessingUtils, get_parser_class

banks = ["CapitalOne", "Chase", "WellsFargo"]


def fetch_data():
    """
    Fetch data for each bank and process it using the appropriate parser.
    """
    for bank in banks:
        LOGGER.info(f"Processing data for {bank}...")
        cast(Literal["CapitalOne", "Chase", "WellsFargo"], bank)
        parser = get_parser_class(
            cast(Literal["CapitalOne", "Chase", "WellsFargo"], bank)
        )
        processor = ProcessingUtils(
            bank=cast(Literal["CapitalOne", "Chase", "WellsFargo"], bank),
            parser=parser,
            data_type="AccountActivity",
        )
        processor.load_directory()
        processor.parse_files()
        LOGGER.info(f"Finished processing data for {bank}.")
    LOGGER.info("All data processing complete.")


if __name__ == "__main__":
    fetch_data()
