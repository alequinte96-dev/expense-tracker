import re
from typing import Literal

import pandas as pd

from expense_tracker.utils.logger import LOGGER
from expense_tracker.wells_fargo.parser import WellsFargoAccountSummaryParser


class ChaseAccountSummaryParser(WellsFargoAccountSummaryParser):
    def __init__(
        self,
        csv_name: str,
        bank: Literal["Chase"] = "Chase",
        data_type: Literal["AccountActivity"] = "AccountActivity",
    ):
        super().__init__(csv_name, bank, data_type)
        self.get_card_id()

    def get_card_id(self):
        """
        Extract the card ID from the CSV name.
        This is a placeholder method and should be implemented based on actual logic.
        """
        LOGGER.debug(f"Processing CSV: {self.csv_name}")
        match = re.search(r"(\d{4})", self.csv_name, re.IGNORECASE)
        LOGGER.debug(f"Match found: {match}")
        if match:
            self.card_id = int(match.group(0))
        else:
            self.card_id = 0000
        LOGGER.debug(f"Card ID set to: {self.card_id}")

    def load_df(self):
        """
        Load the CSV file into a DataFrame.
        """
        self.df = pd.read_csv(
            self.csv_file_path,
            header=0,
            parse_dates=["Transaction Date", "Post Date"],
        )
        if self.df.empty:
            LOGGER.warning(f"No data found in {self.csv_name}.")
        else:
            self.df.drop(columns=["Post Date", "Memo"], inplace=True, errors="ignore")
            self.create_id()
            self.df["Card"] = self.card_id
            self.df.rename(
                columns={
                    "Transaction Date": "Date",
                },
                inplace=True,
            )
            self.df.sort_values(by=["Date", "Description"], inplace=True)
            self.df["Amount"] = self.df["Amount"] * -1  # Make all amounts positive
            self.df["Bank"] = self.bank
        return self.df


if __name__ == "__main__":
    # Example usage of ChaseAccountSummaryParser
    parser = ChaseAccountSummaryParser(
        csv_name="Chase9088_Activity20230719_20250719_20250719.CSV"
    )
    df = parser.load_df()
    print(df.head())
    parser.save_to_aggregate()
