import re
from typing import Literal

import pandas as pd

from expense_tracker.utils.logger import LOGGER
from expense_tracker.wells_fargo.parser import WellsFargoAccountSummaryParser


class CapitalOneAccountSummaryParser(WellsFargoAccountSummaryParser):
    def __init__(
        self,
        csv_name: str,
        bank: Literal["CapitalOne"] = "CapitalOne",
        data_type: Literal["AccountActivity"] = "AccountActivity",
    ):
        super().__init__(csv_name, bank, data_type)

    def load_df(self):
        """
        Load the CSV file into a DataFrame.
        """
        self.df = pd.read_csv(
            self.csv_file_path,
            header=0,
            parse_dates=["Date"],
        )
        if self.df.empty:
            LOGGER.warning(f"No data found in {self.csv_name}.")
        else:
            self.df.drop(columns=["Post Date", "Memo"], inplace=True, errors="ignore")
            self.create_id()
            self.df["Card"] = self.df["Card"].apply(
                lambda s: (
                    int(s[-4:])
                    if isinstance(s, str) and len(s) > 4 and s[-4:].isnumeric()
                    else 0000
                )
            )
            self.df.sort_values(by=["Date", "Description"], inplace=True)
        return self.df


if __name__ == "__main__":
    # Example usage of ChaseAccountSummaryParser
    parser = CapitalOneAccountSummaryParser(
        csv_name="Capital-One-Spending-Insights-Transactions.csv"
    )
    df = parser.load_df()
    print(df.head())
    parser.save_to_aggregate()
