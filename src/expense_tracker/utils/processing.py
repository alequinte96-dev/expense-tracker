import re
from abc import ABC
from pathlib import Path
from typing import Literal, Type

from expense_tracker.utils.logger import LOGGER
from expense_tracker.utils.parser import CSVParser


class ProcessingUtils(ABC):
    """Utility class for processing data."""

    def __init__(
        self,
        bank: Literal[
            "WellsFargo", "Chase", "CapitalOne", "Ally", "Deserve", "Syncrony"
        ],
        parser: Type[CSVParser],
        data_type: Literal[
            "YearEnd", "AccountActivity", "Statements"
        ] = "AccountActivity",
        *args,
        **kwargs,
    ):
        self.bank: Literal[
            "WellsFargo", "Chase", "CapitalOne", "Ally", "Deserve", "Syncrony"
        ] = bank
        self.data_type: Literal["YearEnd", "AccountActivity", "Statements"] = data_type
        self.data_path = Path.cwd() / "data" / self.bank / data_type
        self.aggregate_file = self.data_path / "aggregate.tsv"
        self.parser = parser
        self.global_aggregate_path = Path.cwd() / "data" / "global_aggregate.tsv"

    def load_directory(self):
        """
        Load the directory for the specified bank.
        If the directory does not exist, it will be created.
        """
        if not self.data_path.exists():
            self.data_path.mkdir(parents=True, exist_ok=True)
            LOGGER.info(f"Directory created: {self.data_path}")
        else:
            LOGGER.info(f"Directory already exists: {self.data_path}")
            LOGGER.info(f"Files in directory: {list(self.data_path.iterdir())}")
            self.data = [
                file
                for file in self.data_path.iterdir()
                if file.is_file() and re.search(r".csv", file.suffix, re.IGNORECASE)
            ]

    def parse_files(self):
        """
        Parse all files in the directory using the specified parser.
        """
        if not hasattr(self, "data") or not self.data:
            LOGGER.warning(
                "No CSV files found to parse. Run load_directory() first "
                "or make sure all files are in data/bank/data_type."
            )
            return

        for file in self.data:
            parser_instance = self.parser(file.name, self.bank, self.data_type)
            df = parser_instance.load_df()
            if not df.empty:
                parser_instance.save_to_aggregate()
                LOGGER.info(f"Parsed and saved data from {file.name}")
            else:
                LOGGER.warning(f"No data found in {file.name}")
        parser_instance.save_to_global_aggregate()


def get_parser_class(
    bank: Literal["WellsFargo", "Chase", "CapitalOne"],
) -> Type[CSVParser]:
    """
    Get the parser class for the specified bank.
    """
    if bank == "WellsFargo":
        from expense_tracker.wells_fargo.parser import WellsFargoAccountSummaryParser

        return WellsFargoAccountSummaryParser
    elif bank == "Chase":
        from expense_tracker.chase.parser import ChaseAccountSummaryParser

        return ChaseAccountSummaryParser
    elif bank == "CapitalOne":
        from expense_tracker.capital_one.parser import CapitalOneAccountSummaryParser

        return CapitalOneAccountSummaryParser
    else:
        raise ValueError(f"Unsupported bank: {bank}")


if __name__ == "__main__":
    # Example usage
    from expense_tracker.wells_fargo.parser import WellsFargoAccountSummaryParser

    processor = ProcessingUtils(
        "WellsFargo", WellsFargoAccountSummaryParser, "AccountActivity"
    )
    processor.load_directory()
    processor.parse_files()
    LOGGER.info("Processing complete.")
