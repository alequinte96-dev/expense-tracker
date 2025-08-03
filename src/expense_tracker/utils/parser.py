from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal

import pandas as pd

from expense_tracker.utils.logger import LOGGER

data_path = Path.cwd() / "data"


class PDFParser(ABC):
    def __init__(
        self,
        pdf_name: str,
        bank: Literal[
            "WellsFargo", "Chase", "CapitalOne", "Ally", "Deserve", "Syncrony"
        ] = "WellsFargo",
    ):
        self.df = pd.DataFrame()
        self.tables = []
        self.id = None
        self.year = None
        self.bank = bank
        self.data_path = data_path
        self.pdf_name = pdf_name
        self.pdf_path = self.data_path / self.bank / pdf_name
        self.aggregate_file = data_path / self.bank / "aggregate.tsv"

    @abstractmethod
    def parse(self):
        """
        Abstract method to parse the PDF and return a DataFrame.
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def save_to_aggregate(self):
        """
        Save the parsed DataFrame to the aggregate file.
        If the file does not exist, it will be created.
        """
        pass

    def save_to_tsv(self, filename: str | None = None):
        """
        Save the parsed DataFrame to a TSV file.
        """
        if filename is None:
            filename = str(self.pdf_path).split(".")[0] + ".tsv"
        if hasattr(self, "df") and not self.df.empty:
            self.df.to_csv(filename, header=True, index=False, sep="\t")
            LOGGER.info(f"Data saved to {filename}")
        else:
            LOGGER.warning("No data to save.")


class CSVParser(ABC):
    def __init__(
        self,
        csv_name: str,
        bank: Literal[
            "WellsFargo", "Chase", "CapitalOne", "Ally", "Deserve", "Syncrony"
        ] = "WellsFargo",
        data_type: Literal[
            "YearEnd", "Statements", "AccountActivity"
        ] = "AccountActivity",
        *args,
        **kwargs,
    ):
        self.csv_name = csv_name
        self.bank = bank
        self.data_type = data_type
        self.data_path = data_path
        self.aggregate_file = data_path / self.bank / "aggregate.tsv"
        self.csv_file_path = self.data_path / self.bank / self.data_type / self.csv_name
        self.add_new_only: bool = kwargs.get("add_new_only", True)

    @abstractmethod
    def load_df(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def save_to_aggregate(self):
        """
        Save the parsed DataFrame to the aggregate file.
        If the file does not exist, it will be created.
        """
        pass
