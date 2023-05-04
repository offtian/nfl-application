import logging
import os
import pandas as pd
import glob
from typing import List

# from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.getLogger().setLevel(logging.INFO)


def run_preprocess_data_and_insert_db(
    file_dir: str,
    query_target: str,
    output_file_name: str,
    postgres_conn_id: str,
    headers: List[int] = [0],
    output_dir: str = "../../outputs/",
) -> None:
    """Call dfDBprep class, preprocess data and save dataframes to disk.

    Args:
        file_dir (str): file directory to downloded pro-football-reference data
        query_target (str): query to run on pro-football-reference data
        output_file_name (str): new file name for pro-football-reference data
        postgres_conn_id (str): postgres connection id
        headers (List[int]): list of column headers. Defaults to [0].
        skip_rows (int, optional): number of rows to ignore from original pro-football-reference data. Defaults to 6.
    """
    # hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    # conn = hook.get_conn()
    file_paths = glob.glob(f"{file_dir}/*.csv")  # find the path to the latest download
    print("file path:", file_paths)
    data_process = dfDBprep(
        df_paths=file_paths,
        query_target=query_target,
        postgres_conn_id=postgres_conn_id,
        postgres_table=output_file_name,
        headers=headers,
    )  # initialise dfDBprep class

    df_processed = data_process.preprocess().df  # preprocess data
    # save processed data to csv
    df_processed.to_csv(
        os.path.join(output_dir, output_file_name + "_processed.csv"), index=False
    )
    # df_processed.to_sql(
    #     name=query_target,
    #     conn=conn,
    #     schema=output_file_name,
    #         if_exists="append",
    # )


class dfDBprep(object):

    """A collection of methods to clean up the dataframe's column names and
    read pro-football-reference data"""

    def __init__(
        self,
        df_paths,
        query_target,
        postgres_conn_id,
        postgres_table,
        headers=[0],
        # skiprows=0,
    ):
        """Initialise class

        Parameters
        ----------
        df_path : List[str]
            The path of the pro-football-reference files.
        skiprows: int, optional
            The number of rows to skip due to the pro-football-reference header.

        """
        self._df_paths = df_paths
        self._skiprows = 0
        self._headers = headers
        self.postgres_conn_id = postgres_conn_id
        self.postgres_schema = f"bronze_{query_target}"
        self.postgres_table = postgres_table

    def _read_multiple_dfs(self):
        """Reads the dataframe from a file.
        Identifies and uses appropriate method for csv or xlsx file type.

        Returns
        -------
            self

        """

        # Checking file type
        if os.path.splitext(self._df_paths[0])[-1] == ".csv":
            self.df = pd.concat(
                (
                    pd.read_csv(f, header=[0], skiprows=0).assign(
                        StatsYear=f[-8:-4]
                    )
                    for f in self._df_paths
                ),
                ignore_index=True,
            )

            logging.info(f"The data is in a dataframe of shape {self.df.shape}.")

        else:
            logging.error(
                "The file extension is not supported. Please review the data."
            )

            raise Exception(
                "The file extension is not supported. Please review the data."
            )

        # merge multi columns
        if len(self._headers) > 1:
            self.df.columns = [
                "_".join(pair) if not pair[0].startswith("Unnamed") else pair[1]
                for pair in self.df.columns
            ]

        # Droping unused columns
        unused_cols = self.df.filter(regex="Unnamed").columns
        if len(unused_cols):
            logging.info("This dataset contains an Unnamed column to be removed.")
            self.df = self.df.drop(columns=unused_cols, axis=1)
            logging.info(f"The new shape is {self.df.shape}.")

    # def _query_non_pk_columns(self):
    #     hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
    #     sql_query = f"""
    #         WITH primary_keys AS (
    #             SELECT
    #                 a.attname AS column_name
    #             FROM
    #                 pg_index i
    #                 JOIN pg_attribute a ON a.attrelid = i.indrelid
    #                     AND a.attnum = ANY (i.indkey)
    #             WHERE
    #                 i.indrelid = '{self.postgres_schema}.{self.postgres_table}'::regclass
    #                 AND i.indisprimary
    #         )

    #         SELECT COLUMN_NAME
    #         FROM information_schema.COLUMNS
    #         WHERE TABLE_SCHEMA = '{self.postgres_schema}'
    #             AND TABLE_NAME = '{self.postgres_table}'
    #             AND column_name NOT IN (SELECT column_name from primary_keys);
    #         """
    #     df_columns = hook.get_pandas_df(sql_query)

    #     return df_columns.column_name.values

    # def filter_columns(self, selected_columns, primary_key):
    #     # convert selected columns to a list
    #     selected_columns = list(selected_columns)
    #     # dynamically select all non pk columns
    #     if primary_key:
    #         logging.info(
    #             "Primay key specified. Idenitfying all non primary key columns"
    #         )
    #         columns = self._query_non_pk_columns()
    #         self.df = self.df.loc[:, columns]
    #     # only include selected columns
    #     elif selected_columns:
    #         logging.info("Selected columns specified manually.")
    #         self.df = self.df.loc[:, selected_columns]

    #     # else dataframe is not affected
    #     logging.info("All columns from csv being used")

    #     return self

    def _create_missing_cols(self, *cols_missing):
        """Creates missing columns in case for example  changes the
        available columns and fills with n/a

        Parameters
        ----------
        cols_missing : list

        Returns
        -------
        self

        """
        for col in cols_missing:
            if col not in self.df.columns:
                self.df[col] = pd.NA
            else:
                pass

        return self

    def _clean_cols(self):
        """
        Cleans up the column names of a DF to make them Postgres friendly.
        For example any spaces are replaced with underscores etc.


        Returns
        -------
        dictionary
            The dictionary of original to cleaned column names

        """
        self._clean_col_names = {}
        for col in getattr(self.df, "columns"):
            self._clean_col_names[col] = (
                str(col)
                .strip()
                .lower()
                .replace("/", "_or_")
                .replace(" ", "_")
                .replace("(", "")
                .replace(")", "")
                .replace("%", "_ratio")
                .replace("-", "_minus_")
                .replace("+", "_plus_")
                .replace(".", "_")
                .replace(":", "")
                .replace("'", "")
                .replace("&", "_and_")
            )
        return self._clean_col_names

    def _clean_df_cols(self):
        """Renames the columns of the dataFrame to make them PostgreSQL
        friendly.

        Returns
        -------
        self

        """

        self.df.rename(columns=self._clean_cols(), inplace=True)
        logging.info("Prepared the column names for PostgreSQL.")

    def _creation_date_col(self):
        """Creates a creation_date column to keep track of data ingestion.

        Returns
        -------
        self

        """
        self.df["creation_date"] = pd.Timestamp.now(tz="Europe/London")
        logging.info(
            f"Creation date of this dataframe is {pd.Timestamp.now(tz='Europe/London')}"
        )

    def preprocess(self):
        """Preprocesses the dataframe.

        Returns
        -------
        self
        """
        self._read_multiple_dfs()
        self._clean_df_cols()
        self._creation_date_col()
        return self
