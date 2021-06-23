from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path
from zipfile import ZipFile

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from pandas import DataFrame
from pandas.io.excel._xlsxwriter import XlsxWriter  # noqa

from common.connectors.mx_mysql import MySQLClient


def clean_c(c: str) -> str:
    for char in (" ", "-", "_", ".", ",", "!", "@", "#"):
        c = c.replace(char, "").lower()
    return c


class DataProfileBuilder:
    def __init__(self, data: DataFrame):
        self.data = data
        self.no_of_rows = len(self.data.columns)
        self.df_mem = self.data.memory_usage(deep=True).sum() / 1024 ** 2  # noqa
        self.data_qlt_df: DataFrame | None = None
        self.desc: DataFrame | None = None
        self.mem_used_dtypes: DataFrame | None = None
        self.data_desc_df: DataFrame | None = None
        self.columns = list(self.data.columns)

    def memory_calc(self) -> None:
        """Used for calculating the memory usages of a single column, currently not shown in output."""
        self.mem_used_dtypes = pd.DataFrame(
            self.data.memory_usage(deep=True) / 1024 ** 2
        )
        self.mem_used_dtypes.rename(columns={0: "memory"}, inplace=True)
        self.mem_used_dtypes.drop("Index", axis=0, inplace=True)

    def construct_dq_df(self) -> None:
        # Create empty DF for metadata
        self.data_qlt_df = pd.DataFrame(
            index=np.arange(0, self.no_of_rows),
            columns=(
                "column_name",
                "col_data_type",
                "col_memory",
                "non_null_values",
                "unique_values_count",
                "column_dtype",
            ),
        )
        assert isinstance(self.mem_used_dtypes, DataFrame)

        # Add rows to the data_qlt_df dataframe
        for ind, cols in enumerate(self.data.columns):
            # Count of unique values in the column
            col_unique_count = self.data[cols].nunique()

            self.data_qlt_df.loc[ind] = [
                cols,
                self.data[cols].dtype,
                self.mem_used_dtypes["memory"][ind],
                self.data[cols].count(),
                col_unique_count,
                cols + "~" + str(self.data[cols].dtype),
            ]

    def descriptive_stats(self) -> None:
        raw_num_df = self.data.describe().T.round(2)
        self.data_qlt_df = pd.merge(
            self.data_qlt_df,
            raw_num_df,
            how="left",
            left_on="column_name",
            right_index=True,
        )

    def stats_filler(self) -> None:
        assert isinstance(self.data_qlt_df, DataFrame)

        # Calculate percentage of non-null values over total number of values
        self.data_qlt_df["%_of_non_nulls"] = (
            self.data_qlt_df["non_null_values"] / self.data.shape[0]
        ) * 100

        # Calculate percentage of non-null values over total number of values
        self.data_qlt_df["%_of_non_nulls"] = (
            self.data_qlt_df["non_null_values"] / self.data.shape[0]
        ) * 100

        # Calculate null values for the column
        self.data_qlt_df["null_values"] = (
            self.data.shape[0] - self.data_qlt_df["non_null_values"]
        )

        # Calculate percentage of null values over total number of values
        self.data_qlt_df["%_of_nulls"] = 100 - self.data_qlt_df["%_of_non_nulls"]

        # Calculate percentage of each column memory usage compared
        # to total memory used by raw data datframe
        self.data_qlt_df["%_of_total_memory"] = (
            self.data_qlt_df["col_memory"] / self.data_qlt_df["col_memory"].sum() * 100
        )

        # Calculate the total memory used by a given group of data type
        # See Notes section at the bottom of this notebook for advatages
        # of using 'transform' function with group_by
        self.data_qlt_df["dtype_total"] = self.data_qlt_df.groupby("col_data_type")[
            "col_memory"
        ].transform("sum")

        # Calculate the percentage memory used by each column data type
        # compared to the total memory used by the group of data type
        # the above can be merged to one calculation if we do not need
        # the total as separate column
        self.data_qlt_df["%_of_dtype_mem"] = (
            self.data_qlt_df["col_memory"] / self.data_qlt_df["dtype_total"] * 100
        )

        # Calculate the percentage memory used by each group of data type of the total memory used by dataset
        self.data_qlt_df["dtype_%_total_mem"] = (
            self.data_qlt_df["dtype_total"] / self.df_mem * 100
        )

        # Calculate the count of each data type
        self.data_qlt_df["dtype_count"] = self.data_qlt_df.groupby("col_data_type")[
            "col_data_type"
        ].transform("count")

        # Calculate the total count of column values
        self.data_qlt_df["count"] = (
            self.data_qlt_df["null_values"] + self.data_qlt_df["non_null_values"]
        )

    def get_desc(self) -> None:
        sql = MySQLClient("client_work_google.field_descriptions")
        q = """SELECT *
        FROM client_work_google.field_descriptions"""
        self.desc = pd.read_sql(q, sql.connect(conn=True))

    def map_desc(self) -> None:
        assert isinstance(self.data_qlt_df, DataFrame)
        assert isinstance(self.desc, DataFrame)

        descriptions = []

        for c in self.columns:
            if "std_" in c or "out_" in c or "res_" in c:
                descriptions.append("Consult document")
            elif "Unnamed" not in c:
                cc = clean_c(c)
                mask = self.desc["mapping"].str.split(", ").apply(lambda x: cc in x)
                response = self.desc[mask]["description_nl"]
                if len(response) > 0:
                    descriptions.append(response[response.index[0]])
                else:
                    descriptions.append("-")
            else:
                descriptions.append("-")

        self.data_qlt_df["description"] = descriptions

    def reorder_df(self) -> None:
        """Reorder the Data Profile Dataframe columns"""
        assert isinstance(self.data_qlt_df, DataFrame)
        self.data_desc_df = self.data_qlt_df[["column_name", "description"]]
        self.data_qlt_df = self.data_qlt_df[
            [
                "column_name",
                "col_data_type",
                "null_values",
                "%_of_nulls",
                "unique_values_count",
                "count",
                "mean",
                "std",
                "min",
                "25%",
                "50%",
                "75%",
                "max",
            ]
        ]


class GraphBuilder:
    def __init__(self, data: DataFrame):
        self.data = data
        self.bool_fields = self.data.apply(lambda x: x.nunique()) <= 2
        self.bool_cols: Iterable[str] = self.bool_fields[
            self.bool_fields is True
        ].index.to_list()
        self.num_cols: Iterable[str] = self.data.select_dtypes(include="number").columns
        self.obj_cols: Iterable[str] = self.data.select_dtypes(include="object").columns
        self.colors = ["#037960", "#05AB89", "#01DEB1", "#7FAE92", "#9AD2B1", "#B4FFD2"]
        self.kleur = sns.color_palette(self.colors)
        self.folder_name = "plots_temp"
        self.exclude: list[str] | None = None
        self.desc: DataFrame | None = None
        sns.set_style("whitegrid")
        sns.set_context("talk")

    def make_folder(self) -> None:
        os.makedirs(self.folder_name, exist_ok=True)

    def del_folder(self) -> None:
        for f in Path(self.folder_name).glob("*"):
            f.unlink()
        os.removedirs(self.folder_name)

    def exclude_cols(self) -> None:
        sql = MySQLClient("client_work_google.field_descriptions")
        q = """SELECT *
        FROM client_work_google.field_descriptions"""
        self.desc = pd.read_sql(q, sql.connect(conn=True))

        self.exclude = (
            self.desc[self.desc["field"] == "huisnummer"]["mapping"]
            .to_list()[0]
            .split(", ")
            + self.desc[self.desc["field"] == "huisnummer_toevoeging"]["mapping"]
            .to_list()[0]
            .split(", ")
            + self.desc[self.desc["field"] == "huisnummer_bag_toevoeging"]["mapping"]
            .to_list()[0]
            .split(", ")
            + self.desc[self.desc["field"] == "huisnummer_bag_letter"]["mapping"]
            .to_list()[0]
            .split(", ")
            + self.desc[self.desc["field"] == "telefoonnummer"]["mapping"]
            .to_list()[0]
            .split(", ")
            + self.desc[self.desc["field"] == "mobielnummer"]["mapping"]
            .to_list()[0]
            .split(", ")
        )

    def bool_graph(self) -> None:
        for col_name in self.bool_cols:
            if len(self.data[col_name].value_counts()) > 0:
                data_nonull = self.data[self.data[f"{col_name}"].notna()]

                fig, ax = plt.subplots(figsize=(10, 7))
                fig.subplots_adjust(top=0.8)
                plt.subplots_adjust(hspace=0.4, bottom=0.2)
                fig.suptitle(f"Data profiel van {col_name}", fontsize=25)

                ax.set_title("Distribution plot", fontsize=15)
                g = sns.countplot(
                    data=data_nonull,
                    x=col_name,
                    order=self.data[f"{col_name}"].value_counts().iloc[:15].index,
                    palette=self.kleur,
                )
                g.set_title("Frequentie")

                for p in g.patches:
                    g.annotate(
                        f"{p.get_height() / len(self.data) * 100:.1f} %",
                        (p.get_x() + p.get_width() / 2.0, p.get_height()),
                        ha="center",
                        va="center",
                        xytext=(0, 13),
                        textcoords="offset points",
                        fontsize=20,
                        color="dimgrey",
                    )

                fig_name = "fig_" + col_name
                fig.savefig(self.folder_name + "/" + fig_name, dpi=50)

                plt.close("all")

    def num_graph(self) -> None:
        assert isinstance(self.exclude, list)
        for col_name in set(self.num_cols) - set(self.bool_cols) - set(self.exclude):
            if len(self.data[col_name].value_counts()) > 0:
                data_nonull = self.data[self.data[f"{col_name}"].notna()]

                fig, ax = plt.subplots(1, 3, figsize=(20, 7))
                fig.subplots_adjust(top=0.8)
                plt.subplots_adjust(wspace=0.4, hspace=0.2, bottom=0.2)
                fig.suptitle("Data profiel van " + col_name, fontsize=25)
                ax[0].set_title("Distributie", fontsize=20)
                ax[1].set_title("Boxplot zonder uitschieters", fontsize=20)
                ax[2].set_title("Boxplot alle waardes", fontsize=20)

                sns.histplot(
                    data_nonull,
                    x=f"{col_name}",
                    ax=ax[0],
                    color="#0E5C59",
                    kde=True,
                    linewidth=0,
                )
                sns.boxplot(
                    y=data_nonull[f"{col_name}"],
                    ax=ax[1],
                    showfliers=False,
                    width=0.3,
                    color="#05AB89",
                )
                sns.boxplot(
                    y=data_nonull[f"{col_name}"], ax=ax[2], width=0.3, color="#9AD2B1"
                )

                fig_name = "fig_" + col_name
                fig.savefig(self.folder_name + "/" + fig_name, dpi=50)

                plt.close("all")

    def obj_graph(self) -> None:
        assert isinstance(self.exclude, list)
        for col_name in set(self.obj_cols) - set(self.bool_cols) - set(self.exclude):
            if len(self.data[col_name].value_counts()) > 0:
                fig, ax = plt.subplots(figsize=(20, 7))
                fig.subplots_adjust(top=0.8)
                fig.suptitle("Data profiel van " + col_name, fontsize=25)

                ax.set_title("Distribution plot", fontsize=15)
                g = sns.countplot(
                    data=self.data,
                    y=col_name,
                    order=self.data[f"{col_name}"].value_counts().iloc[:15].index,
                    palette=self.kleur,
                )
                g.set_title("Frequentie van meest voorkomende waardes")

                for p in g.patches:
                    percentage = f"{100 * p.get_width() / len(self.data):.1f}%"
                    x = p.get_x() + p.get_width() + 0.02
                    y = p.get_y() + p.get_height() / 2
                    g.annotate(
                        percentage,
                        (x, y),
                        ha="center",
                        va="center",
                        fontsize=20,
                        color="dimgrey",
                        xytext=(30, 0),
                        textcoords="offset points",
                    )

                fig_name = "fig_" + col_name
                fig.savefig(self.folder_name + "/" + fig_name, dpi=50)

                plt.close("all")


class CodebookBuilder:
    def __init__(
        self,
        data_stat: DataFrame,
        data_desc: DataFrame,
        folder: ZipFile,
        to_zip: bool = True,
    ):
        self.data_stat = data_stat
        self.data_desc = data_desc
        self.writer = pd.ExcelWriter("CodeBoek.xlsx", engine="xlsxwriter")
        self.start = -19
        assert isinstance(self.writer, XlsxWriter)
        self.workbook = self.writer.book
        self.cell_format = self.workbook.add_format()

        self.cell_format.set_align("left")
        self.cell_format.set_align("vcenter")
        self.cell_format.set_text_wrap()

        self.folder = folder
        self.to_zip = to_zip
        self.text_info: str | None = None

    def get_text(self) -> None:
        with open(Path(__file__).parent / "codebook_info.txt") as file:
            self.text_info = file.read()

    def info_page(self) -> None:
        worksheet = self.workbook.add_worksheet("Info")
        options = {
            "width": 2560,
            "height": 1000,
            "fill": {"color": "#E6F6E6"},
        }
        worksheet.insert_textbox("A1", self.text_info, options)

    def meta_page(self) -> None:
        self.data_stat.to_excel(self.writer, sheet_name="Data_overzicht", index=False)
        worksheet = self.writer.sheets["Data_overzicht"]
        worksheet.set_column("A:A", 20)
        worksheet.set_column("B:P", 13)

    def distribution_page(self) -> None:
        cell_format1 = self.workbook.add_format(
            {"bold": True, "font_color": "037960", "font_size": 15, "shrink": True}
        )
        cell_format2 = self.workbook.add_format({"bottom": True})

        for index, row in self.data_stat.iterrows():
            self.start += 20
            col = pd.DataFrame(self.data_stat.iloc[index].reset_index().values).dropna()
            col.to_excel(
                self.writer,
                sheet_name="Data_profiel",
                startrow=self.start + 2,
                header=False,
                index=False,
            )
            worksheet = self.writer.sheets["Data_profiel"]
            worksheet.set_column("A:A", 20)
            worksheet.set_column("B:B", 15, self.cell_format)

            worksheet.write(self.start, 0, self.data_stat.iloc[index][0], cell_format1)
            worksheet.set_row(self.start - 2, 18, cell_format2)
            worksheet.insert_image(
                f"F{self.start + 2}",
                "plots_temp/fig_" + row[0] + ".png",
                {"x_scale": 0.5, "y_scale": 0.5},
            )

    def desc_page(self) -> None:
        self.data_desc.to_excel(
            self.writer, sheet_name="Data_omschrijving", index=False
        )
        worksheet = self.writer.sheets["Data_omschrijving"]
        worksheet.set_column("A:A", 25)
        worksheet.set_column("B:B", 150)

    def save_xlsx(self) -> None:
        self.writer.save()
        self.folder.write("CodeBoek.xlsx")
        if self.to_zip:
            os.remove("CodeBoek.xlsx")


def codebook_exe(
    data: DataFrame,
    folder: ZipFile,
    to_zip: bool = True,
) -> str:
    dp_b = DataProfileBuilder(data)
    dp_b.memory_calc()
    dp_b.construct_dq_df()
    dp_b.descriptive_stats()
    dp_b.stats_filler()
    dp_b.get_desc()
    dp_b.map_desc()
    dp_b.reorder_df()

    gr_b = GraphBuilder(data)
    gr_b.make_folder()
    gr_b.exclude_cols()
    gr_b.bool_graph()
    gr_b.num_graph()
    gr_b.obj_graph()

    cb_b = CodebookBuilder(
        data_stat=dp_b.data_qlt_df,
        data_desc=dp_b.data_desc_df,
        folder=folder,
        to_zip=to_zip,
    )
    cb_b.get_text()
    cb_b.info_page()
    cb_b.meta_page()
    cb_b.distribution_page()
    cb_b.desc_page()
    cb_b.save_xlsx()

    gr_b.del_folder()

    return "Codebook.xlsx"
