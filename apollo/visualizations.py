"""Module for making Matrixian style visualizations.

Example 1::
    y = np.random.randn(100)
    x = np.linspace(1,100,len(y))

    mx_plot = Plot_mx()
    mx_plot.makefig(figsize = (8,8), grid = True, xlabel = 'ex-axis', ylabel = 'why-axis', title = 'title')
    mx_plot.spaghetti(np.linspace(1,100,len(y)) , y)
    mx_plot.spaghetti(np.linspace(1,10,len(y)) , y, color = 'green')
    plt.show()

Example 2::
    n = 100000
    x = np.random.randn(n)
    y = x + np.random.randn(n)

    mx_plot = Plot_mx()
    mx_plot.makefig(figsize = (8,8), grid = True, xlabel = 'ex-axis', ylabel = 'why-axis', title = 'title')
    mx_plot.histogram(TwoD = True,
                      x = x,
                      y = y)

Example 3::
    mx_plot = Plot_mx()
    mx_plot.makefig()
    mx_plot.bar(names = ['Nuclear', 'Hydro', 'Gas', 'Oil', 'Coal', 'Biofuel'],
                x = [5, 6, 15, 22, 24, 8],
                y = [1,2,3,4,5,6],
                horizontal=True)
"""

from __future__ import annotations

__all__ = (
    "DistributionPlot",
    "PlotMx",
    "RadarPlot",
    "plot_stacked_bar",
)

from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib import patches, path, rc
from matplotlib.colors import ListedColormap, LogNorm
from numpy.typing import NDArray

mx_colors = {
    "dark_green": "#0E5C59",
    "green": "#05AB89",
    "light_green": "#9AD2B1",
    "blue_green": "#52BBB5",
    "light_purple": "#C975AA",
    "purple": "#C94397",
}
mx_cmap = ListedColormap(sns.color_palette(mx_colors.values()).as_hex())
plt.style.use("seaborn")


class PlotMx:
    """Make plots, Matrixian style!"""

    def __init__(self) -> None:
        self.size: int = 16

    def makefig(
        self,
        xlabel: str = "No label",
        ylabel: str = "No label",
        dpi: int = 50,
        figsize: tuple[int, int] = (8, 8),
        grid: bool = True,
        size: int | None = None,
        title: str = "",
    ) -> PlotMx:
        """Pre-set the figure with its figure size, labels, grid, sizes, and title name."""

        if size is not None:
            self.size = size
        plt.figure(figsize=figsize, dpi=dpi)
        plt.grid(grid)
        plt.xticks(size=self.size)
        plt.yticks(size=self.size)
        plt.xlabel(xlabel, size=self.size)
        plt.ylabel(ylabel, size=self.size)
        plt.title(title, size=self.size + 2)

        return self

    def histogram(
        self,
        x: Any,
        y: Any,
        two_d: bool = False,
        bins: int = 20,
        color: str = mx_colors["dark_green"],
    ) -> PlotMx:
        """Make a histogram.

        Make sure to call the makefig method first.

        x       ->  Insert your x-axis data
        y       ->  Insert your y-axis data
        two_d   ->  You can choose for a TwoD plot where you can make a histogram density.
        color   ->  color can be changed but is by default matrixian dark green
        bins    ->  by default set on 20
        """
        if two_d:
            plt.hist2d(x, y, norm=LogNorm(), bins=bins, cmap=mx_cmap)
            cbar = plt.colorbar(orientation="horizontal", pad=0.15)
            cbar.ax.set_ylabel("Counts", size=self.size)
        else:
            plt.hist(x, color=color, bins=bins)

        return self

    def spaghetti(
        self,
        x: Any,
        y: Any,
        color: str = mx_colors["dark_green"],
    ) -> PlotMx:
        """Make a line plot.

        x       ->  Insert your x-axis data
        y       ->  Insert your y-axis data
        color   ->  color can be changed but is by default matrixian dark green
        """
        plt.plot(x, y, color=color)
        return self

    def bar(
        self,
        x: Any,
        y: Any,
        names: list[str] | None = None,
        horizontal: bool = False,
        width: float = 0.25,
        color: str | list[str] | None = None,
    ) -> PlotMx:
        """Make a barplot.

        x           ->  Insert your x-axis data
        y           ->  Insert your y-axis data
        names       ->  Names of the bars
        horizontal  ->  True or False gives you the option to do a horizontal or vertical bar plot
        width       ->  You can change the width of the bars. This is by default 0.25
        color       ->  You can choose the colors in list (one or two colors) or as string (one color).
                        By default these are matrixian purple and matrixian green
        """

        if not color:
            color = [mx_colors["purple"], mx_colors["green"]]
        if isinstance(color, str):
            color = [color]
        if len(color) == 1:
            color = [*color, *color]

        if not names:
            names = []
        x_pos = [i for i, _ in enumerate(names)]
        if y:
            ind = np.arange(len(x))  # the x locations for the groups
            if horizontal:
                plt.barh(ind - width / 2, x, width, color=color[0])
                plt.barh(ind + width / 2, y, width, color=color[1])
                plt.yticks(x_pos, names, size=self.size)
                plt.xticks(size=self.size)
            else:
                plt.bar(ind - width / 2, x, width, color=color[0])
                plt.bar(ind + width / 2, y, width, color=color[1])
                plt.xticks(x_pos, names, size=self.size)
                plt.yticks(size=self.size)
        else:
            if horizontal:
                plt.barh(x_pos, x, color=color[0])
                plt.yticks(x_pos, names, size=self.size)
                plt.xticks(size=self.size)
            else:
                plt.bar(x_pos, x, color=color[0])
                plt.xticks(x_pos, names, size=self.size)
                plt.yticks(size=self.size)
        return self


class DistributionPlot:
    """
    Requirements:
        - Input must be a list
        - For categorical data, we need a list of strings -> bar chart
        - For numerical data, we need a list of numerical values -> histogram

    Example categorical data:
        1. data = ["apple", "orange", "apple", "lemon", "lemon", "strawberry", "strawberry", "strawberry"]
        2. plot = DistributionPlot(data)
        3. plot.bar_graph()

    Example numerical data:
        1. data = [1, 1, 1, 1, 14, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 2, 2, 25, 5, 6, 7]
        2. fieldname = "Top 40 Position"
        3. plot = DistributionPlot(x=data, fieldname=fieldname)
        4. plot.histogram()
    """

    def __init__(
        self,
        x: list[Any],
        fieldname: str | None = None,
        grid: bool = False,
    ):
        self.x = x
        self.fieldname = fieldname
        self.grid = grid
        self.name_figure = "figure"
        self.xtick_labels = self.ytick_labels = None

        # Visualization defaults
        self.color_bars = "#20b2aa"
        self.color_baredges = "black"

        # Initialize the figure
        self.fig = self.ax = plt.subplots()
        plt.gca().spines["right"].set_visible(False)
        plt.gca().spines["top"].set_visible(False)
        plt.grid(self.grid)

    def histogram(
        self,
        color_bars: str | None = None,
        color_baredges: str | None = None,
    ) -> dict[str, Any]:
        # Plot the histogram
        plt.hist(
            self.x,
            density=False,
            facecolor=color_bars or self.color_bars,
            edgecolor=color_baredges or self.color_baredges,
        )

        # Plot the labels
        plt.title("Histogram")
        plt.xlabel(f"{self.fieldname}")
        plt.ylabel("Count")

        # Get the ticks
        xticks, self.xtick_labels = plt.xticks()
        yticks, self.ytick_labels = plt.yticks()
        ticks = {"x": xticks, "y": yticks}

        # Show the figure
        plt.tight_layout()
        plt.show()

        return ticks

    def bar_chart(
        self,
        color_bars: str | None = None,
        color_baredges: str | None = None,
    ) -> dict[str, Any]:
        # Prepare the data for the bar chart
        keys, counts = np.unique(self.x, return_counts=True)
        total_counts = sum(counts)

        # Plot the bar chart
        plt.bar(
            keys,
            counts,
            color=color_bars or self.color_bars,
            edgecolor=color_baredges or self.color_baredges,
        )

        # Plot the ticks
        plt.xticks(keys)

        # Plot the percentages
        for index, data in enumerate(counts):
            plt.text(
                x=index,
                y=data,
                s=f"{data / total_counts * 100:.2f}%",
                fontdict={"fontsize": 10},
            )

        # Plot the labels
        plt.title("Bar Chart")
        plt.xlabel(f"{self.fieldname}")
        plt.ylabel("Count")

        # Get the ticks
        xticks, self.xtick_labels = plt.xticks()
        yticks, self.ytick_labels = plt.yticks()
        ticks = {"x": xticks, "y": yticks}

        # Show the figure
        plt.tight_layout()
        plt.show()

        return ticks

    def save(self, name_figure: str | None = None) -> None:
        if not name_figure:
            name_figure = self.name_figure

        plt.savefig(fname=name_figure, bbox_inches="tight", format="png")


class RadarPlot:
    """
    Requirements:
        1. The values must be integers
        2. The maximum value for each field

    Example:
        data = [400000, 10, 10, 4, 6, 7, 8]
        maximum = [500000, 10, 20, 5, 10, 15, 10]
        fieldnames = ["1", "2", "3", "4", "5", "6", "7"]

        plot = RadarPlot(row=data, fieldnames=fieldnames, maximum=maximum)
        plot.create_grid()
        plot.set_ticks()
        plot.add_plot()
        plot.add_labels()
    """

    def __init__(
        self,
        row: list[Any],
        fieldnames: list[str],
        maximum: list[int],
    ):
        self.row = np.array(row)
        self.fieldnames = fieldnames
        self.maximum = maximum
        self.axes: plt.Axes | None = None
        self.fig: plt.Figure | None = None
        self.name_figure = "figure"

        # Initialize layout of the figure
        # Size and colors of the background and grid
        self.size_figure = (10, 10)
        self.color_background = "white"
        self.color_grid = self.color_axes = "#C94397"
        self.linewidth_grid = self.linewidth_axes = 1
        self.linestyle_grid = "-"

        # Ticks
        self.limit = 5

        # Sizes and colors of the figure
        self.color_polygon = "#20b2aa"
        self.alpha_polygon = 0.5
        self.linewidth_polygon = self.linewidth_points = 2
        self.color_points = "white"
        self.edgecolor_points = "black"
        self.size_points = 50
        self.size_ticklabels = 10
        self.size_fieldnamelabels = 14

    def create_grid(
        self,
        size_figure: tuple[Any, Any] | None = None,
        color_background: str | None = None,
        color_grid: str | None = None,
        color_axes: str | None = None,
        linewidth_grid: int | None = None,
        linestyle_grid: str | None = None,
        linewidth_axes: int | None = None,
    ) -> None:
        # Choose some nice colors for the grid
        rc(
            "axes",
            fc=color_background or self.color_background,
            ec=color_axes or self.color_axes,
            lw=linewidth_axes or self.linewidth_axes,
        )
        rc(
            "grid",
            c=color_grid or self.color_grid,
            lw=linewidth_grid or self.linewidth_grid,
            ls=linestyle_grid or self.linestyle_grid,
        )

        # Make figure background the same colors as axes
        self.fig = plt.figure(
            figsize=size_figure or self.size_figure,
            facecolor=color_background or self.color_background,
        )

        # Use a polar axes
        self.axes = plt.subplot(111, polar=True)

        # Set axes limits
        plt.ylim(0, 5)

    def set_ticks(self, lim: int | None = None) -> tuple[NDArray[Any], int]:
        # Set y ticks from lima to limb
        plt.yticks(np.linspace(1, lim or self.limit, lim or self.limit), [])
        t = np.arange(0, 2 * np.pi, 2 * np.pi / len(self.fieldnames))

        # Set ticks to the number of categories (in radians)
        plt.xticks(t, [])

        return t, lim or self.limit

    def add_plot(
        self,
        color_polygon: str | None = None,
        linewidth_polygon: int | None = None,
        alpha_polygon: int | None = None,
        size_points: int | None = None,
        linewidth_points: int | None = None,
        color_points: str | None = None,
        edgecolor_points: str | None = None,
    ) -> NDArray[Any]:
        axes_values = np.array(
            [
                self.row[i] / self.maximum[i] * self.set_ticks()[1]
                for i in range(len(self.fieldnames))
            ]
        )

        # Draw polygon representing values
        points = [(x, y) for x, y in zip(self.set_ticks()[0], axes_values)]
        points.append(points[0])
        point_array = np.array(points)
        codes = (
            [path.Path.MOVETO]
            + [path.Path.LINETO] * (len(axes_values) - 1)
            + [path.Path.CLOSEPOLY]
        )

        _path = path.Path(point_array, codes)  # noqa
        _patch = patches.PathPatch(
            _path,
            fill=True,
            color=color_polygon or self.color_polygon,
            linewidth=linewidth_polygon or self.linewidth_polygon,
            alpha=alpha_polygon or self.alpha_polygon,
        )
        assert isinstance(self.axes, plt.Axes)
        self.axes.add_patch(_patch)
        _patch = patches.PathPatch(
            _path, fill=False, linewidth=linewidth_polygon or self.linewidth_polygon
        )
        self.axes.add_patch(_patch)

        # Draw circles at value points
        plt.scatter(
            point_array[:, 0],
            point_array[:, 1],
            linewidth=linewidth_points or self.linewidth_points,
            s=size_points or self.size_points,
            facecolor=color_points or self.color_points,
            edgecolor=edgecolor_points or self.edgecolor_points,
            zorder=2,
        )
        return point_array

    def add_labels(
        self,
        size_fieldnamelabels: int | None = None,
        size_ticklabels: int | None = None,
    ) -> None:
        for index, value in enumerate(self.fieldnames):
            # Call (the position of) the points
            points = self.add_plot()

            # Draw fieldname labels
            angle_rad = index / float(len(self.fieldnames)) * 2 * np.pi

            # Set the fieldname labels position
            if angle_rad < np.pi / 2 or angle_rad > 3 * np.pi / 2:
                ha = "left"
            else:
                ha = "right"

            # Draw the fieldname labels
            plt.text(
                angle_rad,
                5.5,
                value,
                size=size_fieldnamelabels or self.size_fieldnamelabels,
                horizontalalignment=ha,
                verticalalignment="center",
            )

            # Draw tick labels on the y axes
            plt.text(
                angle_rad,
                points[:, 1][index] + 0.2,
                f"{self.row[index]:.2f}",
                size=size_ticklabels or self.size_ticklabels,
                horizontalalignment=ha,
                verticalalignment="center",
            )

    def save(self, name_figure: str | None = None) -> None:
        if not name_figure:
            name_figure = self.name_figure

        plt.savefig(fname=name_figure, bbox_inches="tight", format="png")


def plot_stacked_bar(
    data: NDArray[Any] | list[list[Any]],
    series_labels: list[str],
    *,
    category_labels: list[str] | None = None,
    show_values: bool = False,
    value_format: str = "{}",
    y_label: str | None = None,
    colors: list[str] | None = None,
    grid: bool = True,
    reverse: bool = False,
) -> None:
    """Plots a stacked bar chart with the data and labels provided.

    Arguments:
        data            -- 2-dimensional numpy array or nested list
                           containing data for each series in rows
        series_labels   -- list of series labels (these appear in
                           the legend)
        category_labels -- list of category labels (these appear
                           on the x-axis)
        show_values     -- If True then numeric value labels will
                           be shown on each bar
        value_format    -- Format string for numeric value labels
                           (default is "{}")
        y_label         -- Label for y-axis (str)
        colors          -- List of color labels
        grid            -- If True display grid
        reverse         -- If True reverse the order that the
                           series are displayed (left-to-right
                           or right-to-left)

    Example:
        plt.figure(figsize=(6, 4))

        series_labels = ['Series 1', 'Series 2']

        data = [
            [0.2, 0.3, 0.35, 0.3],
            [0.8, 0.7, 0.6, 0.5]
        ]

        category_labels = ['Cat A', 'Cat B', 'Cat C', 'Cat D']

        plot_stacked_bar(
            data,
            series_labels,
            category_labels=category_labels,
            show_values=True,
            value_format="{:.1f}",
            colors=['tab:orange', 'tab:green'],
            y_label="Quantity (units)"
        )

        plt.savefig('bar.png')
        plt.show()
    """

    ny = len(data[0])
    ind = list(range(ny))

    axes = []
    cum_size = np.zeros(ny)

    data = np.array(data)

    if reverse:
        data = np.flip(data, axis=1)
        assert isinstance(category_labels, list)
        category_labels.reverse()

    if colors:
        for i, row_data in enumerate(data):
            axes.append(
                plt.bar(
                    ind,
                    row_data,
                    bottom=cum_size,
                    label=series_labels[i],
                    color=colors[i],
                )
            )
            cum_size += row_data
    else:
        for i, row_data in enumerate(data):
            axes.append(plt.bar(ind, row_data, bottom=cum_size, label=series_labels[i]))
            cum_size += row_data

    if category_labels:
        plt.xticks(ind, category_labels)

    if y_label:
        plt.ylabel(y_label)

    plt.legend()

    if grid:
        plt.grid()

    if show_values:
        for axis in axes:
            for bar in axis:
                w, h = bar.get_width(), bar.get_height()
                plt.text(
                    bar.get_x() + w / 2,
                    bar.get_y() + h / 2,
                    value_format.format(h),
                    ha="center",
                    va="center",
                )
