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

from typing import List
import numpy as np
from matplotlib import rc
import matplotlib.path as path
import matplotlib.patches as patches
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, LogNorm
import seaborn as sns

mx_colors = {'dark_green': "#0E5C59", 'green': "#05AB89", 'light_green': "#9AD2B1",
             'blue_green': '#52BBB5', 'light_purple': '#C975AA', 'purple': "#C94397"}
mx_cmap = ListedColormap(sns.color_palette(mx_colors.values()).as_hex())
plt.style.use('seaborn')


class PlotMx:
    """Make plots, Matrixian style!"""
    def __init__(self):
        self.size = None

    def makefig(self, xlabel='No label', ylabel='No label', dpi=50, figsize=(8, 8), grid=True, size=16, title=''):
        """Pre-set the figure with its figure size, labels, grid, sizes, and title name."""

        self.size = size
        plt.figure(figsize=figsize, dpi=dpi)
        plt.grid(grid)
        plt.xticks(size=self.size)
        plt.yticks(size=self.size)
        plt.xlabel(xlabel, size=self.size)
        plt.ylabel(ylabel, size=self.size)
        plt.title(title, size=self.size + 2)

        return self

    def histogram(self, x=None, y=None, TwoD=None, bins=20, color=mx_colors['dark_green']):
        """Make a histogram.

        Make sure to call the makefig method first.

        x       ->  Insert your x-axis data
        y       ->  Insert your y-axis data
        TwoD    ->  You can choose for a TwoD plot where you can make a histogram density.
        color   ->  color can be changed but is by default matrixian dark green
        bins    ->  by default set on 20
        """
        if TwoD:
            plt.hist2d(x, y, norm=LogNorm(), bins=bins, cmap=mx_cmap)
            cbar = plt.colorbar(orientation="horizontal", pad=0.15)
            cbar.ax.set_ylabel('Counts', size=self.size)
        else:
            plt.hist(x, color=color, bins=bins)

        return self

    def spaghetti(self, x=None, y=None, color=mx_colors['dark_green']):
        """Make a line plot.

        x       ->  Insert your x-axis data
        y       ->  Insert your y-axis data
        color   ->  color can be changed but is by default matrixian dark green
        """
        plt.plot(x, y, color=color)
        return self

    def bar(self, x=None, y=None, names=[], horizontal=False, width=0.25, color=None):
        """Make a barplot.

        x           ->  Insert your x-axis data
        y           ->  Insert your y-axis data
        names       ->  Names of the bars
        horizontal  ->  True or False gives you the option to do a horizontal or vertical bar plot
        width       ->  You can change the width of the bars. This is by default 0.25
        color       ->  You can choose the colors in list (one or two colors) or as string (one color).
                        By default these are matrixian purple and matrixian green
        """

        # To-do: add annotation

        if not color:
            color = [mx_colors['purple'], mx_colors['green']]
        if type(color) != list:
            color = [color]
        if len(color) == 1:
            color = [color] * 2

        x_pos = [i for i, _ in enumerate(names)]
        if y:
            if horizontal:
                ind = np.arange(len(x))  # the x locations for the groups
                plt.barh(ind - width / 2, x, width, color=color[0])
                plt.barh(ind + width / 2, y, width, color=color[1])
                plt.yticks(x_pos, names, size=self.size)
                plt.xticks(size=self.size)
            else:
                ind = np.arange(len(x))  # the x locations for the groups
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

    def __init__(self, x: list = None, fieldname: str = None, grid: bool = None):
        self.x = x
        self.fieldname = fieldname
        self.grid = grid
        self.name_figure = "figure"
        self.xtick_labels = self.ytick_labels = None

        # Visualization defaults
        self.color_bars = '#20b2aa'
        self.color_baredges = "black"

        # Initialize the figure
        self.fig = self.ax = plt.subplots()
        plt.gca().spines['right'].set_visible(False)
        plt.gca().spines['top'].set_visible(False)
        plt.grid(self.grid)

    def histogram(self, color_bars: str = None, color_baredges: str = None) -> dict:
        # Set the layout
        layout_dict = {
            "color_bars": (color_bars, self.color_bars),
            "color_baredges": (color_baredges, self.color_baredges)
        }

        layout_dict = {key: item[0] if item[0] else item[1] for key, item in layout_dict.items()}

        # Plot the histogram
        plt.hist(self.x, density=False, facecolor=layout_dict["color_bars"],
                 edgecolor=layout_dict["color_baredges"])

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

    def bar_chart(self, color_bars: str = None, color_baredges: str = None) -> dict:
        # Set the layout
        layout_dict = {
            "color_bars": (color_bars, self.color_bars),
            "color_baredges": (color_baredges, self.color_baredges)
        }

        layout_dict = {key: item[0] if item[0] else item[1] for key, item in layout_dict.items()}

        # Prepare the data for the bar chart
        keys, counts = np.unique(self.x, return_counts=True)
        total_counts = sum(counts)

        # Plot the bar chart
        plt.bar(keys, counts, color=layout_dict["color_bars"], edgecolor=layout_dict["color_baredges"])

        # Plot the ticks
        plt.xticks(keys)

        # Plot the percentages
        for index, data in enumerate(counts):
            plt.text(x=index, y=data, s=f"{data / total_counts * 100:.2f}%", fontdict=dict(fontsize=10))

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

    def save(self, name_figure: str = None):
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

    def __init__(self, row: list = None, fieldnames: List[str] = None, maximum: List[int] = None):
        self.row = np.array(row)
        self.fieldnames = fieldnames
        self.maximum = maximum
        self.fig = self.axes = None
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

    def create_grid(self, size_figure: tuple = None, color_background: str = None, color_grid: str = None,
                    color_axes: str = None, linewidth_grid: int = None, linestyle_grid: str = None,
                    linewidth_axes: int = None):
        layout_dict = {
            "size_figure": (size_figure, self.size_figure),
            "color_background": (color_background, self.color_background),
            "color_grid": (color_grid, self.color_grid),
            "color_axes": (color_axes, self.color_axes),
            "linewidth_grid": (linewidth_grid, self.linewidth_grid),
            "linestyle_grid": (linestyle_grid, self.linestyle_grid),
            "linewidth_axes": (linewidth_axes, self.linewidth_axes)
        }

        layout_dict = {key: item[0] if item[0] else item[1] for key, item in layout_dict.items()}

        # Choose some nice colors for the grid
        rc('axes', fc=layout_dict["color_background"], ec=layout_dict["color_axes"], lw=layout_dict["linewidth_axes"])
        rc('grid', c=layout_dict["color_grid"], lw=layout_dict["linewidth_grid"], ls=layout_dict["linestyle_grid"])

        # Make figure background the same colors as axes
        self.fig = plt.figure(figsize=layout_dict["size_figure"], facecolor=layout_dict["color_background"])

        # Use a polar axes
        self.axes = plt.subplot(111, polar=True)

        # Set axes limits
        plt.ylim(0, 5)

    def set_ticks(self, lim: int = None):
        layout_dict = {
            "limit": (lim, self.limit)
        }

        layout_dict = {key: item[0] if item[0] else item[1] for key, item in layout_dict.items()}

        # Set y ticks from lima to limb
        plt.yticks(np.linspace(1, layout_dict["limit"], layout_dict["limit"]), [])
        t = np.arange(0, 2 * np.pi, 2 * np.pi / len(self.fieldnames))

        # Set ticks to the number of categories (in radians)
        plt.xticks(t, [])

        return [t, layout_dict["limit"]]

    def add_plot(self, color_polygon: str = None, linewidth_polygon: int = None, alpha_polygon: int = None,
                 size_points: int = None, linewidth_points: int = None, color_points: str = None,
                 edgecolor_points: str = None):

        layout_dict = {
            "color_polygon": (color_polygon, self.color_polygon),
            "alpha_polygon": (alpha_polygon, self.alpha_polygon),
            "linewidth_polygon": (linewidth_polygon, self.linewidth_polygon),
            "color_points": (color_points, self.color_points),
            "edgecolor_points": (edgecolor_points, self.edgecolor_points),
            "linewidth_points": (linewidth_points, self.linewidth_points),
            "size_points": (size_points, self.size_points)
        }

        layout_dict = {key: item[0] if item[0] else item[1] for key, item in layout_dict.items()}

        axes_values = np.array(
            [self.row[i] / self.maximum[i] * self.set_ticks()[1] for i in range(len(self.fieldnames))]
        )

        # Draw polygon representing values
        points = [(x, y) for x, y in zip(self.set_ticks()[0], axes_values)]
        points.append(points[0])
        points = np.array(points)
        codes = [path.Path.MOVETO, ] + \
                [path.Path.LINETO, ] * (len(axes_values) - 1) + \
                [path.Path.CLOSEPOLY]

        _path = path.Path(points, codes)
        _patch = patches.PathPatch(_path, fill=True, color=layout_dict["color_polygon"],
                                   linewidth=layout_dict["linewidth_polygon"], alpha=layout_dict["alpha_polygon"])
        self.axes.add_patch(_patch)
        _patch = patches.PathPatch(_path, fill=False, linewidth=layout_dict["linewidth_polygon"])
        self.axes.add_patch(_patch)

        # Draw circles at value points
        plt.scatter(
            points[:, 0],
            points[:, 1],
            linewidth=layout_dict["linewidth_points"],
            s=layout_dict["size_points"],
            facecolor=layout_dict["color_points"],
            edgecolor=layout_dict["edgecolor_points"],
            zorder=2
        )
        return points

    def add_labels(self, size_fieldnamelabels: int = None, size_ticklabels: int = None):

        layout_dict = {
            "size_fieldnamelabels": (size_fieldnamelabels, self.size_fieldnamelabels),
            "size_ticklabels": (size_ticklabels, self.size_ticklabels)
        }

        layout_dict = {key: item[0] if item[0] else item[1] for key, item in layout_dict.items()}

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
            plt.text(angle_rad, 5.5, value, size=layout_dict["size_fieldnamelabels"], horizontalalignment=ha,
                     verticalalignment="center")

            # Draw tick labels on the y axes
            plt.text(angle_rad, points[:, 1][index] + 0.2, f"{self.row[index]:.2f}",
                     size=layout_dict["size_ticklabels"], horizontalalignment=ha, verticalalignment="center")

    def save(self, name_figure: str = None):
        if not name_figure:
            name_figure = self.name_figure

        plt.savefig(fname=name_figure, bbox_inches="tight", format="png")


def plot_stacked_bar(data, series_labels, *, category_labels=None,
                     show_values=False, value_format="{}", y_label=None,
                     colors=None, grid=True, reverse=False):
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
        category_labels = reversed(category_labels)

    if colors:
        for i, row_data in enumerate(data):
            axes.append(plt.bar(ind, row_data, bottom=cum_size,
                                label=series_labels[i], color=colors[i]))
            cum_size += row_data
    else:
        for i, row_data in enumerate(data):
            axes.append(plt.bar(ind, row_data, bottom=cum_size,
                                label=series_labels[i]))
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
                plt.text(bar.get_x() + w / 2, bar.get_y() + h / 2,
                         value_format.format(h), ha="center",
                         va="center")
