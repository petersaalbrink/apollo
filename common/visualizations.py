import numpy as np
from typing import List

# Visualizations
from matplotlib import rc
import matplotlib.pyplot as plt
import matplotlib.path as path
import matplotlib.patches as patches


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
