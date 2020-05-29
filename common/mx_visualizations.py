import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, LogNorm
import numpy as np
import seaborn as sns

mx_colors = {'dark_green': "#0E5C59", 'green': "#05AB89", 'light_green': "#9AD2B1",
             'blue_green': '#52BBB5', 'light_purple': '#C975AA', 'purple': "#C94397"}


class Plotting_mx:

    def __init__(self):
        self.mx_colors = {'dark_green': "#0E5C59", 'green': "#05AB89", 'light_green': "#9AD2B1",
                          'blue_green': '#52BBB5', 'light_purple': '#C975AA', 'purple': "#C94397"}
        self.mx_cmap = ListedColormap(sns.color_palette(self.mx_colors.values()).as_hex())
        plt.style.use('seaborn')

    """
    With the makefig function you can pre-set the figure with its figure size, labels, grid, sizes, and title name.
    """

    def makefig(self, xlabel='No label', ylabel='No label', dpi=50, figsize=(8, 8), grid=True, size=16, title=''):

        self.size = size
        plt.figure(figsize=figsize, dpi=dpi)
        plt.grid(grid)
        plt.xticks(size=self.size)
        plt.yticks(size=self.size)
        plt.xlabel(xlabel, size=self.size)
        plt.ylabel(ylabel, size=self.size)
        plt.title(title, size=self.size + 2)

        return self

    """
    With the histogram function you can call a histogram after calling the makefig function.
    x       ->  Insert your x-axis data
    y       ->  Insert your y-axis data
    TwoD    ->  You can choose for a TwoD plot where you can make a histogram density.
    color   ->  color can be changed but is by default matrixian dark green
    bins    ->  by default set on 20
    """

    def histogram(self, x=None, y=None,  TwoD=None, bins=20, color=mx_colors['dark_green']):
        if TwoD:
            plt.hist2d(x, y, norm=LogNorm(), bins=bins, cmap=self.mx_cmap)
            cbar = plt.colorbar(orientation="horizontal", pad=0.15)
            cbar.ax.set_ylabel('Counts', size=self.size)
        else:
            plt.hist(x, color=color, bins=bins)

        return self

    """
    With this spaghetti function you can call a line plot
    x       ->  Insert your x-axis data
    y       ->  Insert your y-axis data
    color   ->  color can be changed but is by default matrixian dark green
    """

    def spaghetti(self, x=None, y=None, color=mx_colors['dark_green']):
        plt.plot(x, y, color=color)
        return self

    """
    With the bar function you can call a barplot
    x           ->  Insert your x-axis data
    y           ->  Insert your y-axis data
    names       ->  Names of the bars
    horizontal  ->  True or False gives you the option to do a horizontal or vertical bar plot
    width       ->  You can change the width of the bars. This is by default 0.25
    color       ->  You can choose the colors in list (one or two colors) or as string (one color). By default these are matrixian purple and matrixian green
    """

    def bar(self, x=None, y=None, names=[], horizontal=False, width=0.25, color=[mx_colors['purple'], mx_colors['green']]):

        # To-do: add annotation

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

    def timeseries(self):
        pass


""""
Example 1:
y = np.random.randn(100)
x = np.linspace(1,100,len(y))

mx_plot = Plotting_mx()
mx_plot.makefig(figsize = (8,8), grid = True, xlabel = 'ex-axis', ylabel = 'why-axis', title = 'title')
mx_plot.spaghetti(np.linspace(1,100,len(y)) , y)
mx_plot.spaghetti(np.linspace(1,10,len(y)) , y, color = 'green')
plt.show()
"""

"""
Example 2:
n = 100000
x = np.random.randn(n)
y = x + np.random.randn(n)

mx_plot = Plotting_mx()
mx_plot.makefig(figsize = (8,8), grid = True, xlabel = 'ex-axis', ylabel = 'why-axis', title = 'title')
mx_plot.histogram(TwoD = True, 
                  x = x, 
                  y = y)
"""

"""
Example 3:
mx_plot = Plotting_mx()
mx_plot.makefig()
mx_plot.bar(names = ['Nuclear', 'Hydro', 'Gas', 'Oil', 'Coal', 'Biofuel'], 
            x = [5, 6, 15, 22, 24, 8],
            y = [1,2,3,4,5,6], 
            horizontal=True)
"""