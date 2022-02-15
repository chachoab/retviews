#%%
import pandas as pd
import seaborn as sns
import os

os.chdir(os.path.join(os.path.dirname(__file__), os.pardir))
df = pd.read_csv('data\output\kpi_all_weeks.csv')
df = df[df['area'].str.contains('TOPOS')]

# %% Rejections by area
sns.set(font_scale = 2)
sum_by_area = df.groupby(['week', 'area']).sum().reset_index()
row_order = ['TOPOS2', 'TOPOS3', 'TOPOS4']
g = sns.FacetGrid(sum_by_area, row="area", height=4, aspect=5, row_order=row_order)
g.map(sns.barplot, "week", "rejections", ci=None)

# %% Weekly on time delivery rate
sum_by_area['otd'] = sum_by_area['delivered']/sum_by_area['scheduled']
g = sns.FacetGrid(sum_by_area, row="area", height=4, aspect=5, row_order=row_order)
g.map(sns.barplot, "week", "otd", ci=None)
g.set(ylim=(0, 1))

# %% Valid comment rate
sum_by_area['comment_rate'] = sum_by_area['comment_valid']/sum_by_area['total_lines']
g = sns.FacetGrid(sum_by_area, row="area", height=4, aspect=5, row_order=row_order)
g.map(sns.barplot, "week", "comment_rate", ci=None)
g.set(ylim=(0, 1))
