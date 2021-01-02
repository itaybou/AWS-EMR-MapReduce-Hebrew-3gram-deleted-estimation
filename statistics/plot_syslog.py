from os.path import join, splitext
from glob import glob
import seaborn as sns
import matplotlib.pyplot as plt
import re
import pandas as pd

jobs = ['Split Corpus', 'Aggregate Nr', 'Aggregate Tr', 'Join Nr Tr 3grams', 'Calculate deleted estimation', 'Sort deleted estimation']
statistics = ['Map input records', 'Map output records', 'Combine input records', 'Combine output records', 'Reduce input records', 'Reduce output records']
byte_statistics = ['Map output bytes', 'Reduce shuffle bytes', 'Bytes Read', 'Bytes Written']
all_statistics = statistics + byte_statistics
counter = 'NGRAMS_COUNTER'

input_files = glob(join('.', 'syslog*'))

stats_dict = {}
counter_value = 0

for file in input_files:
   job_index = 0
   ext = splitext(file)[0].split('-', 1)[1].capitalize()
   with open(file, 'r') as stats_file:
      lines = stats_file.readlines()
      for line in lines:
         if line.strip().startswith(counter):
            counter_value = int(re.findall(r'\d+', line)[0])
         if any(line.strip().startswith(stats_line) for stats_line in all_statistics):
            desc = f"{re.findall(r'[a-zA-Z ]+', line)[0]}"
            value = int(re.findall(r'\d+', line)[0])
            stats_dict[(ext, desc, jobs[job_index])] = value
            if len(stats_dict) % len(all_statistics) == 0:
               job_index = (job_index + 1) % len(jobs)

df = pd.Series(stats_dict).reset_index()
df.columns = ['Status', 'Statistic', 'Stage', 'Value']

def display_figures(ax, statistics, title):
   stats_index = 0
   for i, p in enumerate(ax.patches):
      width = p.get_width()    # get bar length
      ax.text(width + 1,       # set the text at 1 unit right of the bar
               p.get_y() + p.get_height() / 2, # get Y coordinate + X coordinate / 2
               f'{statistics[stats_index]}: {int(width)}', # set variable to display, 2 decimals
               ha = 'left',   # horizontal alignment
               va = 'center')  # vertical alignment
      if (i + 1) % len(statistics) == 0:
         stats_index = (stats_index + 1) % len(statistics)
   ax.set_title(title)
   plt.show()

def plot_in_out_statistics(value):
   stats_df = df.loc[(df['Status'] == value) & (df['Statistic'].isin(statistics))]
   plt.figure(figsize=(40,25))
   ax = sns.barplot(x=stats_df.Value, y=stats_df.Stage, hue=stats_df.Statistic, data=stats_df, orient='h')
   display_figures(ax, statistics=statistics, title=f'Input Output with {value} Statistics')
   print(stats_df.reset_index(drop=True).to_markdown(tablefmt='github'))

def plot_bytes_statistics(value):
   stats_df = df.loc[(df['Status'] == value) & (df['Statistic'].isin(byte_statistics))]
   plt.figure(figsize=(40,25))
   ax = sns.barplot(x=stats_df.Value, y=stats_df.Stage, hue=stats_df.Statistic, data=stats_df, orient='h')
   display_figures(ax, statistics=byte_statistics, title=f'Byte Statistics with {value}')
   print(stats_df.reset_index(drop=True).to_markdown(tablefmt='github'))

print(f'Total ngrams read from corpus: {counter_value}')
plot_in_out_statistics(value='Combiner')
plot_in_out_statistics(value='No_combiner')

plot_bytes_statistics(value='Combiner')
plot_bytes_statistics(value='No_combiner')