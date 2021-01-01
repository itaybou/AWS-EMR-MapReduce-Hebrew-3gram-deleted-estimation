from os.path import join, splitext
from glob import glob
import seaborn as sns
import matplotlib.pyplot as plt
import re
import pandas as pd

jobs = ['Split Corpus', 'Aggregate Nr', 'Aggregate Tr', 'Join Nr Tr 3grams', 'Calculate deleted estimation', 'Sort deleted estimation']
statistics = ['Map input records', 'Map output records', 'Combine input records', 'Combine output records', 'Reduce input records', 'Reduce output records']
counters = ['NGRAMS_COUNTER']

input_files = glob(join('.', 'syslog*'))

stats_dict = {}

for file in input_files:
   job_index = 0
   ext = splitext(file)[0].split('-', 1)[1].capitalize()
   with open(file, 'r') as stats_file:
      lines = stats_file.readlines()
      for line in lines:
         if any(line.strip().startswith(stats_line) for stats_line in statistics):
            desc = f"{re.findall(r'[a-zA-Z ]+', line)[0]}"
            value = int(re.findall(r'\d+', line)[0])
            stats_dict[(ext, desc, jobs[job_index])] = value
            print((desc, value, jobs[job_index]))
            if len(stats_dict) % len(jobs) == 0:
               job_index = (job_index + 1) % len(jobs)

df = pd.Series(stats_dict).reset_index()
df.columns = ['Status', 'Statistic', 'Stage', 'Value']

def display_figures(ax):
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
   plt.show()

combiner_stats_df = df.loc[df['Status'] == 'Combiner']
print(combiner_stats_df)
plt.figure(figsize=(40,25))
ax = sns.barplot(x=combiner_stats_df.Value, y=combiner_stats_df.Stage, hue=combiner_stats_df.Statistic, data=combiner_stats_df, orient='h')

display_figures(ax)


