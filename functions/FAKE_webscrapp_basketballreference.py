# https://medium.com/analytics-vidhya/intro-to-scraping-basketball-reference-data-8adcaa79664a

# Sports-Reference.com is precisely where sports fandom and data science converge. It’s a massive, structured warehouse of clean sports data. Thus, it’s often the starting blocks for academic data science projects.
# From a sports-reference site, like basketball-reference.com, it’s easy to grab one table. You don’t need to do it programmatically, you can copy and paste or even “export to CSV”. For example, you can get last season’s NBA standings from this page: https://www.basketball-reference.com/leagues/NBA_2020_standings.html

# But, that’s not much data. What if you want to aggregate data from multiple pages to draw meaningful conclusions about teams’ standings over 5, 10, or 50 years? (Or in my case, does tanking help a team reach the finals?) Well, you can do that with python and three libraries.
# To show this, I will outline two examples:
# Scraping one page
# Scraping many pages and aggregating the data
# simple example: scraping data from one page

# import libraries and define your URL:
# needed libraries



from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd

# URL to scrape
url = "https://www.basketball-reference.com/playoffs/"

# collect HTML data and create beautiful soup object:
# collect HTML data
html = urlopen(url)
        
# create beautiful soup object from HTML
soup = BeautifulSoup(html, features="lxml")

# extract column headers into a list:
# use getText()to extract the headers into a list
headers = [th.getText() for th in soup.findAll('tr', limit=2)[1].findAll('th')]

# extract rows from table:
# get rows from table
rows = soup.findAll('tr')[2:]
rows_data = [[td.getText() for td in rows[i].findAll('td')]
                    for i in range(len(rows))]
# if you print row_data here you'll see an empty row
# so, remove the empty row
rows_data.pop(20)
# for simplicity subset the data for only 39 seasons
rows_data = rows_data[0:38]

# add “years” as a column:
# we're missing a column for years
# add the years into rows_data

last_year = 2020
for i in range(0, len(rows_data)):
    rows_data[i].insert(0, last_year)
    last_year -=1

# lastly, create the dataframe and export to CSV:

# create the dataframe
nba_finals = pd.DataFrame(rows_data, columns = headers)

# export dataframe to a CSV 
nba_finals.to_csv("nba_finals_history.csv", index=False)

# complex example: scraping data from multiple pages
# create your looping function:

# import needed libraries
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd

# create a function to scrape team performance for multiple years
def scrape_NBA_team_data(years = [2017, 2018]):
    
    final_df = pd.DataFrame(columns = ["Year", "Team", "W", "L",
                                        "W/L%", "GB", "PS/G", "PA/G",
                                        "SRS", "Playoffs",
                                        "Losing_season"])
    
    # loop through each year
    for y in years:
        # NBA season to scrape
        year = y
        
        # URL to scrape, notice f string:
        url = f"https://www.basketball-reference.com/leagues/NBA_{year}_standings.html"
        
        # collect HTML data
        html = urlopen(url)
        
        # create beautiful soup object from HTML
        soup = BeautifulSoup(html, features="lxml")
        
        # use getText()to extract the headers into a list
        titles = [th.getText() for th in soup.findAll('tr', limit=2)[0].findAll('th')]
        
        # first, find only column headers
        headers = titles[1:titles.index("SRS")+1]
        
        # then, exclude first set of column headers (duplicated)
        titles = titles[titles.index("SRS")+1:]
        
        # next, row titles (ex: Boston Celtics, Toronto Raptors)
        try:
            row_titles = titles[0:titles.index("Eastern Conference")]
        except: row_titles = titles
        # remove the non-teams from this list
        for i in headers:
            row_titles.remove(i)
        row_titles.remove("Western Conference")
        divisions = ["Atlantic Division", "Central Division",
                    "Southeast Division", "Northwest Division",
                    "Pacific Division", "Southwest Division",
                    "Midwest Division"]

        for d in divisions:
            try:
                row_titles.remove(d)
            except:
                print("no division:", d)
        
        # next, grab all data from rows (avoid first row)
        rows = soup.findAll('tr')[1:]
        team_stats = [[td.getText() for td in rows[i].findAll('td')]
                    for i in range(len(rows))]
        # remove empty elements
        team_stats = [e for e in team_stats if e != []]
        # only keep needed rows
        team_stats = team_stats[0:len(row_titles)]
        
        # add team name to each row in team_stats
        for i in range(0, len(team_stats)):
            team_stats[i].insert(0, row_titles[i])
            team_stats[i].insert(0, year)
            
        # add team, year columns to headers
        headers.insert(0, "Team")
        headers.insert(0, "Year")
        
        # create a dataframe with all aquired info
        year_standings = pd.DataFrame(team_stats, columns = headers)
        
        # add a column to dataframe to indicate playoff appearance
        year_standings["Playoffs"] = ["Y" if "*" in ele else "N" for ele in year_standings["Team"]]
        # remove * from team names
        year_standings["Team"] = [ele.replace('*', '') for ele in year_standings["Team"]]
        # add losing season indicator (win % < .5)
        year_standings["Losing_season"] = ["Y" if float(ele) < .5 else "N" for ele in year_standings["W/L%"]]
        
        # append new dataframe to final_df
        final_df = final_df.append(year_standings)
        
    # print final_df
    print(final_df.info)
    # export to csv
    final_df.to_csv("nba_team_data.csv", index=False)

# Test it on the last 30 seasons!
scrape_NBA_team_data(years = [1990, 1991, 1992, 1993, 1994,
                            1995, 1996, 1997, 1998, 1999,
                            2000, 2001, 2002, 2003, 2004,
                            2005, 2006, 2007, 2008, 2009,
                            2010, 2011, 2012, 2013, 2014,
                            2015, 2016, 2017, 2018, 2019,
                            2020])