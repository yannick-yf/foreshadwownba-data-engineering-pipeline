
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import requests
import sys
import numpy as np
from urllib.error import HTTPError

def webscrappe_nba_list_players_and_attributes_data(SEASON):
    
    #------------------------------------------------------
    # Get team reference data 

    team_city_refdata = pd.read_csv('./constants/team_city_refdata.csv', sep = ';')

    #------------------------------------------------------
    # Initialization of the dataframe to fill-in
    players = pd.DataFrame()

    #------------------------------------------------------
    # For Loop Throught all the team abrev for the given season

    for index, row in team_city_refdata.iterrows():
        # URL to scrape
        team = row['team_abrev']
        url = f"https://www.basketball-reference.com/teams/{team}/{SEASON}.html"
        # url = f"https://www.basketball-reference.com/teams/BRK/2001.html"
        print(url)

        # collect HTML data and create beautiful soup object:
        # collect HTML data
        try:
            html = urlopen(url)
            
            # create beautiful soup object from HTML
            soup = BeautifulSoup(html, "html.parser" )

            # rows = soup.findAll('tr')[2:]
            rows = soup.findAll('table')[0]
            rows = rows.find_all('tr')

            rows_data = [[td.getText() for td in rows[i].findAll('td')]
                                for i in range(len(rows))]

            if len(rows_data) != 0:
                # create the dataframe
                players_tmp = pd.DataFrame(rows_data)

                cols = [
                    'Name', 
                    'Position', 
                    'Ht', 
                    'Wt', 
                    'BirthDate',
                    'Nationality',
                    'Experience', 
                    'College']

                players_tmp.columns =  cols
                players_tmp = players_tmp.dropna()
                players_tmp['id_season'] = SEASON
                players_tmp['tm'] = team

                players = players.append(players_tmp)

        except HTTPError as http_error:
            continue

    players = players[[
        'id_season', 
        'tm',
        'Name', 
        'Position', 
        'Ht', 
        'Wt', 
        'BirthDate',
        'Nationality',
        'Experience', 
        'College']]

    return players

#------------------------------------------------------------------------------------------------------------

def webscrappe_nba_schedule_overtime_data(SEASON):
    
    #------------------------------------------------------
    # Get team reference data 

    team_city_refdata = pd.read_csv('./constants/team_city_refdata.csv', sep = ';')

    #------------------------------------------------------
    # Initialization of the dataframe to fill-in
    schedules = pd.DataFrame()

    #------------------------------------------------------
    # For Loop Throught all the team abrev for the given season

    for index, row in team_city_refdata.iterrows():
        
        # URL to scrape
        team = row['team_abrev']

        # URL to scrape
        url = f"https://www.basketball-reference.com/teams/{team}/{SEASON}_games.html"

        if str(requests.get(url)) != '<Response [404]>':

            # collect HTML data and create beautiful soup object:
            # collect HTML data
            html = urlopen(url)
                    
            # create beautiful soup object from HTML
            soup = BeautifulSoup(html,  "html.parser")

            # use getText()to extract the headers into a list
            titles = [th.getText() for th in soup.findAll('tr', limit=2)[1].findAll('th')]

            rows = soup.findAll('tr')[1:]

            rows_data = [[td.getText() for td in rows[i].findAll('td')]
                            for i in range(len(rows))]

            if len(rows_data) != 0:
                # create the dataframe
                schedule_tmp = pd.DataFrame(rows_data)

                cols = ['game_date', 'time_start', 'nc1', 'nc2', 'extdom', 'opponent', 'w_l', 'overtime', 'pts_tm', 'pts_opp', 'w_tot', 'l_tot', 'streak_w_l', 'nc3']
                schedule_tmp.columns =  cols

                schedule_tmp = schedule_tmp.dropna()

                schedule_tmp['id_season'] = SEASON
                schedule_tmp['tm'] = team

                schedules = schedules.append(schedule_tmp)

    schedules = schedules[['id_season', 'tm', 'game_date', 'time_start', 'extdom', 'opponent', 'w_l', 'overtime', 'pts_tm', 'pts_opp', 'w_tot', 'l_tot', 'streak_w_l']]
            
    return schedules




# ### draft: 
# #----------------------------------------------------------------------------------------------------------------
# #
# # LOAD PACKAGE
# #
# #----------------------------------------------------------------------------------------------------------------

# # install.packages("rvest")
# library(rvest)
# library(zoo)
# library(ggmap)
# library(data.table)
# library(dplyr)

# #----------------------------------------------------------------------------------------------------------------
# #
# # DELETE ALL THE DATA IN THE ENVIRONMENT
# #
# #----------------------------------------------------------------------------------------------------------------

# rm(list=ls(all=TRUE))
# T1 <- Sys.time()

# #----------------------------------------------------------------------------------------------------------------
# #
# # CONNECTION PARAMETER TO GET THE ID CALENDAR
# #
# #----------------------------------------------------------------------------------------------------------------

# closeAllConnections()
# lct <- Sys.getlocale("LC_TIME")
# Sys.setlocale("LC_TIME", "C")

# library(RMySQL)
# library(dplyr)

# mydb <- dbConnect(MySQL(),
#                   user = 'root',
#                   password = 'P21yfuobi',
#                   dbname= "nba",
#                   host = '127.0.0.1',
#                   port = 3306)


# team_name <- as.data.frame((dbGetQuery(mydb, "SELECT * FROM team_name")))

# #----------------------------------------------------------------------------------------------------------------
# #
# # SET AS PARAMETER THE YEAR TO GET ALL THE BOCSCORES
# #
# #----------------------------------------------------------------------------------------------------------------

# year <- "2020"

# #----------------------------------------------------------------------------------------------------------------
# #
# # GET LIST OF THE TEAM FOR THE YEAR SELECTED
# #
# #----------------------------------------------------------------------------------------------------------------

# url <- read_html(paste0("https://www.basketball-reference.com/leagues/NBA_",year,".html"))

# list_team_1 <- url %>%
#   html_nodes("table") %>%
#   .[[1]] %>%
#   html_table()

# list_team_2 <- url %>%
#   html_nodes("table") %>%
#   .[[2]] %>%
#   html_table()


# colnames(list_team_2) <- colnames(list_team_1)
# list_team <- rbind(list_team_1,list_team_2)
# list_team$team <- list_team$`Eastern Conference`
# list_team <- list_team %>% select(team)

# rm(list_team_1, list_team_2, url, lct)

# list_team$team <- gsub("\\s*\\([^\\)]+\\)","",as.character(list_team$team))
# list_team$team <- gsub('[*]', '', list_team$team)
# list_team$team <- substr(list_team$team,1,nchar(list_team$team)-1)

# list_team <- merge(list_team, team_name,by.x = "team",by.y=c("team_full"),all.x = TRUE)
# list_team <- na.omit(list_team)
# list_team <- as.data.frame(list_team[,2])
# colnames(list_team) <- "Team"
# list_team <- list_team %>% select(Team) %>% filter(Team != "CHH")

# #----------------------------------------------------------------------------------------------------------------
# #
# # PER TEAM GET THE LIST OF PLAYER
# #
# #----------------------------------------------------------------------------------------------------------------

# list_player <- data.frame(Player= character(),
#                           Pos = character())

# for (team in list_team$Team) {
  
#   print(team)
  
#   closeAllConnections()
#   lct <- Sys.getlocale("LC_TIME")
#   Sys.setlocale("LC_TIME", "C")

#   url <- read_html(paste0("https://www.basketball-reference.com/teams/",team,"/",year,".html"))

#   list_player_tmp <- url %>%
#     html_nodes("table") %>%
#     .[[1]] %>%
#     html_table()

#   list_player_tmp <- list_player_tmp %>% select(Player,Pos)

#   list_player <- as.data.frame(rbind(list_player,list_player_tmp))

# }


# library(tidyr)
# list_player <- list_player %>% separate(Player, c("Name","Surname"), " ")

# list_player$Surname <- ifelse(
#   substr(list_player$Surname,
#          nchar(list_player$Surname)-3,
#          nchar(list_player$Surname)) == "(TW)",
#   substr(list_player$Surname,1, nchar(list_player$Surname)-6),
#   list_player$Surname)

# list_player$Name <- gsub("'","",list_player$Name)
# list_player$Surname <- gsub("'","",list_player$Surname)

# list_player$Surname <- gsub("-","",list_player$Surname)
# list_player$Name <- gsub("-","",list_player$Name)

# list_player$Surname <- gsub("[.]","",list_player$Surname)
# list_player$Name <- gsub("[.]","",list_player$Name)

# list_player$id_webscrappe <- paste0(substr(tolower(list_player$Surname),1,1),
#                                     "/",
#                                     ifelse(nchar(list_player$Surname)<5,
#                                            substr(tolower(list_player$Surname),1,nchar(list_player$Surname)),
#                                            substr(tolower(list_player$Surname),1,5)),
#                                     substr(tolower(list_player$Name),1,2),
#                                     "01",
#                                     "/gamelog/",
#                                     year,
#                                     "/")

# list_player$full_player_name <- paste(list_player$Name,list_player$Surname,sep = " ")

# list_player <- list_player[!duplicated(list_player), ]

# Unaccent <- function(text) {
#   text <- gsub("['`^~\"]", " ", text)
#   text <- iconv(text, to="ASCII//TRANSLIT//IGNORE")
#   text <- gsub("['`^~\"]", "", text)
#   return(text)
# }

# list_player$id_webscrappe <- Unaccent(list_player$id_webscrappe)

# #----------------------------------------------------------------------------------------------------------------
# #
# # WEBSCRAPPE EACH PLAYER TO GET THE PLAYER DATA FOR EACH SEASON
# #
# #----------------------------------------------------------------------------------------------------------------


# data_player <- data.frame(player_name = character(),
#                      position= character(),
#                      game_date = character(),
#                      nb_game_player= numeric(),
#                      nb_game_tm = numeric(),
#                      tm = character(),
#                      opp = character(),
#                      results = character(),
#                      mp= numeric(),
#                      fg= numeric(),
#                      fga= numeric(),
#                      fg_prct= numeric(),
#                      "3p"= numeric(),
#                      "3_pa"= numeric(),
#                      "3p_prct"= numeric(),
#                      ft= numeric(),
#                      fta= numeric(),
#                      ft_prct= numeric(),
#                      orb= numeric(),
#                      drb= numeric(),
#                      trb= numeric(),
#                      ast= numeric(),
#                      stl= numeric(),
#                      blk= numeric(),
#                      tov= numeric(),
#                      pf= numeric(),
#                      pts = numeric(),
#                      gmsc = numeric(),
#                      plusminus = character())

# # 
# # Taurean Waller-Prince

# # list_player$Pos

# i <- 1
# for (i in i:nrow(list_player)) {
#   print(i)
#   print(paste0("https://www.basketball-reference.com/players/",print(list_player[i,4])))
  
  
#   url <- read_html(paste0("https://www.basketball-reference.com/players/",list_player[i,4]))

#   data_player_tmp <- url %>%
#     html_nodes("table") %>%
#     .[[8]] %>%
#     html_table(fill = TRUE)
  
#   full_player_name <- list_player[i,5]
#   position <- list_player[i,3]

#   data_player_tmp$full_player_name <- full_player_name
#   data_player_tmp$pos <- position
  
#   if (ncol(data_player_tmp) == 32) {
    
#     colnames(data_player_tmp) <- c("nb_game_tm","nb_game_player","game_date","age","tm","extdom","opp","results","game_started",
#                                    "mp","fg","fga","fg_prct",
#                                    "3p","3_pa","3p_prct","ft","fta","ft_prct","orb","drb","trb","ast",
#                                    "stl","blk","tov","pf","pts","gmsc","plusminus","player_name","position")
    
#   } else {
    
#     colnames(data_player_tmp) <- c("nb_game_tm","nb_game_player","game_date","age","tm","extdom","opp","results","game_started",
#                                    "mp","fg","fga","fg_prct",
#                                    "3p","3_pa","3p_prct","ft","fta","ft_prct","orb","drb","trb","ast",
#                                    "stl","blk","tov","pf","pts","gmsc","plusminus","col_to_del","player_name","position")
    
#   }
  

#   data_player_tmp <- data_player_tmp %>%
#     filter(nb_game_tm != "Rk") %>%
#     select(player_name,position,game_date,nb_game_player,nb_game_tm,tm,opp,results,mp,fg,fga,fg_prct,
#            "3p","3_pa","3p_prct",ft,fta,ft_prct,orb,drb,trb,ast,
#            stl,blk,tov,pf,pts,gmsc,plusminus)

#   data_player <- as.data.frame(rbind(data_player,data_player_tmp))
# }


# #----------------------------------------------------------------------------------------------------------------
# #
# #
# # DATA PROCESSING OF BASKET BALL PLAYER BOXSCORES (ALL THE DATA FOR 2017)
# #
# #
# #----------------------------------------------------------------------------------------------------------------

# data_player <- data_player[!duplicated(data_player), ]

# write.csv(data_player,paste0("~/nba_prod/NBA DEV/NBA WEBSCRAPPING SCRIPT/data_player_full_",
#                              year,".csv"),row.names=FALSE)


# closeAllConnections()
# all_cons <- dbListConnections(MySQL())

# print(all_cons)

# for(con in all_cons)
#   +  dbDisconnect(con)

# print(paste(length(all_cons), " connections killed."))

# T2 <- Sys.time()
# difftime(T2,T1,units="mins")
