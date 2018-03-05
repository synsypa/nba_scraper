import time
import os
import json
import csv
import pandas as pd
import requests
import urllib.request as ul
from bs4 import BeautifulSoup

class nbapay():
    '''
    Returns a ``toppay()'' object that retrieves the top X player by salary for 
    each NBA team. This will access salary data from spotrac.com
    and player/team stats from stats.nba.com

    Last Modified: kjiang 12/05/15

    Available Stats are: (stats ending in _t are team stats,
                          those ending in _p are player stats)

    ['TEAM_ID', 'TEAM_NAME', 'GP_t', 'W_t', 'L_t', 'W_PCT_t', 'MIN_t',
       'FGM_t', 'FGA_t', 'FG_PCT_t', 'FG3M_t', 'FG3A_t', 'FG3_PCT_t', 'FTM_t',
       'FTA_t', 'FT_PCT_t', 'OREB_t', 'DREB_t', 'REB_t', 'AST_t', 'TOV_t',
       'STL_t', 'BLK_t', 'BLKA_t', 'PF_t', 'PFD_t', 'PTS_t', 'PLUS_MINUS_t',
       'PLAYER_NAME', 'SALARY', 'PLAYER_ID', 'TEAM_ABBREVIATION', 'GP_p',
       'W_p', 'L_p', 'MIN_p', 'POINTS', 'TOUCHES', 'FRONT_CT_TOUCHES',
       'TIME_OF_POSS', 'AVG_SEC_PER_TOUCH', 'AVG_DRIB_PER_TOUCH',
       'PTS_PER_TOUCH', 'ELBOW_TOUCHES', 'POST_TOUCHES', 'PAINT_TOUCHES',
       'PTS_PER_ELBOW_TOUCH', 'PTS_PER_POST_TOUCH', 'PTS_PER_PAINT_TOUCH',
       'AVG_SPEED', 'AVG_SPEED_DEF', 'AVG_SPEED_OFF', 'DIST_FEET',
       'DIST_MILES', 'DIST_MILES_DEF', 'DIST_MILES_OFF', 'MIN1', 'AGE',
       'AST_p', 'BLK_p', 'BLKA_p', 'CFID', 'CFPARAMS', 'DD2', 'DREB_p',
       'FG3A_p', 'FG3M_p', 'FG3_PCT_p', 'FGA_p', 'FGM_p', 'FG_PCT_p', 'FTA_p',
       'FTM_p', 'FT_PCT_p', 'OREB_p', 'PF_p', 'PFD_p', 'PLUS_MINUS_p', 'PTS_p',
       'REB_p', 'STL_p', 'TD3', 'TOV_p', 'W_PCT_p'],
,
    '''

    def __init__(self, output_path = './results'):
        if not os.path.exists(output_path):
            os.makedirs(os.path.abspath(output_path))

        self.output_path = os.path.abspath(output_path)

    def getSalaryStat(self, year = 2014):
        """
        Returns DataFrame of cap hit data for all players in a season.
        Data per spotrac.com

        year:   full starting year of the season (integer). Default = 2014  
        """
        year_str = str(year+1)

        df = pd.read_html('http://espn.go.com/nba/salaries/_/year/' + year_str)[0]
        df.columns = df.iloc[0]
        df = df[df.NAME != 'NAME']
        
        for page in range(2,12):
            page_df = pd.read_html('http://espn.go.com/nba/salaries/_/year/' + \
                                    year_str + '/page/' + str(page))[0]
            page_df.columns = page_df.iloc[0]
            page_df = page_df[page_df.NAME != 'NAME']
            df = pd.concat([df, page_df])
        
        df['NAME'] = df['NAME'].map(lambda x: x.rstrip(', [CSFPG]+'))
        df['NAME'] = df['NAME'].replace('\.(?!$)', '', regex=True)
        df['SALARY'] = df['SALARY'].replace('[\$,]', '', regex=True).astype(int)
        salary_df = df[['NAME', 'SALARY']]
        salary_df = salary_df.reset_index(drop=True)
        salary_df.columns = [['PLAYER_NAME', 'SALARY']]

        # Manual correction for Nene Hilario since he is known as Nene by the NBA   
        salary_df['PLAYER_NAME'] = salary_df['PLAYER_NAME'].replace('Nene Hilario', 'Nene')

        return salary_df

    def getPlayerAdvStat(self, stat, year = 2014):
        """        
        Returns DataFrame of advanced stat data for a list of players and season
        Data from stats.nba.com

        stat:   choice of advanced stat type (string). Options are:
                'touch' 'posession' Returns Possesssion stats
                'speed' 'distance' Returns Speed stats
        year:   full starting year of the season. Default=2014 (integer)
        """

        year_next = (year % 100) + 1
        season = str(year) + '-' + str(year_next)

        stat_call = stat.lower()
        stat_dict = {'touch':'Possessions', 'possession':'Possessions',
                     'speed':'SpeedDistance', 'distance':'SpeedDistance'}

        stat_url = 'http://stats.nba.com/stats/leaguedashptstats?College=&'\
                    'Conference=&Country=&DateFrom=&DateTo=&Division=&'\
                    'DraftPick=&DraftYear=&GameScope=&Height=&LastNGames=0&'\
                    'LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&'\
                    'PORound=0&PerMode=PerGame&PlayerExperience=&PlayerOr'\
                    'Team=Player&PlayerPosition=&PtMeasureType=' + \
                    stat_dict[stat_call] + '&Season=' + season + \
                    '&SeasonSegment=&SeasonType=Regular+Season&StarterBench=&'\
                    'TeamID=0&VsConference=&VsDivision=&Weight='

        response = requests.get(stat_url)
        data = json.loads(response.text)

        headers = data['resultSets'][0]['headers']
        stat_data = data['resultSets'][0]['rowSet']
        advStat_df = pd.DataFrame(stat_data,columns=headers) 

        return advStat_df

    def getPlayerBaseStat(self, year = 2014):
        """        
        Returns DataFrame of traditional stats for a list of players and season
        Data from stats.nba.com

        player_list:    list of players (list)
        year:           full starting year of the season. Default=2014 (integer)
        """
        
        year_next = (year % 100) + 1
        season = str(year) + '-' + str(year_next)

        stat_url = 'http://stats.nba.com/stats/leaguedashplayerstats?College=&'\
                    'Conference=&Country=&DateFrom=&DateTo=&Division=&'\
                    'DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&'\
                    'LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&'\
                    'OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&'\
                    'PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&'\
                    'PlusMinus=N&Rank=N&Season='+ season + '&SeasonSegment=&'\
                    'SeasonType=Regular+Season&ShotClockRange=&StarterBench=&'\
                    'TeamID=0&VsConference=&VsDivision=&Weight='

        response = requests.get(stat_url)
        data = json.loads(response.text)

        headers = data['resultSets'][0]['headers']
        stat_data = data['resultSets'][0]['rowSet']
        baseStat_df = pd.DataFrame(stat_data,columns=headers) 

        return baseStat_df

    def getTeamStat(self, year = 2014):
        """        
        Returns DataFrame of team stats for a given season
        Data from stats.nba.com

        year:   full starting year of the season. Default=2014 (integer)
        """

        year_next = (year % 100) + 1
        season = str(year) + '-' + str(year_next)

        stat_url = 'http://stats.nba.com/stats/leaguedashteamstats?Conference=&'\
                    'DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&'\
                    'LastNGames=0&LeagueID=00&Location=&MeasureType=Base&'\
                    'Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&'\
                    'PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&'\
                    'PlusMinus=N&Rank=N&Season=' + season + '&SeasonSegment=&'\
                    'SeasonType=Regular+Season&ShotClockRange=&StarterBench=&'\
                    'TeamID=0&VsConference=&VsDivision='

        response = requests.get(stat_url)
        data = json.loads(response.text)

        headers = data['resultSets'][0]['headers']
        stat_data = data['resultSets'][0]['rowSet']
        df = pd.DataFrame(stat_data,columns=headers) 

        team_df = df[["TEAM_ID","TEAM_NAME","GP","W","L","W_PCT","MIN","FGM",
                     "FGA","FG_PCT","FG3M","FG3A","FG3_PCT","FTM","FTA","FT_PCT",
                     "OREB","DREB","REB","AST","TOV","STL","BLK","BLKA","PF",
                     "PFD","PTS","PLUS_MINUS"]]

        return team_df
    
    def getDataframe(self, year = 2014):
        """
        Returns a merged dataframe of salary, advanced, base, and team stats for 
        each player in the NBA in a given season. 
        NOTE: Players who have to not played are listed as having no team and
              no stats but are included in the dataframe. (i.e. Joel Embiid)
        
        year:   full starting year of the season (integer). Default = 2014 
        topx:   number of top players (integer). Default = 2 
        """

        # Retrieve Stat Dataframes
        salary_df = self.getSalaryStat(year)
        touch_df = self.getPlayerAdvStat('touch', year)
        speed_df = self.getPlayerAdvStat('speed', year)
        base_df = self.getPlayerBaseStat(year)
        team_df = self.getTeamStat(year)

        # Set of Merge Variables to prevent overlap
        to_merge_1 = ['PLAYER_NAME', 'AVG_SPEED', 'AVG_SPEED_DEF', 'AVG_SPEED_OFF', 
                      'DIST_FEET', 'DIST_MILES', 'DIST_MILES_DEF', 'DIST_MILES_OFF',
                      'MIN1']
        to_merge_2 = ['PLAYER_NAME', 'AGE', 'AST', 'BLK', 'BLKA', 'CFID', 'CFPARAMS', 'DD2',
                      'DREB', 'FG3A', 'FG3M', 'FG3_PCT', 'FGA', 'FGM', 'FG_PCT', 'FTA', 
                      'FTM', 'FT_PCT', 'OREB', 'PF', 'PFD', 'PLUS_MINUS', 'PTS', 'REB', 
                      'STL', 'TD3', 'TOV', 'W_PCT']              

        player_df = pd.merge(salary_df, 
                        pd.merge(
                            pd.merge(touch_df, speed_df[to_merge_1], on = 'PLAYER_NAME',how = 'outer'),
                        base_df[to_merge_2], on = 'PLAYER_NAME', how = 'outer'),
                    on = 'PLAYER_NAME', how = 'outer')

        all_df = pd.merge(team_df, player_df, on = 'TEAM_ID', suffixes= ['_t', '_p'], how = 'right')

        return all_df
