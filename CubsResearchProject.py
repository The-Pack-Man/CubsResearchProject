'''
Created on Jun 17, 2021

@author: Trey
'''

###API PULL IMPORT
import requests

###DATA MUNGING IMPORTS
import pandas as pd

###DATA TYPE IMPORTS
import datetime
from datetime import timedelta
import psycopg2

###DATABASE IMPORTS
from sqlalchemy import create_engine

###PROCESS SCHEDULE IMPORTS
##from airflow.operators import python_operator
###from airflow import DAG

"""
This function pulls all finished games on the current date, it does this by using the default functionality of the API to get the current days games

We rename the column names to be more user friendly

This function takes the game primary key as an optional input, if it is not provided we pull all games for the current day

The cleaning of the API Data could've been done as a separate DAG task

"""
def get_games(gamePk=None):
    ###SET GAME PK STRING
    if(gamePk!=None):
        gameString="&gamePk="+gamePk
    else:
        gameString=""
    
    ###GET JSON RESPONSE
    response = requests.get("https://statsapi.mlb.com/api/v1/schedule?sportId=1"+gameString)
    
    ###PARSE OUT GAMES
    games_df=pd.json_normalize(response.json()['dates'][0]['games'],meta=['games'])
    
    ###GET FINISHED GAMES
    games_df=games_df[games_df['status.statusCode']=="F"]
    
    ###GRAB AND RENAME COLUMNS OF INTEREST
    games_df=games_df[["gamePk","officialDate","gameDate","status.abstractGameState","venue.id","venue.name","teams.away.score","teams.home.score","teams.away.team.id","teams.away.team.name","teams.home.team.id","teams.home.team.name"]]
    games_df.columns=["gamePk","Game Date", "Game Time", "Game Status", "Venue ID", "Venue Name", "Away Team Score", "Home Team Score","Away Team ID","Away Team Name", "Home Team ID", "Home Team Name"]
    
    return games_df

"""
This function returns the game events for a particular game primary key

This function requires a game primary key, but we use a default one to prevent problems

"""
def get_events(gamePk="531060"):
    
    ###GAME INFO DICT
    gameInfo={}
    gameInfo["gamePk"]=gamePk
    
    ###GET RESPONSE
    response = requests.get("https://statsapi.mlb.com/api/v1/game/"+gamePk+"/playByPlay")
    
    ###PARSE DATA, PARSING JSON RECORD FOR MATCHUP
    events_df=pd.json_normalize(response.json()["allPlays"],record_path=["playEvents"],meta=["matchup"],errors="ignore")
    events_df=pd.concat([events_df.drop(['matchup'], axis=1), events_df['matchup'].apply(pd.Series)["batter"].apply(pd.Series)], axis=1)
    
    ###GETS PITCH COUNT
    pitch_count=len(events_df[events_df["isPitch"]==True])
    gameInfo["Pitch_Count"]=pitch_count
    
    ###GETS MAX LAUNCH SPEED RECORD
    max_launch_speed=events_df[events_df["hitData.launchSpeed"]==events_df["hitData.launchSpeed"].max()]
    
    ###GETS PLAYER INFO, STORE IN DICT
    max_launch_player=max_launch_speed["fullName"]
    max_launch_player_id=max_launch_speed["id"]
    gameInfo["Max_Exit_Velocity_Player_Name"]=max_launch_player
    gameInfo["Max_Exit_Velocity_Player_ID"]=max_launch_player_id
    
    ###CONVERT DICT TO DATAFRAME
    gameInfo_df=pd.DataFrame(gameInfo)

    return gameInfo_df

"""

This gets all of the information we need to commit to the database, first we get the completed game data, then we get the event data for each of these games

"""
def get_completed_game_info(gamePk=None):
    
    ###GET COMPLETED GAMES
    completed_games=get_games(gamePk)

    ###CONVERT TO LIST
    games_pk_list=completed_games["gamePk"].to_list()
    
    ###GET LIST OF FRAMES
    event_frames=[]
    ###FOR EACH GAME 
    for gamePk in games_pk_list:
        event_frames.append(get_events(str(gamePk)))
    
    event_df=pd.concat(event_frames)
    return completed_games,event_df

"""

Here we execute all events in the intended order

"""
def full_etl_pipeline(gamesPk=None):
    game_info_df,event_info_df=get_completed_game_info(gamesPk)
    
    ###CREATE ENGINE CONNECTION STRING
    engine = create_engine("'insert connection string here'")
    
    ###GET EXISTING DATA
    existing_game_info=pd.read_sql("SELECT * FROM game_info",engine)
    existing_event_info=pd.read_sql("SELECT * FROM event_info",engine)
    
    ###REMOVE EXISTING DATA
    game_info_df=game_info_df[~game_info_df.gamePk.isin(existing_game_info.gamePk)]
    event_info_df=event_info_df[~event_info_df.gamePk.isin(existing_event_info.gamePk)]
    
    ###INSERT GAME DATA INTO TEMPORARY TABLE
    game_info_df.to_sql(name='game_info', con=engine, if_exists = 'append', index=False)
    
    ###INSERT GAME DATA INTO TEMPORARY TABLE
    event_info_df.to_sql(name='event_info', con=engine, if_exists = 'append', index=False)
    
    return

"""

Here we could set up more task dependencies, but they are not necessary

This is setup in order to repeat the task every hour starting on 6/19

"""
"""
default_args = {
  'owner': 'TPackard',
  'start_date': datetime(2021, 6, 19),
  'retries': 3,
  'retry_delay': timedelta(minutes=20)
}

### INSTANTIATE DAG
cubs_dag = DAG('Cubs Research ETL', default_args=default_args, schedule_interval='0 * * * *')

pull_games = python_operator(
    task_id='pull_game_data',
    
    ###FUNCTION TO BE CALLED
    python_callable=full_etl_pipeline,
    
    ###DEFINE DAG ARGUMENTS
    op_kwargs={'gamePK':None},
    dag=cubs_dag
    )
"""


full_etl_pipeline()