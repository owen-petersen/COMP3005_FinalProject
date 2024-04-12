import json
import psycopg

def getTableColumns(table_name:str) -> str:
    header = "()" #in case table_name is wrong
    match (table_name.lower()): #determine which table to get the header for
        case "competitions":
            header = "(competition_id, season_id, competition_name, competition_gender, country_name, season_name)"
        case "managers":
            header = "(id, fullname, nickname, dob, country_name)"
        case "matches":
            header = "(match_id, competition_id, country_name, season_id, season_name, match_date, kick_off, stadium, stadium_country, referee_name, referee_country, home_team_name, home_team_gender, home_team_manager, home_team_group, home_team_country, away_team_name, away_team_gender, away_team_manager, away_team_group, away_team_country, home_score, away_score, match_week, competition_stage)"
        case "lineups":
            header = "(lineup_id, team_id, team_name, match_id)"
        case "plays":
            header = "(player_id, jersey_number, lineup_id)"
        case "players":
            header = "(player_id, player_name, player_nickname, jersey_number, country)"
        case "events":
            header = "(event_id, index, period, timestamp, minute, second, type, possession, possession_team, play_pattern, team, player_name, position, location, duration, under_pressure, off_camera, out, tactics, match_id)"
        case "bad_behaviour":
            header = "(card, event_id)"
        case "ball_receipt":
            header = "(outcome, event_id)"
        case "interception":
            header = "(outcome, event_id)"
        case "injury_stoppage":
            header = "(in_chain, event_id)"
        case "miscontrol":
            header = "(aerial_won, event_id)"
        case "player_off":
            header = "(permanent, event_id)"
        case "carry":
            header = "(end_location, event_id)"
        case "pressure":
            header = "(counterpress, event_id)"
        case "dribbled_past":
            header = "(counterpress, event_id)"
        case "half_start":
            header = "(late_video_start, event_id)"
        case "clearance":
            header = "(aerial_won, body_part, event_id)"
        case "substitution":
            header = "(replacement, outcome, event_id)"
        case "fifty_fifty":
            header = "(outcome, counterpress, event_id)"
        case "ball_recovery":
            header = "(offensive, recovery_failure, event_id)"
        case "foul_won":
            header = "(defensive, advantage, penalty, event_id)"
        case "duel":
            header = "(counterpress, type, outcome, event_id)"
        case "half_end":
            header = "(early_video_end, match_suspended, event_id)"
        case "dribble":
            header = "(nutmeg, outcome, no_touch, overrun, event_id)"
        case "goalkeeper":
            header = "(position, technique, bodypart, type, outcome, event_id)"
        case "block":
            header = "(deflection, offensive, save_block, counterpress, event_id)"
        case "foul_committed":
            header = "(counterpress, offensive, type, advantage, penalty, card, event_id)"
        case "shot":
            header = "(key_pass_id, end_location, aerial_won, follows_dribble, first_time, open_goal, statsbomb_xg, deflected, technique, bodypart, type, outcome, event_id)"
        case "pass":
            header = "(recipient, length, angle, height, end_location, assisted_shot_id, backheel, deflected, miscommunication, crossed, cutback, switch, shot_assist, goal_assist, bodypart, type, outcome, technique, event_id)"
    return header

class DatabaseInterface:
    def __init__(self):
        """
        Creates and interface for the database. Database is currently hardcoded but can be changed.
        """
        try:
            self.conn = psycopg.connect(
                "dbname=postgres user=postgres password=10/11/2003 host=localhost port=5432"
            )
        except psycopg.OperationalError as e:
            print(f"Error: {e}")
            exit(1)
        self.conn.autocommit = True #automatically commit any changes to the database
        self.cursor = self.conn.cursor()

    def close(self) -> None:
        """
        closes the connection to the database once finished.
        """
        self.cursor.close()
        self.conn.close()

    def getTable(self, table) -> None:
        """
        Gets all students in the table
        """
        self.cursor.execute(f"SELECT * FROM {table}")
        rows = self.cursor.fetchall()
        for row in rows:
            print(row) #Prints each row of the database

    def insertValues(self, values:list[str], table_name:str) -> None:
        """
        Inserts a new student into the table with the specified values
        """
        if (len(values) > 0):
            query = f"INSERT INTO {table_name} {getTableColumns(table_name)} VALUES "

            values[-1] = values[-1].strip(',') + ';'
            query += ''.join(values)
            
            print("Executing query...")
            
            self.cursor.execute(query)

    
    def setUpDatabase(self,) -> None:
        with open("./createTables.sql") as f: # create tables
            self.cursor.execute(f.read())
        


def loadCompetitions(db:DatabaseInterface) -> list:

    f = open("./open-data/data/competitions.json")

    data = json.load(f)

    competition_value_tuples = []

    competitionsAndSeasonIDs = set()

    #loop through the json and create a tuple as a string for the query
    for competition in data:
        if (competition.get("competition_name") in ["La Liga", "Premier League"] and competition.get("season_name") in ["2020/2021", "2019/2020", "2018/2019", "2003/2004"]): #["2020/2021", "2019/2020", "2018/2019", "2003/2004"]
            competitionsAndSeasonIDs.add((competition.get("competition_id"), competition.get("season_id")))
        value = f"({competition.get("competition_id")}, {competition.get("season_id")}, \'{competition.get("competition_name").replace("\'", "\'\'")}\', \'{competition.get("competition_gender").replace("\'", "\'\'")}\', \'{competition.get("country_name").replace("\'", "\'\'")}\', \'{competition.get("season_name").replace("\'", "\'\'")}\'),"
        competition_value_tuples.append(value)

    db.insertValues(competition_value_tuples, "competitions")

    print(f"Inserted {len(competition_value_tuples)} values into Competitions table")

    f.close()

    return (list(competitionsAndSeasonIDs))



def loadMatches(db:DatabaseInterface, competitionAndSeasonIDs) -> list:
    manager_value_tuples = set()
    match_value_tuples = []
    match_IDs = []
    for ID in competitionAndSeasonIDs:
        with open(f"./open-data/data/matches/{ID[0]}/{ID[1]}.json", encoding='utf-8') as f:
            
            data = json.load(f)

            #loop through the json and create a tuple as a string for the query
            for match in data:
                
                try:
                    home_team_manager_id = f"\'{match.get("home_team").get("managers")[0].get("id")}\'"
                except:
                    home_team_manager_id = "NULL"
                try:
                    home_team_manager_name = f"\'{match.get("home_team").get("managers")[0].get("name")}\'"
                except:
                    home_team_manager_name = "NULL"
                try:
                    home_team_manager_nickname = f"\'{match.get("home_team").get("managers")[0].get("nickname")}\'"
                except:
                    home_team_manager_nickname = "NULL"
                try:
                    home_team_manager_dob = f"\'{match.get("home_team").get("managers")[0].get("dob")}\'"
                except:
                    home_team_manager_dob = "NULL"
                try:
                    home_team_manager_country = f"\'{match.get("home_team").get("managers")[0].get("country").get("name")}\'"
                except:
                    home_team_manager_country = "NULL"

                home_manager_value = f"({home_team_manager_id}, {home_team_manager_name}, {home_team_manager_nickname}, {home_team_manager_dob}, {home_team_manager_country}),"
                
                try:
                    away_team_manager_id = f"\'{match.get("home_team").get("managers")[0].get("id")}\'"
                except:
                    away_team_manager_id = "NULL"
                try:
                    away_team_manager_name = f"\'{match.get("home_team").get("managers")[0].get("name")}\'"
                except:
                    away_team_manager_name = "NULL"
                try:
                    away_team_manager_nickname = f"\'{match.get("home_team").get("managers")[0].get("nickname")}\'"
                except:
                    away_team_manager_nickname = "NULL"
                try:
                    away_team_manager_dob = f"\'{match.get("home_team").get("managers")[0].get("dob")}\'"
                except:
                    away_team_manager_dob = "NULL"
                try:
                    away_team_manager_country = f"\'{match.get("home_team").get("managers")[0].get("country").get("name")}\'"
                except:
                    away_team_manager_country = "NULL"

                away_manager_value = f"({away_team_manager_id}, {away_team_manager_name}, {away_team_manager_nickname}, {away_team_manager_dob}, {away_team_manager_country}),"

                if (home_team_manager_id != "NULL"):
                    manager_value_tuples.add(home_manager_value)
                if (away_team_manager_id != "NULL"):
                    manager_value_tuples.add(away_manager_value)

                match_id = match.get("match_id", "NULL")
                competition_id = match.get("competition").get("competition_id", "NULL")
                country_name = match.get("competition").get("country_name", "NULL").replace("\'", "\'\'")
                season_id = match.get("season").get("season_id", "NULL")
                season_name = match.get("season").get("season_name", "NULL").replace("\'", "\'\'")
                match_date = match.get("match_date")
                kick_off = match.get("kick_off")
                try:
                    stadium_name = f"\'{match.get("stadium").get("name")}\'"
                except:
                    stadium_name = "NULL"
                try:
                    stadium_country = f"\'{match.get("stadium").get("country").get("name")}\'"
                except:
                    stadium_country = "NULL"
                try:
                    referee_name = f"\'{match.get("referee").get("name")}\'"
                except:
                    referee_name = "NULL"
                try:
                    referee_country = f"\'{match.get("referee").get("country").get("name")}\'"
                except:
                    referee_country = "NULL"
                home_team_name = match.get("home_team").get("home_team_name")
                home_team_gender = match.get("home_team").get("home_team_gender")
                home_team_group = match.get("home_team").get("home_team_group")
                home_team_country = match.get("home_team").get("country").get("name")
                away_team_name = match.get("away_team").get("away_team_name")
                away_team_gender = match.get("away_team").get("away_team_gender")
                away_team_group = match.get("away_team").get("away_team_group")
                away_team_country = match.get("away_team").get("country").get("name")
                home_score = match.get("home_score")
                away_score = match.get("away_score")
                match_week = match.get("match_week")
                competition_stage = match.get("competition_stage").get("name")

                match_value = f"({match_id}, {competition_id}, \'{country_name}\', {season_id}, \'{season_name}\', \'{match_date}\', \'{kick_off}\', {stadium_name}, {stadium_country}, {referee_name}, {referee_country}, \'{home_team_name}\', \'{home_team_gender}\', {home_team_manager_id}, \'{home_team_group}\', \'{home_team_country}\', \'{away_team_name}\', \'{away_team_gender}\', {away_team_manager_id}, \'{away_team_group}\', \'{away_team_country}\', {home_score}, {away_score}, {match_week}, \'{competition_stage}\'),"
                match_value_tuples.append(match_value)
                match_IDs.append(match_id)
    
    
    db.insertValues(list(manager_value_tuples), "Managers")

    db.insertValues(match_value_tuples, "Matches")

    print(f"Inserted {len(manager_value_tuples)} values into Managers table")

    print(f"Inserted {len(match_value_tuples)} values into Matches table")

    return match_IDs

def loadLineups(db:DatabaseInterface, match_IDs:list[int]) -> None:
    player_value_tuples = set()
    lineup_value_tuples = []
    plays_value_tuples = set() #table for connecting many-to-many relation of players and lineups
    def storePlayers(lineupID:int, players:list[dict]) -> None:
        for player in players:
            player_id = player.get("player_id")
            player_name = f"\'{player.get("player_name").replace("\'", "\'\'")}\'"
            try:
                player_nickname = f"\'{player.get("player_nickname").replace("\'", "\'\'")}\'"
            except:
                player_nickname = "NULL"
            jersey_number = player.get("jersey_number")
            country = f"\'{player.get("country").get("name").replace("\'", "\'\'")}\'"

            player_value = f"({player_id}, {player_name}, {player_nickname}, {jersey_number}, {country}),"
            player_value_tuples.add(player_value)

            plays_value = f"({player_id}, {jersey_number}, {lineupID}),"
            plays_value_tuples.add(plays_value)

    def insertPlayers(db:DatabaseInterface) -> None:
        db.insertValues(list(player_value_tuples), "Players")
        print(f"Inserted {len(player_value_tuples)} values into Players table")


        db.insertValues(list(plays_value_tuples), "Plays")
        print(f"Inserted {len(plays_value_tuples)} values into Plays table")

        

    lineup_serial_id = 0
    for matchID in match_IDs:
        with open(f"./open-data/data/lineups/{matchID}.json", encoding='utf-8') as f:
            
            data = json.load(f)

            #loop through the json and create a tuple as a string for the query
            for lineup in data:
                lineup_id = lineup_serial_id
                lineup_serial_id += 1
                team_id = lineup.get("team_id")
                team_name = f"\'{lineup.get("team_name")}\'"

                storePlayers(lineup_id, lineup.get("lineup", []))                

                lineup_value = f"({lineup_id}, {team_id}, {team_name}, {matchID}),"
                lineup_value_tuples.append(lineup_value)
    
    
    db.insertValues(lineup_value_tuples, "Lineups")

    print(f"Inserted {len(lineup_value_tuples)} values into Lineups table")

    insertPlayers(db)





def loadEvents(db:DatabaseInterface, match_IDs:list[int]) -> None:
    eventTypes = {"Bad_Behaviour":[], "Ball_Receipt":[], "Interception":[], "Injury_Stoppage":[], "Miscontrol":[], "Player_Off":[], "Carry":[], "Pressure":[], "Dribbled_Past":[], "Half_Start":[], "Clearance":[], "Substitution":[], "Fifty_Fifty":[], "Ball_Recovery":[], "Foul_Won":[], "Duel":[], "Half_End":[], "Dribble":[], "Goalkeeper":[], "Block":[], "Foul_Committed":[], "Shot":[], "Pass":[]}
    def storeEventType(eventType:str, event:dict, eventID:int) -> None:
        match eventType:
            case "Bad Behaviour":
                eventAttributes = event.get("bad_behaviour")
                try:
                    card = f"\'{eventAttributes.get("card").get("name")}\'"
                except:
                    card = "NULL"

                eventType_value = f"({card}, {eventID}),"
                eventTypes["Bad_Behaviour"].append(eventType_value)
            case "Ball Receipt":
                eventAttributes = event.get("ball_receipt")
                try:
                    outcome = f"\'{eventAttributes.get("outcome").get("name")}\'"
                except:
                    outcome = "NULL"

                eventType_value = f"({outcome}, {eventID}),"
                eventTypes["Ball_Receipt"].append(eventType_value)
            case "Interception":
                eventAttributes = event.get("interception")
                try:
                    outcome = f"\'{eventAttributes.get("outcome").get("name")}\'"
                except:
                    outcome = "NULL"

                eventType_value = f"({outcome}, {eventID}),"
                eventTypes["Interception"].append(eventType_value)
            case "Injury Stoppage":
                eventAttributes = event.get("injury_stoppage")
                try:
                    in_chain = eventAttributes.get("in_chain",False)
                except:
                    in_chain = False

                eventType_value = f"({in_chain}, {eventID}),"
                eventTypes["Injury_Stoppage"].append(eventType_value)
            case "Miscontrol":
                eventAttributes = event.get("miscontrol")
                try:
                    aerial_won = eventAttributes.get("aerial_won",False)
                except:
                    aerial_won = False

                eventType_value = f"({aerial_won}, {eventID}),"
                eventTypes["Miscontrol"].append(eventType_value)
            case "Player Off":
                eventAttributes = event.get("player_off")
                try:
                    permanent = eventAttributes.get("permanent",False)
                except:
                    permanent = False

                eventType_value = f"({permanent}, {eventID}),"
                eventTypes["Player_Off"].append(eventType_value)
            case "Carry":
                eventAttributes = event.get("carry")
                end_location = f"\'({eventAttributes.get("end_location")[0]},{eventAttributes.get("end_location")[1]})\'"

                eventType_value = f"({end_location}, {eventID}),"
                eventTypes["Carry"].append(eventType_value)
            case "Pressure":
                eventAttributes = event.get("pressure")
                try:
                    counterpress = eventAttributes.get("counterpress",False)
                except:
                    counterpress = False

                eventType_value = f"({counterpress}, {eventID}),"
                eventTypes["Pressure"].append(eventType_value)
            case "Dribbled Past":
                eventAttributes = event.get("dribbled_past")
                try:
                    counterpress = eventAttributes.get("counterpress",False)
                except:
                    counterpress = False

                eventType_value = f"({counterpress}, {eventID}),"
                eventTypes["Dribbled_Past"].append(eventType_value)
            case "Half Start":
                eventAttributes = event.get("half_start")
                try:
                    late_video_start = eventAttributes.get("late_video_start",False)
                except:
                    late_video_start = False

                eventType_value = f"({late_video_start}, {eventID}),"
                eventTypes["Half_Start"].append(eventType_value)
            case "Clearance":
                eventAttributes = event.get("clearance")
                aerial_won = eventAttributes.get("aerial_won",False)
                try:
                    bodypart = f"\'{eventAttributes.get("body_part").get("name")}\'"
                except:
                    bodypart = "NULL"

                eventType_value = f"({aerial_won}, {bodypart}, {eventID}),"
                eventTypes["Clearance"].append(eventType_value)
            case "Substitution":
                eventAttributes = event.get("substitution")
                replacement = f"\'{eventAttributes.get("replacement").get("name").replace("\'", "\'\'")}\'"
                try:
                    outcome = f"\'{eventAttributes.get("outcome").get("name")}\'"
                except:
                    outcome = "NULL"

                eventType_value = f"({replacement}, {outcome}, {eventID}),"
                eventTypes["Substitution"].append(eventType_value)
            case "50/50":
                eventAttributes = event.get("50_50")
                try:
                    outcome = f"\'{eventAttributes.get("outcome").get("name")}\'"
                except:
                    outcome = "NULL"
                try:
                    counterpress = eventAttributes.get("counterpress",False)
                except:
                    counterpress = False

                eventType_value = f"({outcome}, {counterpress}, {eventID}),"
                eventTypes["Fifty_Fifty"].append(eventType_value)
            case "Ball Recovery":
                eventAttributes = event.get("ball_recovery")
                try:
                    offensive = eventAttributes.get("offensive",False)
                except:
                    offensive = False
                try:
                    recovery_failure = eventAttributes.get("recover_failure",False)
                except:
                    recovery_failure = False

                eventType_value = f"({offensive}, {recovery_failure}, {eventID}),"
                eventTypes["Ball_Recovery"].append(eventType_value)
            case "Foul Won":
                eventAttributes = event.get("foul_won")
                try:
                    defensive = eventAttributes.get("defensive",False)
                except:
                    defensive = False
                try:
                    advantage = eventAttributes.get("advantage",False)
                except:
                    advantage = False
                try:
                    penalty = eventAttributes.get("penalty",False)
                except:
                    penalty = False

                eventType_value = f"({defensive}, {advantage}, {penalty}, {eventID}),"
                eventTypes["Foul_Won"].append(eventType_value)
            case "Duel":
                eventAttributes = event.get("duel")
                counterpress = eventAttributes.get("counterpress",False)
                try:
                    type = f"\'{eventAttributes.get("type").get("name")}\'"
                except:
                    type = "NULL"
                try:
                    outcome = f"\'{eventAttributes.get("outcome").get("name")}\'"
                except:
                    outcome = "NULL"

                eventType_value = f"({counterpress}, {type}, {outcome}, {eventID}),"
                eventTypes["Duel"].append(eventType_value)
            case "Half End":
                eventAttributes = event.get("half_end")
                try:
                    early_video_end = eventAttributes.get("early_video_end",False)
                except:
                    early_video_end = False
                try:
                    match_suspended = eventAttributes.get("match_suspended",False)
                except:
                    match_suspended = False

                eventType_value = f"({early_video_end}, {match_suspended}, {eventID}),"
                eventTypes["Half_End"].append(eventType_value)
            case "Dribble":
                eventAttributes = event.get("dribble")
                try:
                    nutmeg = eventAttributes.get("nutmeg",False)
                except:
                    nutmeg = False
                try:
                    outcome = f"\'{eventAttributes.get("outcome").get("name")}\'"
                except:
                    outcome = "NULL"
                try:
                    no_touch = eventAttributes.get("no_touch",False)
                except:
                    no_touch = False
                try:
                    overrun = eventAttributes.get("overrun",False)
                except:
                    overrun = False

                eventType_value = f"({nutmeg}, {outcome}, {no_touch}, {overrun}, {eventID}),"
                eventTypes["Dribble"].append(eventType_value)
            case "Goal Keeper":
                eventAttributes = event.get("goalkeeper")
                try:
                    position = f"\'{eventAttributes.get("position").get("name")}\'"
                except:
                    position = "NULL"
                try:
                    technique = f"\'{eventAttributes.get("technique").get("name")}\'"
                except:
                    technique = "NULL"
                try:
                    bodypart = f"\'{eventAttributes.get("bodypart").get("name")}\'"
                except:
                    bodypart = "NULL"
                try:
                    type = f"\'{eventAttributes.get("type").get("name")}\'"
                except:
                    type = "NULL"
                try:
                    outcome = f"\'{eventAttributes.get("outcome").get("name")}\'"
                except:
                    outcome = "NULL"

                eventType_value = f"({position}, {technique}, {bodypart}, {type}, {outcome}, {eventID}),"
                eventTypes["Goalkeeper"].append(eventType_value)
            case "Block":
                eventAttributes = event.get("block")
                try:
                    deflection = eventAttributes.get("deflection",False)
                except:
                    deflection = False
                try:
                    offensive = eventAttributes.get("offensive",False)
                except:
                    offensive = False
                try:
                    save_block = eventAttributes.get("save_block",False)
                except:
                    save_block = False
                try:
                    counterpress = eventAttributes.get("counterpress",False)
                except:
                    counterpress = False

                eventType_value = f"({deflection}, {offensive}, {save_block}, {counterpress}, {eventID}),"
                eventTypes["Block"].append(eventType_value)
            case "Foul Committed":
                eventAttributes = event.get("foul_committed")
                try:
                    counterpress = eventAttributes.get("counterpress",False)
                except:
                    counterpress = "False"
                try:
                    offensive = eventAttributes.get("offensive",False)
                except:
                    offensive = False
                try:
                    type = f"\'{eventAttributes.get("type").get("name")}\'"
                except:
                    type = "NULL"
                try:
                    advantage = eventAttributes.get("advantage",False)
                except:
                    advantage = False
                try:
                    penalty = eventAttributes.get("penalty",False)
                except:
                    penalty = False
                try:
                    card = f"\'{eventAttributes.get("card").get("name")}\'"
                except:
                    card = "NULL"
                
                eventType_value = f"({counterpress}, {offensive}, {type}, {advantage}, {penalty}, {card}, {eventID}),"
                eventTypes["Foul_Committed"].append(eventType_value)
            case "Shot":
                eventAttributes = event.get("shot")
                key_pass_id = f"\'{eventAttributes.get("key_pass_id")}\'"
                end_location = f"\'({eventAttributes.get("end_location")[0]},{eventAttributes.get("end_location")[1]})\'"
                aerial_won = eventAttributes.get("aerial_won",False)
                follows_dribble = eventAttributes.get("follows_dribble",False)
                first_time = eventAttributes.get("first_time",False)
                open_goal = eventAttributes.get("open_goal",False)
                statsbomb_xg = eventAttributes.get("statsbomb_xg")
                deflected = eventAttributes.get("deflected",False)
                try:
                    technique = f"\'{eventAttributes.get("technique").get("name")}\'"
                except:
                    technique = "NULL"
                bodypart = f"\'{eventAttributes.get("body_part").get("name")}\'"
                try:
                    type = f"\'{eventAttributes.get("type").get("name")}\'"
                except:
                    type = "NULL"
                try:
                    outcome = f"\'{eventAttributes.get("outcome").get("name")}\'"
                except:
                    outcome = "NULL"

                eventType_value = f"({key_pass_id}, {end_location}, {aerial_won}, {follows_dribble}, {first_time}, {open_goal}, {statsbomb_xg}, {deflected}, {technique}, {bodypart}, {type}, {outcome}, {eventID}),"
                eventTypes["Shot"].append(eventType_value)
            case "Pass":
                eventAttributes = event.get("pass")
                try:
                    recipient = f"\'{eventAttributes.get("recipient").get("name").replace("\'", "\'\'")}\'"
                except:
                    recipient = "NULL"
                length = eventAttributes.get("length", "NULL")
                angle = eventAttributes.get("angle", "NULL")
                try:
                    height = f"\'{eventAttributes.get("height").get("name")}\'"
                except:
                    height = "NULL"
                end_location = f"\'({eventAttributes.get("end_location")[0]},{eventAttributes.get("end_location")[1]})\'"
                assisted_shot_id = f"\'{eventAttributes.get("assisted_shot_id")}\'"
                backheel = eventAttributes.get("backheel", False)
                deflected = eventAttributes.get("deflected", False)
                miscommunication = eventAttributes.get("miscommunication", False)
                crossed = eventAttributes.get("crossed", False)
                cutback = eventAttributes.get("cutback",False)
                switch =  eventAttributes.get("switch", False)
                shot_assist = eventAttributes.get("shot-assist",False)
                goal_assist = eventAttributes.get("goal-assist",False)
                try:
                    bodypart = f"\'{eventAttributes.get("body_part").get("name")}\'"
                except:
                    bodypart = "NULL"
                try:
                    type = f"\'{eventAttributes.get("type").get("name")}\'"
                except:
                    type = "NULL"
                try:
                    outcome = f"\'{eventAttributes.get("outcome").get("name")}\'"
                except:
                    outcome = "NULL"
                try:
                    technique = f"\'{eventAttributes.get("technique").get("name")}\'"
                except:
                    technique = "NULL"

                eventType_value = f"({recipient}, {length}, {angle}, {height}, {end_location}, {assisted_shot_id}, {backheel}, {deflected}, {miscommunication}, {crossed}, {cutback}, {switch}, {shot_assist}, {goal_assist}, {bodypart}, {type}, {outcome}, {technique}, {eventID}),"
                eventTypes["Pass"].append(eventType_value)
    
    def insertEventTypes(db:DatabaseInterface) -> None:
        for eventType in eventTypes.keys():

            db.insertValues(eventTypes[eventType], eventType)
            
            print(f"Inserted {len(eventTypes[eventType])} values into {eventType} table")

    
    event_value_tuples = []
    count = 0
    for matchID in match_IDs:
        with open(f"./open-data/data/events/{matchID}.json", encoding='utf-8') as f:
            
            data = json.load(f)

            #loop through the json and create a tuple as a string for the query
            for event in data:

                
                event_id = f"\'{event.get("id")}\'"
                index = event.get("index")
                period = event.get("period")
                timestamp = f"\'{event.get("timestamp")}\'"
                minute = event.get("minute")
                second = event.get("second")
                type = event.get("type").get("name").strip("*")
                possession = event.get("possession")
                possession_team = f"\'{event.get("possession_team").get("name")}\'"
                play_pattern = f"\'{event.get("play_pattern").get("name")}\'"
                team = f"\'{event.get("team").get("name")}\'"
                try:
                    player_name = f"\'{event.get("player").get("name").replace("\'", "\'\'")}\'"
                except:
                    player_name = "NULL"
                try:
                    position = f"\'{event.get("position").get("name")}\'"
                except:
                    position = "NULL"
                try:
                    location = f"\'({event.get("location")[0]},{event.get("location")[1]})\'"
                except:
                    location = "NULL"

                    duration = event.get("duration", "NULL")
                under_pressure = event.get("under_pressure", False)
                off_camera = event.get("off_camera", False)
                out = event.get("out", False)
                try:
                    tactics = event.get("tactics").get("formation")
                except:
                    tactics = "NULL"

                storeEventType(type, event, event_id)

                event_value = f"({event_id}, {index}, {period}, {timestamp}, {minute}, {second}, \'{type}\', {possession}, {possession_team}, {play_pattern}, {team}, {player_name}, {position}, {location}, {duration}, {under_pressure}, {off_camera}, {out}, {tactics}, {matchID}),"
                event_value_tuples.append(event_value)
    
    
    db.insertValues(event_value_tuples, "Events")
    print(f"Inserted {len(event_value_tuples)} values into Events table")

    insertEventTypes(db)



db = DatabaseInterface()
db.setUpDatabase()

competitionsAndSeasons = loadCompetitions(db)
match_IDs = loadMatches(db, competitionsAndSeasons)
loadLineups(db, match_IDs)
loadEvents(db, match_IDs)