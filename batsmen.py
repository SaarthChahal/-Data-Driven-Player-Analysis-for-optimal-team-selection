#----------------------------------------------------------------------------------------------
import pandas as pd
import os 
#----------------------------------------------------------------------------------------------

file_path = r"C:\Users\SAARTH CHAHAL\Downloads\amex round 2\6671501a18c55_round2_input.xlsx"


print("****// Start of program")

#----------------------------------------------------------------------------------------------
class Batsman:
    # Define the points award system
    points_award_system = {
        'Centuries': {
            '>= 3': 30,'= 2': 20,'= 1': 10,'= 0': 0,
        },
        'Fifties': {'>= 5': 20,'= 3 or 4': 10,
        },
        'Average Runs': {'>= 50': 30,'[40, 50)': 20,'[30, 40)': 10,'< 30': 5,
        },
        'Strike Rate': {'>= 150': 50,'[100, 150)': 40,'[80, 100)': 30, '< 80': 0,
        }
    }
    # Define the points based on quantile ranks
    points_award_system_consistency = {
        0: 40,  # 0th quantile (lowest SD)
        1: 30,
        2: 20,
        3: 10,   # 3rd quantile (highest SD)
    }


    # Read each tab of the Excel file into separate DataFrames
    print("****// Started reading excel file for batsmen")
    batsman_df = pd.read_excel(file_path, sheet_name='batsman_scorecard')
    print("READ FILE ONCE COMPLETED!")
    print("STEP 1 COMPLETED!")
    print("**"*50)

    # Extract years from match_dt
    batsman_df['Year'] = pd.to_datetime(batsman_df['match_dt']).dt.year

    # Calculate the number of centuries and half-centuries scored by each batsman
    centuries_count = batsman_df[batsman_df['runs'] >= 100].groupby('batsman_id')['runs'].count()
    fifties_count = batsman_df[(batsman_df['runs'] >= 50) & (batsman_df['runs'] < 100)].groupby('batsman_id')['runs'].count()

    # Calculate the average runs and strike rate for each batsman
    average_runs = batsman_df.groupby('batsman_id')['runs'].mean()
    average_strike_rate = batsman_df.groupby('batsman_id')['strike_rate'].mean()

    # =================================================================================================
    # Creating a database to store values
    batsman_points = pd.DataFrame({
        'batsman_id': batsman_df['batsman_id'].unique(),
    }).set_index('batsman_id')

    batsman_points['Centuries'] = centuries_count
    batsman_points['Fifties'] = fifties_count
    batsman_points['Average Runs'] = 0.0
    batsman_points['Average Runs'] = average_runs
    batsman_points['Strike Rate'] = 0.0
    batsman_points['Strike Rate'] = average_strike_rate
    batsman_points['Points'] = 0.0
    batsman_points['Points'] = 0.0


    print("STEP 2 COMPLETED!")


    # Fill NaN values with 0 for centuries and fifties
    batsman_points['Centuries'].fillna(0, inplace=True)
    batsman_points['Fifties'].fillna(0, inplace=True)


    print("ERROR CLEARER COMPLETED!         ===================================")


    # Calculate points for centuries
    for condition, points in points_award_system['Centuries'].items():
        if condition.startswith('>='):
            batsman_points.loc[batsman_points['Centuries'] >= int(condition[3:]), 'Points'] += points
        elif condition.startswith('='):
            batsman_points.loc[batsman_points['Centuries'] == int(condition[2:]), 'Points'] += points

    # Calculate points for fifties
    for condition, points in points_award_system['Fifties'].items():
        if condition.startswith('>='):
            batsman_points.loc[batsman_points['Fifties'] >= int(condition[3:]), 'Points'] += points
        elif condition == '= 3 or 4':
            batsman_points.loc[batsman_points['Fifties'].isin([3, 4]), 'Points'] += points

    # Calculate points for average runs
    for condition, points in points_award_system['Average Runs'].items():
        if condition.startswith('>='):
            batsman_points.loc[batsman_points['Average Runs'] >= int(condition[3:]), 'Points'] += points
        elif condition.startswith('['):
            lower_bound, upper_bound = map(int, condition[1:-1].split(','))
            batsman_points.loc[(batsman_points['Average Runs'] >= lower_bound) & (batsman_points['Average Runs'] < upper_bound), 'Points'] += points
        elif condition.startswith('<'):
            batsman_points.loc[batsman_points['Average Runs'] < int(condition[2:]), 'Points'] += points

    # Calculate points for strike rate
    for condition, points in points_award_system['Strike Rate'].items():
        if condition.startswith('>='):
            batsman_points.loc[batsman_points['Strike Rate'] >= int(condition[3:]), 'Points'] += points
        elif condition.startswith('['):
            lower_bound, upper_bound = map(int, condition[1:-1].split(','))
            batsman_points.loc[(batsman_points['Strike Rate'] >= lower_bound) & (batsman_points['Strike Rate'] < upper_bound), 'Points'] += points

    # Initialize the recency adjustment columns
    batsman_points['Recency Adjustment'] = batsman_points['Points']
    batsman_points['Points in 2023'] = 0.0
    batsman_points['Points in 2022'] = 0.0
    batsman_points['Points in 2021 and before'] = 0.0


    for player_id in batsman_points.index:
        matches_2023 = batsman_df[(batsman_df['batsman_id'] == player_id) & (batsman_df['Year'] == 2023)]
        matches_2022 = batsman_df[(batsman_df['batsman_id'] == player_id) & (batsman_df['Year'] == 2022)]
        matches_2021_before = batsman_df[(batsman_df['batsman_id'] == player_id) & (batsman_df['Year'] <= 2021)]
        
        # Calculate average runs and strike rate for each year
        avg_runs_2023 = matches_2023['runs'].mean() if not matches_2023.empty else 0
        avg_strike_rate_2023 = matches_2023['strike_rate'].mean() if not matches_2023.empty else 0
        avg_runs_2022 = matches_2022['runs'].mean() if not matches_2022.empty else 0
        avg_strike_rate_2022 = matches_2022['strike_rate'].mean() if not matches_2022.empty else 0
        avg_runs_2021_before = matches_2021_before['runs'].mean() if not matches_2021_before.empty else 0
        avg_strike_rate_2021_before = matches_2021_before['strike_rate'].mean() if not matches_2021_before.empty else 0
        
        # Store these points in the respective columns
        batsman_points.at[player_id, 'Points in 2023'] = 0.0
        batsman_points.at[player_id, 'Points in 2023'] = avg_runs_2023 + avg_strike_rate_2023
        batsman_points.at[player_id, 'Points in 2022'] = 0.0
        batsman_points.at[player_id, 'Points in 2022'] = avg_runs_2022 + avg_strike_rate_2022
        batsman_points.at[player_id, 'Points in 2021 and before'] = 0.0
        batsman_points.at[player_id, 'Points in 2021 and before'] = avg_runs_2021_before + avg_strike_rate_2021_before
        
        # Adjust points based on recency
        if avg_runs_2023 >= 50:
            batsman_points.at[player_id, 'Recency Adjustment'] += 30 * 0.10
        elif 40 <= avg_runs_2023 < 50:
            batsman_points.at[player_id, 'Recency Adjustment'] += 20 * 0.10
        elif 30 <= avg_runs_2023 < 40:
            batsman_points.at[player_id, 'Recency Adjustment'] += 10 * 0.10
        else:
            batsman_points.at[player_id, 'Recency Adjustment'] += 5 * 0.10
        
        if avg_strike_rate_2023 >= 150:
            batsman_points.at[player_id, 'Recency Adjustment'] += 50 * 0.10
        elif 100 <= avg_strike_rate_2023 < 150:
            batsman_points.at[player_id, 'Recency Adjustment'] += 40 * 0.10
        elif 80 <= avg_strike_rate_2023 < 100:
            batsman_points.at[player_id, 'Recency Adjustment'] += 30 * 0.10
        else:
            batsman_points.at[player_id, 'Recency Adjustment'] += 0 * 0.10
        
        if avg_runs_2022 >= 50:
            batsman_points.at[player_id, 'Recency Adjustment'] += 30 * 0.05
        elif 40 <= avg_runs_2022 < 50:
            batsman_points.at[player_id, 'Recency Adjustment'] += 20 * 0.05
        elif 30 <= avg_runs_2022 < 40:
            batsman_points.at[player_id, 'Recency Adjustment'] += 10 * 0.05
        else:
            batsman_points.at[player_id, 'Recency Adjustment'] += 5 * 0.05
        
        if avg_strike_rate_2022 >= 150:
            batsman_points.at[player_id, 'Recency Adjustment'] += 50 * 0.05
        elif 100 <= avg_strike_rate_2022 < 150:
            batsman_points.at[player_id, 'Recency Adjustment'] += 40 * 0.05
        elif 80 <= avg_strike_rate_2022 < 100:
            batsman_points.at[player_id, 'Recency Adjustment'] += 30 * 0.05
        else:
            batsman_points.at[player_id, 'Recency Adjustment'] += 0 * 0.05  


    print("WEIGHT ASSIGNMENT COMPLETED!")

    # Add is_batsman_keeper column
    batsman_keeper = batsman_df.groupby('batsman_id')['is_batsman_keeper'].max()
    # Add is is_bowler_captain
    bowler_captain = batsman_df.groupby('batsman_id')['is_bowler_captain'].max()
    # Add is is_batsman_captain
    batsman_captain = batsman_df.groupby('batsman_id')['is_batsman_captain'].max()

    batsman_points['is_batsman_keeper'] = batsman_keeper
    batsman_points['is_bowler_captain'] = bowler_captain
    batsman_points['is_batsman_captain'] = batsman_captain

    # Add columns for number of fours and sixes scored
    fours_count = batsman_df.groupby('batsman_id')['Fours'].sum()
    sixes_count = batsman_df.groupby('batsman_id')['Sixes'].sum()

    batsman_points['Number of Fours'] = fours_count
    batsman_points['Number of Sixes'] = sixes_count        

    # Calculate consistency using the standard deviation of runs scored
    consistency = batsman_df.groupby('batsman_id')['runs'].std()
    batsman_points['Consistency'] = consistency

    # Calculate quantile ranks based on standard deviation :: 4 is quantiles, idk what that is
    batsman_points['Consistency Rank'] = pd.qcut(batsman_points['Consistency'], 4, labels=False)

    # Map the ranks to points
    batsman_points['Consistency Points'] = batsman_points['Consistency Rank'].map(points_award_system_consistency)

    # Add the consistency points to the total points
    batsman_points['Total Points'] = batsman_points['Points'] + batsman_points['Consistency Points']
    batsman_points['Total Points with Recency'] = batsman_points['Recency Adjustment'] + batsman_points['Consistency Points']
    #batsman_points = batsman_points.drop(columns=['Consistency Rank'])         Drop this or nah?
    
    #-- Top players in order of recency
    top_players = batsman_points.sort_values(by='Total Points with Recency', ascending=False).head(20)   
    


    # Save the top players to a new Excel file
    output_file_path = r"C:\Users\SAARTH CHAHAL\Downloads\amex round 2\output\Top_Batsmen.xlsx"
    top_players.to_excel(output_file_path, sheet_name='Top Players')

    # Display the top players based on total points without recency adjustment
    #top_players_without_recency = batsman_points.sort_values(by='Total Points', ascending=False).head(10)
    print("****// Work done check folder")
    print("**" * 50)