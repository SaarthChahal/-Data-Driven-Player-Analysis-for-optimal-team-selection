#----------------------------------------------------------------------------------------------
import pandas as pd

#----------------------------------------------------------------------------------------------

file_path = r'C:\Users\SAARTH CHAHAL\Downloads\amex round 2\input\6671501a18c55_round2_input.xlsx'

print("****// Start of program")

#----------------------------------------------------------------------------------------------
class bowler:
    # Define the points award system
    points_award_system = {
        '4w per innings': {
            '>= 4': 30,'= 3': 20,'= 2':20,'= 1': 10,'= 0': 0,
        },
        'Economy': {'<= 3': 50,'(3,5]': 40,'(5,7)': 30,'>=7':0, 
        },
        'Bowler Average': {'<= 20': 30,'(20, 30]': 20,'(30, 40]': 10,'> 40': 0,
        },
        'Strike Rate': {'<= 15': 30,'(15, 19]': 20,'(19, 24]': 10, '> 24': 0,
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
    print("****// Started reading excel file for bowlers")
    initial_bowler_df = pd.read_excel(file_path, sheet_name='bowler_scorecard')
    print("READ FILE ONCE COMPLETED!")
    print("STEP 1 COMPLETED!")
    print("**"*50)
    #start removing bowlers with less than total 10 wickets taken
    
    # Step 1: Group by 'bowler_id' and sum 'wicket_count'
    total_wickets = initial_bowler_df.groupby('bowler_id')['wicket_count'].sum()

    # Step 2: Filter out bowlers with total wickets less than 10
    bowlers_to_keep = total_wickets[total_wickets >= 10].index

    # Step 3: Use this mask to filter the original DataFrame
    bowler_df = initial_bowler_df[initial_bowler_df['bowler_id'].isin(bowlers_to_keep)]
    
    # Extract years from match_dt
    bowler_df['Year'] = pd.to_datetime(bowler_df['match_dt']).dt.year

    # Calculate the Bowler Average and strike rate for each bowler


    bowler_average = bowler_df.groupby('bowler_id')['runs'].sum()/bowler_df.groupby('bowler_id')['balls_bowled'].sum()

    average_strike_rate = bowler_df.groupby('bowler_id')['runs'].sum()/bowler_df.groupby('bowler_id')['wicket_count'].sum()

    Economy_count=bowler_df.groupby('bowler_id')['economy'].mean()
    w4_per_innings_count = bowler_df.groupby('bowler_id')['wicket_count'].apply(lambda x: (x >= 4).sum())

    # =================================================================================================
    # Creating a database to store values
    bowler_points = pd.DataFrame({
        'bowler_id': bowler_df['bowler_id'].unique(),
    }).set_index('bowler_id')
    # Initializing columns as float type
    bowler_points['4w per innings'] = 0.0
    bowler_points['Economy'] = 0.0
    bowler_points['Bowler Average'] = 0.0
    bowler_points['Strike Rate'] = 0.0

    bowler_points['4w per innings'] = w4_per_innings_count
    bowler_points['Economy'] = Economy_count
    bowler_points['Bowler Average'] = bowler_average
    bowler_points['Strike Rate'] = average_strike_rate
    bowler_points['Points'] = 0.0


    print("STEP 2 COMPLETED!")


    print("ERROR CLEARER COMPLETED!         ===================================")


    # Calculate points for 4w per innings
    for condition, points in points_award_system['4w per innings'].items():
        if condition.startswith('>='):
            bowler_points.loc[bowler_points['4w per innings'] >= int(condition[3:]), 'Points'] += points
        elif condition.startswith('='):
            bowler_points.loc[bowler_points['4w per innings'] == condition[2:], 'Points'] += points

    # Calculate points for Economy
    for condition, points in points_award_system['Economy'].items():
        if condition.startswith('<='):
            bowler_points.loc[bowler_points['Economy'] <= int(condition[3:]), 'Points'] += points
        elif condition.startswith('('):
            lower_bound,upper_bound = map(int,condition[1:-1].split(','))
            if condition.endswith(']'):
                bowler_points.loc[(bowler_points['Economy']> lower_bound) & (bowler_points['Economy']<= upper_bound), 'Points'] += points
            if condition.endswith(')'):
                bowler_points.loc[(bowler_points['Economy']> lower_bound) & (bowler_points['Economy']< upper_bound), 'Points'] += points

    # Calculate points for Bowler Average
    for condition, points in points_award_system['Bowler Average'].items():
        if condition.startswith('<='):
            bowler_points.loc[bowler_points['Bowler Average'] <= int(condition[3:]), 'Points'] += points
        elif condition.startswith('('):
            lower_bound, upper_bound = map(int, condition[1:-1].split(','))
            bowler_points.loc[(bowler_points['Bowler Average'] > lower_bound) & (bowler_points['Bowler Average'] <= upper_bound), 'Points'] += points
        elif condition.startswith('>'):
            bowler_points.loc[bowler_points['Bowler Average'] > int(condition[2:]), 'Points'] += points

    # Calculate points for strike rate
    for condition, points in points_award_system['Strike Rate'].items():
        if condition.startswith('<='):
            bowler_points.loc[bowler_points['Strike Rate'] <= int(condition[3:]), 'Points'] += points
        elif condition.startswith('('):
            lower_bound, upper_bound = map(int, condition[1:-1].split(','))
            bowler_points.loc[(bowler_points['Strike Rate'] > lower_bound) & (bowler_points['Strike Rate'] <= upper_bound), 'Points'] += points
        elif condition.startswith('>'):
            bowler_points.loc[bowler_points['Strike Rate'] > int(condition[2:]), 'Points'] += points

    # Initialize the recency adjustment columns
    bowler_points['Recency Adjustment'] = 0.0
    bowler_points['Recency Adjustment'] = bowler_points['Points']
    bowler_points['Points in 2023'] = 0.0
    bowler_points['Points in 2022'] = 0.0
    bowler_points['Points in 2021 and before'] = 0.0


    for player_id in bowler_points.index:
        matches_2023 = bowler_df[(bowler_df['bowler_id'] == player_id) & (bowler_df['Year'] == 2023)]
        matches_2022 = bowler_df[(bowler_df['bowler_id'] == player_id) & (bowler_df['Year'] == 2022)]
        matches_2021_before = bowler_df[(bowler_df['bowler_id'] == player_id) & (bowler_df['Year'] <= 2021)]
        
        # Calculate Bowler Average and strike rate for each year
        bowler_average_2023 = (matches_2023['runs'].sum()/matches_2023['balls_bowled'].sum()) if not matches_2023.empty else 999

        avg_strike_rate_2023 = (matches_2023['runs'].sum()/matches_2023['balls_bowled'].sum()) if not matches_2023.empty else 999

        bowler_average_2022 = (matches_2022['runs'].sum()/matches_2022['balls_bowled'].sum()) if not matches_2022.empty else 999

        avg_strike_rate_2022 = (matches_2022['runs'].sum()/matches_2022['balls_bowled'].sum()) if not matches_2022.empty else 999
      
        bowler_average_2021_before = (matches_2021_before['runs'].sum()/matches_2021_before['balls_bowled'].sum()) if not matches_2021_before.empty else 999

        avg_strike_rate_2021_before = (matches_2021_before['runs'].sum()/matches_2021_before['balls_bowled'].sum()) if not matches_2021_before.empty else 999
      
        # Initialize float columns and Store these points in the respective columns
        bowler_points['Points in 2021 and before'] = 0.0
        bowler_points.at[player_id, 'Points in 2023'] = 0.0 
        bowler_points.at[player_id, 'Points in 2022'] = 0.0
        bowler_points.at[player_id, 'Points in 2021 and before'] = 0.0

        bowler_points.at[player_id, 'Points in 2023'] = bowler_average_2023 + avg_strike_rate_2023
        bowler_points.at[player_id, 'Points in 2022'] = bowler_average_2022 + avg_strike_rate_2022
        bowler_points.at[player_id, 'Points in 2021 and before'] = bowler_average_2021_before + avg_strike_rate_2021_before
        
        # Adjust points based on recency
        if bowler_average_2023 <= 20:
            bowler_points.at[player_id, 'Recency Adjustment'] += 30 * 0.10
        elif 20 < bowler_average_2023 <= 30:
            bowler_points.at[player_id, 'Recency Adjustment'] += 20 * 0.10
        elif 30 < bowler_average_2023 <= 40:
            bowler_points.at[player_id, 'Recency Adjustment'] += 10 * 0.10
        else:
            bowler_points.at[player_id, 'Recency Adjustment'] += 0 * 0.10
        
        if avg_strike_rate_2023 <= 15:
            bowler_points.at[player_id, 'Recency Adjustment'] += 50 * 0.10
        elif 15 < avg_strike_rate_2023 <= 19:
            bowler_points.at[player_id, 'Recency Adjustment'] += 40 * 0.10
        elif 19 < avg_strike_rate_2023 <= 24:
            bowler_points.at[player_id, 'Recency Adjustment'] += 30 * 0.10
        else:
            bowler_points.at[player_id, 'Recency Adjustment'] += 0 * 0.10
        
        if bowler_average_2022 <= 20:
            bowler_points.at[player_id, 'Recency Adjustment'] += 30 * 0.05
        elif 20 < bowler_average_2022 <= 30:
            bowler_points.at[player_id, 'Recency Adjustment'] += 20 * 0.05
        elif 30 < bowler_average_2022 <= 40:
            bowler_points.at[player_id, 'Recency Adjustment'] += 10 * 0.05
        else:
            bowler_points.at[player_id, 'Recency Adjustment'] += 0 * 0.05
        
        if avg_strike_rate_2022 <= 15:
            bowler_points.at[player_id, 'Recency Adjustment'] += 50 * 0.05
        elif 15 < avg_strike_rate_2022 <= 19:
            bowler_points.at[player_id, 'Recency Adjustment'] += 40 * 0.05
        elif 19 < avg_strike_rate_2022 <= 24:
            bowler_points.at[player_id, 'Recency Adjustment'] += 30 * 0.05
        else:
            bowler_points.at[player_id, 'Recency Adjustment'] += 0 * 0.05
  


    print("WEIGHT ASSIGNMENT COMPLETED!")
      

    # Calculate consistency using the standard deviation of runs scored
    consistency = bowler_df.groupby('bowler_id')['economy'].std()
    bowler_points['Consistency'] = consistency

    # Calculate quantile ranks based on standard deviation :: 4 is quantiles, idk what that is
    bowler_points['Consistency Rank'] = pd.qcut(bowler_points['Consistency'], 4, labels=False)

    # Map the ranks to points
    bowler_points['Consistency Points'] = bowler_points['Consistency Rank'].map(points_award_system_consistency)

    # Add the consistency points to the total points
    bowler_points['Total Points'] = bowler_points['Points'] + bowler_points['Consistency Points']
    bowler_points['Total Points with Recency'] = bowler_points['Recency Adjustment'] + bowler_points['Consistency Points']
    #bowler_points = bowler_points.drop(columns=['Consistency Rank'])         Drop this or nah?
    
    #-- Top players in order of recency
    top_players = bowler_points.sort_values(by='Total Points with Recency', ascending=False).head(20)
    
    # Add is bowler captain
    bowler_points['is_bowler_captain'] = bowler_df.groupby('bowler_id')['is_bowler_captain'].max()


    # Save the top players to a new Excel file
    output_file_path = r"C:\Users\SAARTH CHAHAL\Downloads\amex round 2\output\Top_Bowlers.xlsx"
    top_players.to_excel(output_file_path, sheet_name='Top Players')

    # Display the top players based on total points without recency adjustment
    #top_players_without_recency = bowler_points.sort_values(by='Total Points', ascending=False).head(10)
    print("****// Work done check folder")
    print("**" * 50)