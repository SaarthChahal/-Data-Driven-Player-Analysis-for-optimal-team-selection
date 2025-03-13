#----------------------------------------------------------------------------------------------
import pandas as pd
import os
#----------------------------------------------------------------------------------------------

file_path=r"C:\Users\SAARTH CHAHAL\Downloads\amex round 2\6671501a18c55_round2_input.xlsx"

print("****// Start of program")

# Read each tab of the Excel file into separate DataFrames
print("****// Started reading excel file for batsmen")
batsman_df = pd.read_excel(file_path, sheet_name='batsman_scorecard')
print("****// Started reading excel file for bowlers")
base_bowler_df = pd.read_excel(file_path, sheet_name='bowler_scorecard')


temp_batsman_df = batsman_df
totalRuns = temp_batsman_df.groupby('batsman_id')['runs'].sum()
playersToKeep = totalRuns[totalRuns > 100].index
batsman_df = temp_batsman_df[temp_batsman_df['batsman_id'].isin(playersToKeep)]

## Remove Bowlers having less than 10 wickets, group them by their ids
total_wickets = base_bowler_df.groupby('bowler_id')['wicket_count'].sum()
bowlers_to_keep = total_wickets[total_wickets > 10].index
bowler_df = base_bowler_df[base_bowler_df['bowler_id'].isin(bowlers_to_keep)]


print("Scraping of files :: COMPLETED!")

#----------------------------------------------------------------------------------------------
class Bowler:

    global bowler_points
    bowler_points = pd.DataFrame({
            'bowler_id': bowler_df['bowler_id'].unique(),
        }).set_index('bowler_id')

    # Define the points award system
    def Start():
        print("Initializing Class :: Bowlers")
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

        print("STEP 1 :: COMPLETED!")
        print("**"*50)

        ## Extract years from match_dt
        bowler_df['Year'] = pd.to_datetime(bowler_df['match_dt']).dt.year

        # Calculate the Bowler Average and strike rate for each bowler
        bowler_average = bowler_df.groupby('bowler_id')['runs'].sum()/bowler_df.groupby('bowler_id')['balls_bowled'].sum()
        average_strike_rate = bowler_df.groupby('bowler_id')['runs'].sum()/bowler_df.groupby('bowler_id')['wicket_count'].sum()
        Economy_count = bowler_df.groupby('bowler_id')['economy'].mean()
        w4_per_innings_count = bowler_df.groupby('bowler_id')['wicket_count'].apply(lambda x: (x >= 4).sum())


        # =================================================================================================
        # Creating a database to store values

        bowler_points['4w per innings'] = w4_per_innings_count
        bowler_points['Economy'] = Economy_count
        bowler_points['Bowler Average'] = bowler_average
        bowler_points['Strike Rate'] = average_strike_rate
        bowler_points['Points'] = 0


        print("STEP 2 COMPLETED!        [DATABASE CREATION AND DISQUALIFIYING UNDESIRABLES]")


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
        bowler_points['Recency Adjustment'] = bowler_points['Points']
        bowler_points['Points in 2023'] = 0
        bowler_points['Points in 2022'] = 0
        bowler_points['Points in 2021 and before'] = 0


        for player_id in bowler_points.index:
            matches_2023 = bowler_df[(bowler_df['bowler_id'] == player_id) & (bowler_df['Year'] == 2023)]
            matches_2022 = bowler_df[(bowler_df['bowler_id'] == player_id) & (bowler_df['Year'] == 2022)]
            matches_2021_before = bowler_df[(bowler_df['bowler_id'] == player_id) & (bowler_df['Year'] <= 2021)]
            
            # Calculate Bowler Average and strike rate for each year
            bowler_average_2023 = (matches_2023['runs'].sum()/matches_2023['balls_bowled'].sum()) if not matches_2023.empty else 999
            if matches_2023['wicket_count'].sum()!= 0:
                avg_strike_rate_2023 = (matches_2023['runs'].sum()/matches_2023['balls_bowled'].sum()) if not matches_2023.empty else 999
            else: 
                avg_strike_rate_2023 = 999
            bowler_average_2022 = (matches_2022['runs'].sum()/matches_2022['balls_bowled'].sum()) if not matches_2022.empty else 999
            if matches_2022['wicket_count'].sum() != 0:
                avg_strike_rate_2022 = (matches_2022['runs'].sum()/matches_2022['balls_bowled'].sum()) if not matches_2022.empty else 999
            else: 
                avg_strike_rate_2022 = 999        
            bowler_average_2021_before = (matches_2021_before['runs'].sum()/matches_2021_before['balls_bowled'].sum()) if not matches_2023.empty else 999
            if matches_2021_before['wicket_count'].sum()!=0:
                avg_strike_rate_2021_before = (matches_2021_before['runs'].sum()/matches_2021_before['balls_bowled'].sum()) if not matches_2021_before.empty else 999
            else: 
                avg_strike_rate_2021_before = 999        
            # Store these points in the respective columns
            bowler_points.at[player_id, 'Points in 2023'] = bowler_average_2023 + avg_strike_rate_2023
            bowler_points.at[player_id, 'Points in 2022'] = bowler_average_2022 + avg_strike_rate_2022
            bowler_points.at[player_id, 'Points in 2021 and before'] = 0; 
            bowler_average_2021_before + avg_strike_rate_2021_before
            
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
        top_players = bowler_points.sort_values(by='Total Points with Recency', ascending=False).head(15)

        # Add is bowler captain
        bowler_points['is_bowler_captain'] = bowler_df.groupby('bowler_id')['is_bowler_captain'].max()


        # Save the top players to a new Excel file
        output_file_path = "Top_Bowlers.xlsx"
        top_players.to_excel(output_file_path, sheet_name='Top Players')

        # Display the top players based on total points without recency adjustment
        #top_players_without_recency = bowler_points.sort_values(by='Total Points', ascending=False).head(10)
        print("****// Work done check folder")
        print("**" * 50)


class Batsman:
      # Creating a database to store values
    global batsman_points; 
    batsman_points = pd.DataFrame({
        'batsman_id': batsman_df['batsman_id'].unique(),
    }).set_index('batsman_id')

    def Start():
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

        batsman_points['Centuries'] = centuries_count
        batsman_points['Fifties'] = fifties_count
        batsman_points['Average Runs'] = average_runs
        batsman_points['Strike Rate'] = average_strike_rate
        batsman_points['Points'] = 0
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
        batsman_points['Points in 2023'] = batsman_points['Points in 2022'] = batsman_points['Points in 2021 and before'] = 0


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
            batsman_points.at[player_id, 'Points in 2022'] = 0.0
            batsman_points.at[player_id, 'Points in 2021 and before'] = 0.0
            batsman_points.at[player_id, 'Points in 2023'] = avg_runs_2023 + avg_strike_rate_2023
            batsman_points.at[player_id, 'Points in 2022'] = avg_runs_2022 + avg_strike_rate_2022
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

        #=================================================================================
        #-- Top players in order of recency
        top_players = batsman_points.sort_values(by='Total Points with Recency', ascending=False).head(15)
        
        # Add is_batsman_keeper column
        batsman_points['is_batsman_keeper'] = batsman_df.groupby('batsman_id')['is_batsman_keeper'].max()
        # Add is is_bowler_captain
        batsman_points['is_bowler_captain'] = batsman_df.groupby('batsman_id')['is_bowler_captain'].max()
        # Add is is_batsman_captain
        batsman_points['is_batsman_captain'] = batsman_df.groupby('batsman_id')['is_batsman_captain'].max()


        # Save the top players to a new Excel file
        output_file_path = 'Top_Batsmen.xlsx'
        top_players.to_excel(output_file_path, sheet_name='Top Players')

        print("****// Script Execution Complete :: Check folder")
        print("**" * 50)


class All_Rounder():
    def Start():
        print("Starting calc for allrounders")
        all_rounders_df = pd.merge(batsman_df, bowler_df, left_on="batsman_id", right_on="bowler_id", suffixes=('_bat', '_bowl'))
        all_rounders_df = all_rounders_df.rename(columns={'batsman_id': 'player_id'})

        all_rounders_df = all_rounders_df.join(batsman_points, how='left', rsuffix='_points').fillna(0).reset_index()
        all_rounders_df = all_rounders_df.join(bowler_points, how='left', rsuffix='_points').fillna(0).reset_index()
        

        top_players = all_rounders_df[(all_rounders_df['Average Runs'] >= 10) & (all_rounders_df['wicket_count'] >= 2)]
        top_players = all_rounders_df.sort_values(by='Total Points with Recency', ascending=False).head(30)

        # Export the top players to an Excel file
        output_file_path = r"C:\Users\SAARTH CHAHAL\Downloads\amex round 2\output\Top_All_Rounders.xlsx"
        top_players.to_excel(output_file_path, sheet_name='Top All-Rounders')

        print("Top All-Rounders exported to Top_AllRounders.xlsx")





#// Start the functions
Batsman.Start()
Bowler.Start()
All_Rounder.Start()
