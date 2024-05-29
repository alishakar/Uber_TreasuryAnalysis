import requests
import pandas as pd
from bs4 import BeautifulSoup
import csv
import os

urls = {
    "USA" : "https://www.federalreserve.gov/datadownload/Output.aspx?rel=H15&series=d7e27b7b09a3a7feae95b9c61781fcd8&lastobs=12&from=&to=&filetype=csv&label=include&layout=seriescolumn&type=package",
    "Australia" : "https://www.rba.gov.au/statistics/cash-rate/",
    "Europe" : "https://www.ecb.europa.eu/stats/policy_and_exchange_rates/key_ecb_interest_rates/html/index.en.html",
    "Canada" : "https://www.bankofcanada.ca/valet/observations/group/CORRA/csv",
    "England" :"https://www.bankofengland.co.uk/boeapps/database/Bank-Rate.asp",
}



def get_usa_data(url,columns_to_import):
    with requests.Session() as s:
        download = s.get(url)

        decoded_content = download.content.decode('utf-8')

        cr = csv.reader(decoded_content.splitlines(), delimiter=',')

        # Extract header and rows
        header = next(cr)
       
        rows = list(cr)

        
        # Find the indices of the columns to keep
        indices_to_keep = [header.index(col) for col in columns_to_import]
        
        # Filter the rows to keep only the specified columns

        filtered_rows = [
            [row[idx] for idx in indices_to_keep]
            for row in rows
        ]

        # Create a DataFrame with the filtered data
        df = pd.DataFrame(filtered_rows, columns=columns_to_import)
        df_sliced = df.iloc[5:] 
        df_sliced.reset_index(drop=True, inplace=True)
        

        return df_sliced

def get_england_data(url, columns_to_import):
    headers = {
      "User-agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36",
      "cookie":"necessary"  # to avoid unnecessary cookies while parsing the page
    }
    
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    table = soup.find('table')
    eng_data = pd.read_html(str(table))[0]

    eng_data = eng_data[[ 'Date Changed', 'Rate']]
    eng_data['Date Changed'] = pd.to_datetime(eng_data['Date Changed'], format='%d %b %y', errors='coerce')

    # Format the 'Date' column to "yyyy-mm-dd"
    eng_data['Date Changed'] = eng_data['Date Changed'].dt.strftime('%Y-%m')
    eng_data = eng_data.dropna(subset=['Date Changed'])

    eng_data = eng_data[(eng_data['Date Changed'] >= '2023-01') & (eng_data['Date Changed'] <= '2024-04')]
    
    
    # Generate a date range from May 2023 to April 2024
    date_range = pd.date_range(start='2023-05-01', end='2024-04-01', freq='MS')
    # Create a DataFrame with the generated date range
    reference_df = pd.DataFrame(date_range, columns=['Date'])
    # Convert the 'Date' column to the format 'yyyy-mm'
    reference_df['Date'] = reference_df['Date'].dt.strftime('%Y-%m')
    eng_data = eng_data.sort_values(by='Date Changed')
    eng_data = eng_data.rename(columns={'Date Changed': 'Date'})

    merged_df = reference_df.merge(eng_data, on='Date', how='left').fillna(method='ffill')
    
    return merged_df
    


        

def get_australia_data():
    page = requests.get(urls['Australia'])
    soup = BeautifulSoup(page.content, 'html.parser')
    
    table = soup.find('table')
    aus_data = pd.read_html(str(table))[0]
    
    #rba_data.columns = ['Effective Date', 'Cash Rate Target']  # Correct the column names
    aus_data = aus_data[['Effective Date', 'Cash rate target %']]
    aus_data['Effective Date'] = pd.to_datetime(aus_data['Effective Date'], format='%d %b %Y', errors='coerce')

    # Format the 'Date' column to "yyyy-mm-dd"
    aus_data['Effective Date'] = aus_data['Effective Date'].dt.strftime('%Y-%m')

    aus_data = aus_data.dropna(subset=['Effective Date'])
    
    aus_data = aus_data[(aus_data['Effective Date'] >= '2023-01') & (aus_data['Effective Date'] <= '2024-04')]
    
    
    # Generate a date range from May 2023 to April 2024
    date_range = pd.date_range(start='2023-05-01', end='2024-04-01', freq='MS')
    # Create a DataFrame with the generated date range
    reference_df = pd.DataFrame(date_range, columns=['Date'])
    # Convert the 'Date' column to the format 'yyyy-mm'
    reference_df['Date'] = reference_df['Date'].dt.strftime('%Y-%m')
    aus_data = aus_data.sort_values(by='Effective Date')
    aus_data = aus_data.rename(columns={'Effective Date': 'Date'})

    merged_df = reference_df.merge(aus_data, on='Date', how='left').fillna(method='ffill')
    
    return merged_df

def get_europe_data():
    page = requests.get(urls['Europe'])
    soup = BeautifulSoup(page.content, 'html.parser')
    
    table = soup.find('table')
    europe_data = pd.read_html(str(table))[0]

    #SOME COLUMN CLEANING IS DONE OVER HERE AS IT WAS NOT IN ONE SINGLE COLUMN, INSTEAD IT WAS MULTIINDEX COLUMN
    
    europe_data.columns = ['|'.join(column)  if 'Unnamed' not in column[0] else column[1] for column in europe_data.columns]
    europe_data.rename(columns={europe_data.columns[0]: "Year", europe_data.columns[1]: "month-date", 
                               europe_data.columns[5]: "Marginal lending facility"}, inplace=True)
    europe_data['Period'] = europe_data['Year'] + " " + europe_data['month-date']
    europe_data = europe_data[['Period', 'Marginal lending facility']]
  
   
    
    europe_data['Period'] = europe_data['Period'].str.replace('.','')

    # Convert the 'Period' column to datetime format
    europe_data['Period'] = pd.to_datetime(europe_data['Period'], format='%Y %d %b', errors='coerce')
    
    europe_data['Period'] = europe_data['Period'].dt.strftime('%Y-%m')
   
    europe_data = europe_data.dropna(subset=['Period'])
    
    # Filter the data between the specified date range
    europe_data = europe_data[(europe_data['Period'] >= '2023-05') & (europe_data['Period'] <= '2024-04')]
    
  
    # Generate a date range from May 2023 to April 2024
    date_range = pd.date_range(start='2023-05-01', end='2024-04-01', freq='MS')
    # Create a DataFrame with the generated date range
    reference_df = pd.DataFrame(date_range, columns=['Date'])
    # Convert the 'Date' column to the format 'yyyy-mm'
    reference_df['Date'] = reference_df['Date'].dt.strftime('%Y-%m')
    
    europe_data = europe_data.sort_values(by='Period')
    europe_data = europe_data.rename(columns={'Period': 'Date'})

    merged_df = reference_df.merge(europe_data, on='Date', how='left').fillna(method='ffill') 
    
    return merged_df

def get_canada_data(url,columns_to_import):
    with requests.Session() as s:
        download = s.get(url)

        decoded_content = download.content.decode('utf-8')

        cr = csv.reader(decoded_content.splitlines(), delimiter=',')

        # Extract header and rows
        rows = list(cr)

        row_idx = -1
        for idx, row in enumerate(rows):
            if set(columns_to_import).issubset(set(row)):
                row_idx = idx
                break
            
        if row_idx != -1:
            rows = rows[row_idx:]
        header = rows[0]
        
        
        # Find the indices of the columns to keep
        indices_to_keep = [header.index(col) for col in columns_to_import]
        
        # Filter the rows to keep only the specified columns

        filtered_rows = [
            [row[idx] for idx in indices_to_keep]
            for row in rows
        ]

        # Create a DataFrame with the filtered data
        df = pd.DataFrame(filtered_rows[1:], columns=filtered_rows[0])
        # Convert the 'date' column to datetime format
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y-%m')
        
        start_date = '2023-05'
        end_date = '2024-04'
        df_filtered = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

        # To ensure proper grouping, convert the 'date' column back to a Period type
        df_filtered['YearMonth'] = pd.to_datetime(df_filtered['date']).dt.to_period('M')

        # Group by the YearMonth and get the last available entry for each month
        last_day_of_month = df_filtered.groupby('YearMonth').last().reset_index(drop=True)

        # Keep only the 'date' and 'avg.intwo' columns
        result_df = last_day_of_month[['date', 'AVG.INTWO']]

        return result_df
    
        
if __name__ == "__main__":
    # Generate a date range from May 2023 to April 2024
    date_range = pd.date_range(start='2023-05-01', end='2024-04-30', freq='MS')

    # Create a DataFrame with the generated date range
    df = pd.DataFrame(date_range, columns=['Date'])

    # Format the date column to "yyyy-mm"
    
    df['Date'] = df['Date'].dt.strftime('%Y-%m')
   
    usa_df = get_usa_data(urls['USA'], ['Series Description', 'Federal funds effective rate'])
    df['USA_Interest_Rates'] = usa_df['Federal funds effective rate']

    aus_df = get_australia_data()
    df['Aus_Interest_Rates'] = aus_df['Cash rate target %']

    eur_df = get_europe_data()
    df['Eur_Interest_Rates'] = eur_df['Marginal lending facility']

    canada_df = get_canada_data(urls['Canada'], ['date', 'AVG.INTWO'])
    df['Canada_Interest_Rates'] = canada_df['AVG.INTWO']
    
    eng_df= get_england_data(urls['England'], ['Date Changed', 'Rate'])
    df['England_Interest_Rates'] = eng_df['Rate']

   
    downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads")

    # Define the file path for the CSV file in the Downloads folder
    csv_file_path = os.path.join(downloads_folder, "Uber_API.csv")

    # Export the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=False)

    print(f"DataFrame exported to: {csv_file_path}")
   
    
