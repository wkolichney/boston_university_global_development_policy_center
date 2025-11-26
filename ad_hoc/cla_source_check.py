import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
from webdriver_manager.chrome import ChromeDriverManager



########################################  PREPROCESSING / DATA CLEANING FOR CLA GOOGLE DRIVE SOURCES  #####################################################


#this dataframe is a excel download of our CLA dataset
df = pd.read_excel('C:/Users/wikku/global_china_initiative/workspace/cla_source/CLA-Database-Raw-Data-Public-2025-FIN.xlsx', sheet_name='USE THIS', skiprows=1)

#select relevant columns
df_select = df[[
    'BU ID',
    'Source 1',
    'Source 2',
    "Loan Signed Source (Int'l/Local)",
    "Loan Signed CN Source",
    'Source 1 - url',
    'Source 2 - url'
    ]]

#not necessary but just to see how many entries we have in each column
s1 =df_select['Source 1'].count()
s2 =df_select['Source 2'].count()
local =df_select["Loan Signed Source (Int'l/Local)"].count()
cn = df_select["Loan Signed CN Source"].count()
s1_url = df_select['Source 1 - url'].count()
s2_url = df_select['Source 2 - url'].count()

s1+s2+local+cn+s1_url+s2_url

# Combine all source columns into a single DataFrame and as a singele column 'Source'. We are checking for which google drive links are broken
df_sources = pd.concat([df_select[['BU ID', 'Source 1']].rename(columns={'Source 1': 'Source'}),
                        df_select[['BU ID', 'Source 2']].rename(columns={'Source 2': 'Source'}),
                        df_select[['BU ID', "Loan Signed Source (Int'l/Local)"]].rename(columns={"Loan Signed Source (Int'l/Local)": 'Source'}),
                        df_select[['BU ID', "Loan Signed CN Source"]].rename(columns={"Loan Signed CN Source": 'Source'}),
                        df_select[['BU ID', 'Source 1 - url']].rename(columns={'Source 1 - url': 'Source'}),
                        df_select[['BU ID', 'Source 2 - url']].rename(columns={'Source 2 - url': 'Source'})
                       ], ignore_index=True)

df_sources = df_sources.dropna(subset=['Source']).reset_index(drop=True)
# Strip whitespace from all sources
df_sources['Source'] = df_sources['Source'].str.strip()

# really only care about google drive links.
drive_link = df_sources[df_sources['Source'].str.contains('drive.google', na=False)]
drive_link['Source'] = drive_link['Source'].str.split(' ').str[0]
drive_link = drive_link.drop_duplicates().reset_index(drop=True)

drive_link.to_csv('C:/Users/wikku/global_china_initiative/workspace/cla_source/cla_drive_links.csv', index=False)
print(drive_link.isna().any())
df = pd.read_csv('C:/Users/wikku/global_china_initiative/workspace/cla_source/cla_drive_links.csv')
print(df.isna().sum()) #simple double check


########################################  END OF PREPROCESSING / DATA CLEANING FOR CLA GOOGLE DRIVE SOURCES  #####################################################




########################################  WEBSCRAPING USING SELENIUM TO CHECK IF GOOGLE DRIVE SOURCES WORK  #####################################################



# Setup the driver
serv_obj = Service('C:/chromedriver-win64/chromedriver.exe')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# import df of URLs
df = pd.read_csv('C:/Users/wikku/global_china_initiative/workspace/cla_source/cla_drive_links.csv')




# Collect all Google Drive links
url_list = []
for link in df['Source'].tolist():
    if 'drive.google' in link:
        url_list.append(link)

# Store results for all URLs
results = []



# Check each URL
for url in url_list:
    try:
        driver.get(url)
        
        # Wait for the page to load
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        print(f"Page loaded successfully: {url}")
        
        # Check if the page contains the error message
        if "Sorry, the file you have requested does not exist" in driver.page_source:
            results.append({"url": url, "status": "broken"})
            print(f"Error detected for: {url}")
        # Check if page requires sign-in
        elif "Sign in" in driver.page_source and "accounts.google.com" in driver.current_url:
            results.append({"url": url, "status": "need_sign_in"})
            print(f"Sign-in required for: {url}")
        else:
            results.append({"url": url, "status": "exists"})
            print(f"No error detected for: {url}")
            
    except TimeoutException:
        results.append({"url": url, "status": "timeout"})
        print(f"Page loading timed out for: {url}")
    except Exception as e:
        results.append({"url": url, "status": f"error: {str(e)}"})
        print(f"Error occurred for {url}: {str(e)}")

# Create DataFrame from results
data = pd.DataFrame(results)
print("\nFinal Results:")
print(data)

data.to_csv('C:/Users/wikku/global_china_initiative/workspace/cla_source/cla_drive_link_check_results.csv', index=False)