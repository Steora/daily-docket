from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time

# 1. Set up the browser
options = webdriver.ChromeOptions()
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 20) # Increased to 20 seconds for slower page loads

all_court_data = [] 

try:
    print("Navigating to website and accepting terms...")
    driver.get("https://www.ontariocourtdates.ca/Default.aspx")
    
    # Check the 'I Agree' box and click Enter
    wait.until(EC.element_to_be_clickable((By.ID, "ctl00_MainContent_chkAgree"))).click()
    wait.until(EC.element_to_be_clickable((By.ID, "ctl00_MainContent_btnEnter"))).click()
    
    # Wait until the daily docket page loads the Municipality dropdown
    wait.until(EC.presence_of_element_located((By.ID, "ctl00_MainContent_ddlCity")))
    time.sleep(2) 

    print("Setting standard dropdowns (Court = Both, Case Type = All)...")
    Select(driver.find_element(By.ID, "ctl00_MainContent_ddlCourt")).select_by_visible_text("Both")
    Select(driver.find_element(By.ID, "ctl00_MainContent_ddlLob")).select_by_visible_text("All")

    # Find how many Municipalities there are
    mun_select_element = driver.find_element(By.ID, "ctl00_MainContent_ddlCity") 
    num_municipalities = len(Select(mun_select_element).options)

    # Loop through Municipalities (starting at 1 to skip "Select a Municipality...")
    for i in range(1, num_municipalities):
        
        # Re-find dropdown and select
        mun_select_element = wait.until(EC.presence_of_element_located((By.ID, "ctl00_MainContent_ddlCity")))
        mun_select = Select(mun_select_element)
        municipality_name = mun_select.options[i].text
        mun_select.select_by_index(i)
        
        print(f"\nProcessing Municipality: {municipality_name}")
        time.sleep(4) # Wait for ASP.NET to refresh the location dropdown

        # Find how many Locations are in this Municipality
        loc_select_element = wait.until(EC.presence_of_element_located((By.ID, "ctl00_MainContent_listBoxCourtOffice")))
        num_locations = len(Select(loc_select_element).options)

        # Loop through Locations (Starting at 0 now)
        for j in range(0, num_locations):
            
            # Re-find the location listbox
            loc_select_element = wait.until(EC.presence_of_element_located((By.ID, "ctl00_MainContent_listBoxCourtOffice")))
            loc_select = Select(loc_select_element)
            location_name = loc_select.options[j].text
            
            # Skip the "All Below" grouping so we don't get duplicate data
            if "All Below" in location_name:
                continue
                
            loc_select.select_by_index(j)
            print(f"  -> Fetching data for: {location_name}")

            # Click Submit
            submit_btn = driver.find_element(By.ID, "ctl00_MainContent_btnSubmit")
            submit_btn.click()
            
            # VERY IMPORTANT: Wait for the search to finish and the page to reload
            time.sleep(6) 

            # Extract the Data
            try:
                tables = pd.read_html(driver.page_source, flavor='lxml')
                
                if tables:
                    main_table = tables[-1] 
                    main_table['Municipality'] = municipality_name
                    main_table['Court_Location'] = location_name
                    
                    all_court_data.append(main_table)
                    print(f"      [Success] Data extracted!")
                else:
                    print(f"      [Notice] No tables found.")
            except ValueError:
                 print(f"      [Notice] No readable data tables found on page.")
            except Exception as e:
                print(f"      [Error] Could not extract: {e}")

finally:
    driver.quit()

print("\nCompiling master spreadsheet...")
if all_court_data:
    final_dataframe = pd.concat(all_court_data, ignore_index=True)
    final_dataframe.dropna(how="all", inplace=True) 
    final_dataframe.to_excel("Master_Ontario_Court_Dates.xlsx", index=False)
    print("🎉 All done! Saved to Master_Ontario_Court_Dates.xlsx")
else:
    print("⚠️ No data was collected.")