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
# options.add_argument('--headless') # Remove the '#' at the start of this line to run it invisibly later
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 15)

all_court_data = [] # Master list to hold our extracted tables

try:
    # --- STEP 1: Pass the Agreement Page ---
    # print("Navigating to website and accepting terms...")
    # driver.get("https://www.ontariocourtdates.ca/Default.aspx")
    # agree_button = wait.until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "I Agree")))
    # agree_button.click()
    print("Navigating to website and accepting terms...")
    driver.get("https://www.ontariocourtdates.ca/Default.aspx")
    
    # 1a. Check the 'I Agree' box
    print("Checking 'I Agree' box...")
    agree_checkbox = wait.until(EC.element_to_be_clickable((By.ID, "ctl00_MainContent_chkAgree")))
    agree_checkbox.click()
    
    # 1b. Click the 'Enter' button
    print("Clicking 'Enter' button...")
    enter_button = wait.until(EC.element_to_be_clickable((By.ID, "ctl00_MainContent_btnEnter")))
    enter_button.click()
    # Wait until the daily docket page loads
    wait.until(EC.presence_of_element_located((By.ID, "ctl00_MainContent_ddlCity")))
    time.sleep(2) 

    # --- STEP 2: Set the static dropdowns (Court and Case Type) ---
    print("Setting standard dropdowns (Court = Both, Case Type = All)...")
    Select(driver.find_element(By.ID, "ctl00_MainContent_ddlCourt")).select_by_visible_text("Both")
    Select(driver.find_element(By.ID, "ctl00_MainContent_ddlLob")).select_by_visible_text("All")

    # --- STEP 3: Find how many Municipalities there are ---
    mun_select_element = driver.find_element(By.ID, "ctl00_MainContent_ddlCity") 
    num_municipalities = len(Select(mun_select_element).options)

    # --- STEP 4: Loop through Municipalities ---
    # We start at index 1 to skip the placeholder (e.g., "Select a Municipality...")
    for i in range(1, num_municipalities):
        
        # Re-find the dropdown to avoid StaleElement exceptions
        mun_select = Select(driver.find_element(By.ID, "ctl00_MainContent_ddlCity"))
        municipality_name = mun_select.options[i].text
        mun_select.select_by_index(i)
        
        print(f"\nProcessing Municipality: {municipality_name}")
        
        # Crucial wait: ASP.NET reloads the Court Location box when Municipality changes
        time.sleep(3) 

        # --- STEP 5: Find how many Locations are in this Municipality ---
        loc_select_element = driver.find_element(By.ID, "ctl00_MainContent_listBoxCourtOffice")
        num_locations = len(Select(loc_select_element).options)

        # --- STEP 6: Loop through Court Locations ---
        # Starting at 1 skips the "--- All Below ---" option so we get them one by one
        for j in range(1, num_locations):
            
            # Re-find the location listbox
            loc_select = Select(driver.find_element(By.ID, "ctl00_MainContent_listBoxCourtOffice"))
            location_name = loc_select.options[j].text
            loc_select.select_by_index(j)
            
            print(f"  -> Fetching data for: {location_name}")

            # --- STEP 7: Click Submit ---
            submit_btn = driver.find_element(By.ID, "ctl00_MainContent_btnSubmit")
            submit_btn.click()
            
            # Wait for the table to generate on the page
            time.sleep(3) 

            # --- STEP 8: Extract the Data ---
            try:
                # pandas reads all tables from the current HTML
                tables = pd.read_html(driver.page_source)
                
                # The actual court data is usually the last or largest table on the page
                if tables:
                    # Let's grab the last table (you can tweak this index [-1] if it grabs the wrong thing)
                    main_table = tables[-1] 
                    
                    # Add columns so you know exactly where this data came from in your master sheet
                    main_table['Municipality'] = municipality_name
                    main_table['Court_Location'] = location_name
                    
                    all_court_data.append(main_table)
                    print(f"      [Success] Data extracted!")
                else:
                    print(f"      [Notice] No tables found for this location.")
            except ValueError:
                 print(f"      [Notice] No readable data tables found on page.")
            except Exception as e:
                print(f"      [Error] Could not extract: {e}")

finally:
    # Always ensure the browser closes, even if the script crashes
    driver.quit()

# --- STEP 9: Save everything to one Master Excel file ---
print("\nCompiling master spreadsheet...")
if all_court_data:
    final_dataframe = pd.concat(all_court_data, ignore_index=True)
    final_dataframe.dropna(how="all", inplace=True) # Clean up empty rows
    
    output_filename = "Master_Ontario_Court_Dates.xlsx"
    final_dataframe.to_excel(output_filename, index=False)
    print(f"🎉 All done! Saved to {output_filename}")
else:
    print("⚠️ No data was collected. The tables might have a different structure.")