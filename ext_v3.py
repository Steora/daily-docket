from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
from datetime import date
from io import StringIO # <-- Added this!

# 1. Set up the browser
options = webdriver.ChromeOptions()
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 20)

all_court_data = [] 

try:
    print("Navigating to website and accepting terms...")
    driver.get("https://www.ontariocourtdates.ca/Default.aspx")
    
    # Check the 'I Agree' box and click Enter (Agree repaints the DOM—avoid stale refs on Enter)
    wait.until(EC.element_to_be_clickable((By.ID, "ctl00_MainContent_chkAgree"))).click()
    time.sleep(0.8)
    wait.until(
        lambda d: d.execute_script(
            "var e=document.getElementById('ctl00_MainContent_btnEnter');"
            "return !!(e && !e.disabled);"
        )
    )
    driver.execute_script(
        "document.getElementById('ctl00_MainContent_btnEnter').click();"
    )
    
    # Wait until the daily docket page loads
    wait.until(EC.presence_of_element_located((By.ID, "ctl00_MainContent_ddlCity")))
    time.sleep(2) 

    # Find how many Municipalities there are
    mun_select_element = driver.find_element(By.ID, "ctl00_MainContent_ddlCity") 
    num_municipalities = len(Select(mun_select_element).options)

    # Loop through Municipalities
    for i in range(1, num_municipalities):
        
        # --- RETURN TO FORM ---
        # Ensure we set the static dropdowns every time we return to the form
        wait.until(EC.presence_of_element_located((By.ID, "ctl00_MainContent_ddlCity")))
        Select(driver.find_element(By.ID, "ctl00_MainContent_ddlCourt")).select_by_visible_text("Both")
        Select(driver.find_element(By.ID, "ctl00_MainContent_ddlLob")).select_by_visible_text("All")

        # Select Municipality
        mun_select = Select(driver.find_element(By.ID, "ctl00_MainContent_ddlCity"))
        municipality_name = mun_select.options[i].text
        mun_select.select_by_index(i)
        
        print(f"\nProcessing Municipality: {municipality_name}")
        time.sleep(4) 

        # Find how many Locations are in this Municipality
        loc_select_element = wait.until(EC.presence_of_element_located((By.ID, "ctl00_MainContent_listBoxCourtOffice")))
        num_locations = len(Select(loc_select_element).options)

        # Loop through Locations
        for j in range(0, num_locations):
            
            # --- We must re-select the municipality and static dropdowns if we went back ---
            if j > 0: 
                wait.until(EC.presence_of_element_located((By.ID, "ctl00_MainContent_ddlCity")))
                Select(driver.find_element(By.ID, "ctl00_MainContent_ddlCourt")).select_by_visible_text("Both")
                Select(driver.find_element(By.ID, "ctl00_MainContent_ddlLob")).select_by_visible_text("All")
                
                mun_select = Select(driver.find_element(By.ID, "ctl00_MainContent_ddlCity"))
                mun_select.select_by_index(i)
                time.sleep(3) # Wait for location box to reload

            # Select Location
            loc_select = Select(driver.find_element(By.ID, "ctl00_MainContent_listBoxCourtOffice"))
            location_name = loc_select.options[j].text
            
            if "All Below" in location_name:
                continue
                
            loc_select.select_by_index(j)
            print(f"  -> Fetching data for: {location_name}")

            # Click Submit
            driver.find_element(By.ID, "ctl00_MainContent_btnSubmit").click()
            time.sleep(6) # Wait for results page

            # --- EXTRACT DATA ---
            try:
                # Wrap page_source in StringIO to prevent "[Errno 2]" file path errors
                html_io = StringIO(driver.page_source)
                tables = pd.read_html(html_io, flavor='lxml')
                
                # Check if we found tables. The actual data table is usually index 0 on the results page.
                if tables:
                    main_table = tables[0] # Grab the first table on the results page
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

            # --- GO BACK TO THE FORM ---
            driver.back()
            time.sleep(3) # Wait for the form to reload before the next loop

finally:
    driver.quit()

print("\nCompiling master spreadsheet...")
if all_court_data:
    final_dataframe = pd.concat(all_court_data, ignore_index=True)
    final_dataframe.dropna(how="all", inplace=True)
    out_name = f"Ontario_Court_Dates_{date.today().strftime('%Y-%m-%d')}.xlsx"
    final_dataframe.to_excel(out_name, index=False)
    print(f"🎉 All done! Saved to {out_name}")
else:
    print("⚠️ No data was collected.")