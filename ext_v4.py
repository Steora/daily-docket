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
from io import StringIO

# 1. Set up the browser
options = webdriver.ChromeOptions()
# options.add_argument('--headless') # Uncomment this to hide the browser window while it runs
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 20)

all_court_data = [] 

try:
    print("Navigating to website and accepting terms...")
    driver.get("https://www.ontariocourtdates.ca/Default.aspx")
    
    wait.until(EC.element_to_be_clickable((By.ID, "ctl00_MainContent_chkAgree"))).click()
    wait.until(EC.element_to_be_clickable((By.ID, "ctl00_MainContent_btnEnter"))).click()
    wait.until(EC.presence_of_element_located((By.ID, "ctl00_MainContent_ddlCity")))
    time.sleep(2) 

    # --- LEVEL 1: COURTS ---
    num_courts = len(Select(driver.find_element(By.ID, "ctl00_MainContent_ddlCourt")).options)
    for c in range(num_courts):
        court_select = Select(driver.find_element(By.ID, "ctl00_MainContent_ddlCourt"))
        court_name = court_select.options[c].text
        
        # Skip the "Both" option so we only get them one by one
        if "Both" in court_name or "Select" in court_name: 
            continue
            
        court_select.select_by_index(c)
        time.sleep(2) # Wait for postback
        
        # --- LEVEL 2: MUNICIPALITIES ---
        num_muns = len(Select(driver.find_element(By.ID, "ctl00_MainContent_ddlCity")).options)
        for m in range(num_muns):
            mun_select = Select(driver.find_element(By.ID, "ctl00_MainContent_ddlCity"))
            mun_name = mun_select.options[m].text
            
            if "Select" in mun_name or "All" in mun_name:
                continue
                
            mun_select.select_by_index(m)
            time.sleep(3)
            
            # --- LEVEL 3: CASE TYPES / LINE OF BUSINESS ---
            num_lobs = len(Select(driver.find_element(By.ID, "ctl00_MainContent_ddlLob")).options)
            for l in range(num_lobs):
                lob_select = Select(driver.find_element(By.ID, "ctl00_MainContent_ddlLob"))
                lob_name = lob_select.options[l].text
                
                # Skip the "All" option to get them one by one
                if "All" in lob_name or "Select" in lob_name:
                    continue
                    
                lob_select.select_by_index(l)
                time.sleep(2)
                
                # --- LEVEL 4: COURT LOCATIONS ---
                num_locs = len(Select(driver.find_element(By.ID, "ctl00_MainContent_listBoxCourtOffice")).options)
                for j in range(num_locs):
                    loc_select = Select(driver.find_element(By.ID, "ctl00_MainContent_listBoxCourtOffice"))
                    loc_name = loc_select.options[j].text
                    
                    if "All Below" in loc_name:
                        continue
                        
                    print(f"\nSearching -> Court: {court_name} | Mun: {mun_name} | Type: {lob_name} | Loc: {loc_name}")
                    loc_select.select_by_index(j)
                    
                    # Click Submit
                    driver.find_element(By.ID, "ctl00_MainContent_btnSubmit").click()
                    time.sleep(6) # Wait for results page
                    
                    # Extract Data
                    try:
                        html_io = StringIO(driver.page_source)
                        tables = pd.read_html(html_io, flavor='lxml')
                        if tables:
                            main_table = tables[0] 
                            # Add our four new tracking columns!
                            main_table['Court'] = court_name
                            main_table['Case_Type'] = lob_name
                            main_table['Municipality'] = mun_name
                            main_table['Court_Location'] = loc_name
                            all_court_data.append(main_table)
                            print(f"  [Success] Data extracted!")
                        else:
                            print(f"  [Notice] No data for this combination.")
                    except ValueError:
                         print(f"  [Notice] No readable data tables found.")
                    except Exception as e:
                        print(f"  [Error] Could not extract: {e}")
                        
                    # Go back to the form
                    driver.back()
                    time.sleep(3)
                    wait.until(EC.presence_of_element_located((By.ID, "ctl00_MainContent_ddlCity")))
                    
                    # CRITICAL: Smarter rebuild of the dropdown state
                    
                    # 1. Re-select Court (only if it lost its state)
                    court_select = Select(driver.find_element(By.ID, "ctl00_MainContent_ddlCourt"))
                    if court_select.first_selected_option.text != court_name:
                        court_select.select_by_index(c)
                        time.sleep(2)
                    
                    # 2. Re-select Municipality (only if it lost its state)
                    mun_select = Select(driver.find_element(By.ID, "ctl00_MainContent_ddlCity"))
                    if mun_select.first_selected_option.text != mun_name:
                        mun_select.select_by_index(m)
                        time.sleep(3) # Give the site time to start its background refresh
                    
                    # 3. Re-select Case Type/LOB
                    lob_select = Select(driver.find_element(By.ID, "ctl00_MainContent_ddlLob"))
                    if lob_select.first_selected_option.text != lob_name:
                        # SMART WAIT: Force Selenium to wait until the dropdown has actually loaded 
                        # enough options to contain the index 'l' we are looking for.
                        wait.until(lambda d: len(Select(d.find_element(By.ID, "ctl00_MainContent_ddlLob")).options) > l)
                        
                        Select(driver.find_element(By.ID, "ctl00_MainContent_ddlLob")).select_by_index(l)
                        time.sleep(2)

finally:
    driver.quit()

# --- COMPILE AND EXPORT WITH AUTO-ADJUSTING COLUMNS ---
print("\nCompiling master spreadsheet...")
if all_court_data:
    final_dataframe = pd.concat(all_court_data, ignore_index=True)
    final_dataframe.dropna(how="all", inplace=True) 
    
    output_filename = f"Ontario_Court_Dockets_{date.today().strftime('%Y-%m-%d')}.xlsx"
    
    # Use Pandas ExcelWriter to auto-adjust column widths
    with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
        final_dataframe.to_excel(writer, index=False, sheet_name='Court Data')
        worksheet = writer.sheets['Court Data']
        
        # Auto-fit columns
        for idx, col in enumerate(final_dataframe.columns):
            max_len = max(
                final_dataframe[col].astype(str).map(len).max(),
                len(str(col))
            ) + 2
            worksheet.column_dimensions[chr(65 + idx)].width = max_len
            
    print(f"🎉 All done! Saved to {output_filename}")
else:
    print("⚠️ No data was collected.")