import os
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from supabase import create_client, Client
import json
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KibanaWebScraper:
    def __init__(self):
        """Initialize with environment variables"""
        self.kibana_base_url = os.environ.get('KIBANA_BASE_URL')
        self.kibana_username = os.environ.get('KIBANA_USERNAME')
        self.kibana_password = os.environ.get('KIBANA_PASSWORD')
        self.supabase_url = os.environ.get('SUPABASE_URL')
        self.supabase_key = os.environ.get('SUPABASE_ANON_KEY')
        
        # Initialize Supabase client
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        
    async def login_to_kibana(self, page):
        """Login to Kibana using credentials"""
        logger.info("Navigating to Kibana login page...")
        
        # Go to main Kibana URL - it should redirect to login
        await page.goto(self.kibana_base_url)
        
        # Wait for login form to appear
        try:
            # Look for various login form selectors
            await page.wait_for_selector('input[type="email"], input[name="username"], input[data-test-subj="loginUsername"]', timeout=10000)
            
            # Fill in username/email
            username_selector = None
            for selector in ['input[type="email"]', 'input[name="username"]', 'input[data-test-subj="loginUsername"]']:
                try:
                    await page.wait_for_selector(selector, timeout=2000)
                    username_selector = selector
                    break
                except:
                    continue
            
            if username_selector:
                await page.fill(username_selector, self.kibana_username)
                logger.info("Filled username")
            else:
                raise Exception("Could not find username field")
            
            # Fill in password
            password_selector = None
            for selector in ['input[type="password"]', 'input[name="password"]', 'input[data-test-subj="loginPassword"]']:
                try:
                    await page.wait_for_selector(selector, timeout=2000)
                    password_selector = selector
                    break
                except:
                    continue
                    
            if password_selector:
                await page.fill(password_selector, self.kibana_password)
                logger.info("Filled password")
            else:
                raise Exception("Could not find password field")
            
            # Click login button
            login_button_selector = None
            for selector in ['button[type="submit"]', 'button[data-test-subj="loginSubmit"]', 'input[type="submit"]']:
                try:
                    await page.wait_for_selector(selector, timeout=2000)
                    login_button_selector = selector
                    break
                except:
                    continue
                    
            if login_button_selector:
                await page.click(login_button_selector)
                logger.info("Clicked login button")
            else:
                raise Exception("Could not find login button")
            
            # Wait for successful login (look for Kibana dashboard elements)
            await page.wait_for_selector('[data-test-subj="kibanaChrome"]', timeout=15000)
            logger.info("Successfully logged into Kibana")
            
        except Exception as e:
            logger.error(f"Login failed: {e}")
            # Take screenshot for debugging
            await page.screenshot(path='login_error.png')
            raise
    
    async def navigate_to_discover(self, page, target_date=None):
        """Navigate to the discover page with appointment data"""
        if target_date is None:
            target_date = datetime.now()
            
        # Calculate date range for URL
        start_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=1)
        
        # Your original discover URL with dynamic date range
        discover_url = f"{self.kibana_base_url}/app/discover#/view/84b881a0-6b52-11f0-89e0-f9470fca93e5?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:'{start_date.isoformat()}Z',to:'{end_date.isoformat()}Z'))&_a=(columns:!(bookingId,location.businessName,location.businessId,client.lastName,isGoogleBooking,offering.name,client.firstName,client.email,bookingStatus.label,startDateTime),filters:!(),index:a8e8cdc8-c993-429b-b378-d2bfa0589440,interval:auto,query:(language:kuery,query:''),sort:!(!(utcCreatedDateTime,asc)))"
        
        logger.info(f"Navigating to discover page for {target_date.date()}")
        await page.goto(discover_url)
        
        # Wait for the data to load
        await page.wait_for_selector('[data-test-subj="discoverDocTable"], .euiDataGrid', timeout=30000)
        logger.info("Discover page loaded")
        
        # Wait a bit more for data to populate
        await page.wait_for_timeout(5000)
    
    async def extract_appointment_data(self, page):
        """Extract appointment data from the Kibana discover table"""
        logger.info("Extracting appointment data from page...")
        
        appointments = []
        
        try:
            # Look for different table structures that Kibana might use
            table_selectors = [
                '[data-test-subj="discoverDocTable"]',
                '.euiDataGrid',
                '.kuiTable',
                '.discover-table'
            ]
            
            table_element = None
            for selector in table_selectors:
                try:
                    table_element = await page.wait_for_selector(selector, timeout=5000)
                    if table_element:
                        logger.info(f"Found table with selector: {selector}")
                        break
                except:
                    continue
            
            if not table_element:
                # Fallback: try to extract from any table on the page
                tables = await page.query_selector_all('table')
                if tables:
                    table_element = tables[0]
                    logger.info("Using fallback table selector")
                else:
                    raise Exception("No table found on page")
            
            # Extract headers
            headers = []
            header_selectors = ['th', '.euiDataGridHeaderCell', '.kuiTableHeaderCell']
            for header_selector in header_selectors:
                header_elements = await table_element.query_selector_all(header_selector)
                if header_elements:
                    for header in header_elements:
                        text = await header.inner_text()
                        headers.append(text.strip())
                    break
            
            logger.info(f"Found headers: {headers}")
            
            # Extract rows
            row_selectors = ['tbody tr', '.euiDataGridRow', '.kuiTableRow']
            for row_selector in row_selectors:
                rows = await table_element.query_selector_all(row_selector)
                if rows:
                    logger.info(f"Found {len(rows)} rows")
                    
                    for row in rows[:50]:  # Limit to first 50 rows to avoid timeout
                        try:
                            # Extract cells from this row
                            cell_selectors = ['td', '.euiDataGridRowCell', '.kuiTableRowCell']
                            cells = []
                            
                            for cell_selector in cell_selectors:
                                cell_elements = await row.query_selector_all(cell_selector)
                                if cell_elements:
                                    for cell in cell_elements:
                                        text = await cell.inner_text()
                                        cells.append(text.strip())
                                    break
                            
                            if cells and len(cells) >= len(headers):
                                # Create appointment record
                                appointment = {}
                                for i, header in enumerate(headers):
                                    if i < len(cells):
                                        appointment[header] = cells[i]
                                
                                appointments.append(appointment)
                                
                        except Exception as e:
                            logger.warning(f"Error processing row: {e}")
                            continue
                    
                    break  # Found rows, stop trying other selectors
            
            logger.info(f"Extracted {len(appointments)} appointments")
            return appointments
            
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            # Take screenshot for debugging
            await page.screenshot(path='extraction_error.png')
            
            # Try alternative approach: get all text and parse
            page_content = await page.content()
            with open('page_debug.html', 'w') as f:
                f.write(page_content)
            
            return []
    
    def process_appointment_data(self, raw_appointments, target_date):
        """Process raw scraped data into clean format"""
        if not raw_appointments:
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(raw_appointments)
        logger.info(f"Processing {len(df)} raw appointments")
        logger.info(f"Columns found: {list(df.columns)}")
        
        # Map common column names to standard format
        column_mapping = {
            'client.firstName': 'first_name',
            'client.lastName': 'last_name',
            'client.email': 'email',
            'offering.name': 'service_type',
            'startDateTime': 'appointment_datetime',
            'location.businessName': 'location_name',
            'location.businessId': 'location_id',
            'bookingStatus.label': 'status',
            'bookingId': 'booking_id',
            # Add any other mappings based on what you see
        }
        
        # Apply mappings
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df[new_col] = df[old_col]
        
        # Create customer name
        if 'first_name' in df.columns and 'last_name' in df.columns:
            df['customer_name'] = (df['first_name'].fillna('') + ' ' + df['last_name'].fillna('')).str.strip()
        elif 'client.firstName' in df.columns and 'client.lastName' in df.columns:
            df['customer_name'] = (df['client.firstName'].fillna('') + ' ' + df['client.lastName'].fillna('')).str.strip()
        
        # Parse appointment datetime
        datetime_columns = ['appointment_datetime', 'startDateTime', 'utcStartDateTime']
        for col in datetime_columns:
            if col in df.columns:
                try:
                    df['appointment_datetime'] = pd.to_datetime(df[col])
                    df['appointment_date'] = df['appointment_datetime'].dt.date
                    df['appointment_time'] = df['appointment_datetime'].dt.strftime('%H:%M')
                    df['appointment_time_12h'] = df['appointment_datetime'].dt.strftime('%I:%M %p')
                    break
                except:
                    continue
        
        # Add metadata
        df['extracted_at'] = datetime.utcnow()
        df['data_date'] = target_date.date()
        
        # Select final columns
        final_columns = [
            'booking_id', 'customer_name', 'email', 'service_type',
            'appointment_date', 'appointment_time', 'appointment_time_12h',
            'appointment_datetime', 'location_name', 'location_id', 'status',
            'extracted_at', 'data_date'
        ]
        
        available_columns = [col for col in final_columns if col in df.columns]
        result_df = df[available_columns] if available_columns else df
        
        logger.info(f"Processed DataFrame shape: {result_df.shape}")
        logger.info(f"Final columns: {list(result_df.columns)}")
        
        return result_df
    
    def save_to_supabase(self, df):
        """Save processed data to Supabase"""
        if df.empty:
            logger.info("No data to save")
            return
            
        try:
            # Convert DataFrame to list of dictionaries
            data_to_insert = df.to_dict('records')
            
            # Convert datetime objects to strings for JSON serialization
            for record in data_to_insert:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif isinstance(value, (pd.Timestamp, datetime)):
                        record[key] = value.isoformat()
                    elif hasattr(value, 'date') and callable(getattr(value, 'date')):
                        record[key] = value.date().isoformat()
            
            # Insert data into Supabase
            result = self.supabase.table('daily_appointments').upsert(
                data_to_insert,
                on_conflict='booking_id,data_date'
            ).execute()
            
            logger.info(f"Successfully saved {len(data_to_insert)} records to Supabase")
            return result
            
        except Exception as e:
            logger.error(f"Error saving to Supabase: {e}")
            raise
    
    async def run_daily_scraping(self, target_date=None):
        """Main method to run daily scraping"""
        if target_date is None:
            target_date = datetime.now()
        
        logger.info(f"Starting web scraping for {target_date.date()}")
        
        async with async_playwright() as p:
            # Launch browser (use headless=False for debugging)
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                # Login to Kibana
                await self.login_to_kibana(page)
                
                # Navigate to discover page
                await self.navigate_to_discover(page, target_date)
                
                # Extract appointment data
                raw_appointments = await self.extract_appointment_data(page)
                
                # Process the data
                processed_df = self.process_appointment_data(raw_appointments, target_date)
                
                if not processed_df.empty:
                    # Save to Supabase
                    self.save_to_supabase(processed_df)
                    
                    logger.info("Daily scraping completed successfully")
                    return {
                        'status': 'success',
                        'records_processed': len(processed_df),
                        'date': target_date.date().isoformat()
                    }
                else:
                    logger.info("No appointments found")
                    return {
                        'status': 'success',
                        'records_processed': 0,
                        'date': target_date.date().isoformat(),
                        'message': 'No appointments found'
                    }
                    
            except Exception as e:
                logger.error(f"Scraping failed: {e}")
                return {
                    'status': 'error',
                    'error': str(e),
                    'date': target_date.date().isoformat()
                }
            finally:
                await browser.close()

async def main():
    """Main entry point"""
    scraper = KibanaWebScraper()
    result = await scraper.run_daily_scraping()
    
    print(f"Scraping result: {json.dumps(result, indent=2)}")
    
    if result['status'] == 'error':
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())
