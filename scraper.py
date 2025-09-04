# scraper.py - Clean cookie-based Kibana scraper
import os
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from supabase import create_client, Client
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KibanaWebScraper:
    def __init__(self):
        """Initialize with environment variables"""
        self.kibana_base_url = os.environ.get('KIBANA_BASE_URL')
        self.supabase_url = os.environ.get('SUPABASE_URL')
        self.supabase_key = os.environ.get('SUPABASE_ANON_KEY')
        
        # Initialize Supabase client
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        
    async def setup_authenticated_session(self, page):
        """Set up authenticated session using existing cookie"""
        logger.info("Setting up authenticated session using existing cookie...")
        
        # Get the session cookie from environment variable
        session_cookie = os.environ.get('KIBANA_SESSION_COOKIE')
        if not session_cookie:
            raise Exception("KIBANA_SESSION_COOKIE environment variable not set")
        
        # First navigate to the Kibana domain to set the cookie context
        await page.goto(self.kibana_base_url, timeout=30000)
        
        # Set the authentication cookie with multiple possible names and domains
        cookie_configs = [
            {
                'name': 'sid',
                'value': session_cookie,
                'domain': '.aws.elastic-cloud.com',
                'path': '/',
                'httpOnly': True,
                'secure': True
            },
            {
                'name': 'sid',
                'value': session_cookie,
                'domain': 'ef337caea6fe4ab2832e7738e53d998f.ca-central-1.aws.elastic-cloud.com',
                'path': '/',
                'httpOnly': True,
                'secure': True
            },
            {
                'name': 'elastic_session',
                'value': session_cookie,
                'domain': '.aws.elastic-cloud.com',
                'path': '/',
                'httpOnly': True,
                'secure': True
            }
        ]
        
        for cookie_config in cookie_configs:
            try:
                await page.context.add_cookies([cookie_config])
                logger.info(f"Set cookie: {cookie_config['name']} for domain: {cookie_config['domain']}")
            except Exception as e:
                logger.warning(f"Failed to set cookie {cookie_config['name']}: {e}")
        
        logger.info("Session cookies set, testing authentication...")
        
        # Test the session by navigating to a basic Kibana page
        test_url = f"{self.kibana_base_url}/app/home"
        await page.goto(test_url, timeout=30000)
        await page.wait_for_load_state('networkidle', timeout=20000)
        
        # Take screenshot to verify
        await page.screenshot(path='session_test.png')
        
        # Check if we're authenticated (not redirected to login)
        current_url = page.url
        logger.info(f"After setting cookies, current URL: {current_url}")
        
        if "login" in current_url.lower() or "auth" in current_url.lower():
            await page.screenshot(path='cookie_auth_failed.png')
            raise Exception(f"Cookie authentication failed - still at login: {current_url}")
        
        # Look for Kibana UI elements to confirm we're authenticated
        kibana_indicators = [
            '[data-test-subj="kibanaChrome"]',
            '.kbnAppWrapper',
            'nav[aria-label="Primary"]',
            '.euiHeader',
            '.globalNav'
        ]
        
        session_verified = False
        for selector in kibana_indicators:
            try:
                await page.wait_for_selector(selector, timeout=5000)
                logger.info(f"Authentication verified - found Kibana UI: {selector}")
                session_verified = True
                break
            except:
                continue
        
        if not session_verified:
            await page.screenshot(path='no_kibana_ui_after_cookie.png')
            logger.warning("Could not verify Kibana session with cookie - continuing anyway")
        
        logger.info("Cookie-based authentication completed")
    
    async def navigate_to_discover(self, page, target_date=None):
        """Navigate to the discover page with 15-day appointment data"""
        # Use the 15-day rolling window URL which is more reliable
        discover_url = f"{self.kibana_base_url}/app/discover#/view/84b881a0-6b52-11f0-89e0-f9470fca93e5?_g=(filters%3A!()%2CrefreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A(from%3Anow-15d%2Cto%3Anow))"
        
        logger.info(f"Navigating to 15-day appointment data view...")
        logger.info(f"Target URL: {discover_url}")
        
        try:
            await page.goto(discover_url, timeout=45000)
            
            # Wait for the page to load completely
            await page.wait_for_load_state('networkidle', timeout=30000)
            
            # Check if we got redirected back to login
            current_url = page.url
            logger.info(f"Current page URL: {current_url}")
            
            if "login" in current_url.lower() or "auth" in current_url.lower():
                await page.screenshot(path='redirected_to_login.png')
                raise Exception(f"Got redirected to login when trying to access discover page: {current_url}")
            
            logger.info("Discover page loaded")
            
            # Take screenshot after navigation
            await page.screenshot(path='discover_loaded.png')
            
            # Wait for data table to appear
            table_selectors = [
                '[data-test-subj="discoverDocTable"]',
                '.euiDataGrid',
                '.kuiTable',
                'table'
            ]
            
            table_found = False
            for selector in table_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=15000)
                    logger.info(f"Found data table with selector: {selector}")
                    table_found = True
                    break
                except:
                    continue
            
            if not table_found:
                await page.screenshot(path='no_table_found.png')
                
                # Save page content for debugging
                content = await page.content()
                with open('discover_page_debug.html', 'w') as f:
                    f.write(content)
                
                # Get page title and any error messages
                page_title = await page.title()
                logger.error(f"No data table found on page: {page_title}")
                logger.error(f"Current URL: {current_url}")
                
                # Look for any error messages on the page
                try:
                    error_text = await page.evaluate('''() => {
                        const errors = document.querySelectorAll('.euiCallOut--danger, .error, .alert-danger, [data-test-subj="discoverNoResults"]');
                        return Array.from(errors).map(el => el.innerText).join('; ');
                    }''')
                    if error_text:
                        logger.error(f"Error messages on page: {error_text}")
                except:
                    pass
                
                raise Exception(f"No data table found on discover page. URL: {current_url}, Title: {page_title}")
            
            # Wait a bit more for data to populate
            logger.info("Waiting for data to populate...")
            await page.wait_for_timeout(8000)
            
            # Final screenshot
            await page.screenshot(path='discover_ready.png')
            logger.info("Discover page ready for data extraction")
            
        except Exception as e:
            # Take error screenshot and save page content
            await page.screenshot(path='discover_navigation_error.png')
            
            try:
                content = await page.content()
                with open('discover_navigation_error.html', 'w') as f:
                    f.write(content)
                    
                current_url = page.url
                page_title = await page.title()
                logger.error(f"Navigation failed. URL: {current_url}, Title: {page_title}")
            except:
                pass
                
            raise
    
    async def extract_appointment_data(self, page):
        """Extract appointment data from the Kibana discover table"""
        logger.info("Extracting appointment data from page...")
        
        # Always take a screenshot before extraction for debugging
        await page.screenshot(path='before_extraction.png')
        
        # Log current URL and page title
        current_url = page.url
        page_title = await page.title()
        logger.info(f"Extracting from URL: {current_url}")
        logger.info(f"Page title: {page_title}")
        
        appointments = []
        
        try:
            # Look for tables using multiple selectors
            table_selectors = [
                'table',
                '[data-test-subj="discoverDocTable"] table',
                '.euiDataGrid',
                '.kuiTable table',
                '.discover-table table'
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
                # Try to get any table on the page
                tables = await page.query_selector_all('table')
                if tables:
                    table_element = tables[0]
                    logger.info("Using first table found on page")
                else:
                    await page.screenshot(path='no_table_elements.png')
                    
                    # Save page content for debugging
                    content = await page.content()
                    with open('extraction_page_debug.html', 'w') as f:
                        f.write(content)
                    
                    # Get all text content for debugging
                    try:
                        page_text = await page.evaluate('''() => {
                            return document.body.innerText;
                        }''')
                        
                        with open('page_text_content.txt', 'w') as f:
                            f.write(page_text)
                        
                        logger.info("Saved page text content for debugging")
                    except Exception as e2:
                        logger.error(f"Failed to extract page text: {e2}")
                    
                    raise Exception(f"No table elements found on page: {current_url}")
            
            # Extract all rows from the table
            rows = await table_element.query_selector_all('tr')
            logger.info(f"Found {len(rows)} table rows")
            
            if len(rows) < 2:  # Need at least header + 1 data row
                await page.screenshot(path='empty_table.png')
                logger.warning("Table found but no data rows")
                return []
            
            # Extract headers from first row
            header_row = rows[0]
            header_cells = await header_row.query_selector_all('th, td')
            headers = []
            
            for cell in header_cells:
                text = await cell.inner_text()
                headers.append(text.strip())
            
            logger.info(f"Table headers: {headers}")
            
            # Extract data rows (skip header row)
            for i, row in enumerate(rows[1:], 1):
                try:
                    cells = await row.query_selector_all('td, th')
                    if len(cells) == 0:
                        continue
                    
                    row_data = []
                    for cell in cells:
                        text = await cell.inner_text()
                        row_data.append(text.strip())
                    
                    # Only process rows that have the expected number of columns
                    if len(row_data) >= len(headers):
                        appointment = {}
                        for j, header in enumerate(headers):
                            if j < len(row_data):
                                appointment[header] = row_data[j]
                        
                        # Skip empty or invalid rows
                        if appointment.get('bookingId') and appointment.get('bookingId') != '-':
                            appointments.append(appointment)
                            
                        # Limit to prevent timeout - process first 100 rows
                        if i >= 100:
                            logger.info("Processed 100 rows, stopping to prevent timeout")
                            break
                            
                except Exception as e:
                    logger.warning(f"Error processing row {i}: {e}")
                    continue
            
            logger.info(f"Successfully extracted {len(appointments)} appointments")
            
            # Take a screenshot showing the extracted data
            await page.screenshot(path='data_extracted.png')
            
            # Save extracted data for debugging
            with open('extracted_data.json', 'w') as f:
                json.dump(appointments, f, indent=2)
            
            return appointments
            
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            await page.screenshot(path='extraction_error.png')
            
            # Always save debugging info on errors
            try:
                content = await page.content()
                with open('extraction_error_page.html', 'w') as f:
                    f.write(content)
            except:
                pass
            
            raise  # Re-raise the error so the main function knows it failed
    
    def process_appointment_data(self, raw_appointments, target_date):
        """Process raw scraped data into clean format"""
        if not raw_appointments:
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(raw_appointments)
        logger.info(f"Processing {len(df)} raw appointments from 15-day data")
        logger.info(f"Columns found: {list(df.columns)}")
        
        if df.empty:
            return df
        
        # Clean up column names and map to standard format
        column_mapping = {
            'bookingId': 'booking_id',
            'client.firstName': 'first_name',
            'client.lastName': 'last_name',
            'client.email': 'email',
            'offering.name': 'service_type',
            'startDateTime': 'appointment_datetime',
            'location.businessName': 'location_name',
            'location.businessId': 'location_id',
            'bookingStatus.label': 'status',
            'isGoogleBooking': 'is_google_booking'
        }
        
        # Apply mappings for available columns
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df[new_col] = df[old_col]
        
        # Create customer name from first and last name
        if 'first_name' in df.columns and 'last_name' in df.columns:
            df['customer_name'] = (df['first_name'].fillna('') + ' ' + df['last_name'].fillna('')).str.strip()
        
        # Parse appointment datetime
        if 'appointment_datetime' in df.columns:
            try:
                # Handle different date formats that might appear
                df['appointment_datetime'] = pd.to_datetime(df['appointment_datetime'], errors='coerce')
                df['appointment_date'] = df['appointment_datetime'].dt.date
                df['appointment_time'] = df['appointment_datetime'].dt.strftime('%H:%M')
                df['appointment_time_12h'] = df['appointment_datetime'].dt.strftime('%I:%M %p')
                
                # Filter for target date if specified
                if target_date:
                    target_date_obj = target_date.date()
                    df = df[df['appointment_date'] == target_date_obj]
                    logger.info(f"Filtered to {len(df)} appointments for {target_date_obj}")
                
            except Exception as e:
                logger.warning(f"Error parsing appointment_datetime: {e}")
                # If datetime parsing fails, don't filter by date
                df['appointment_date'] = None
                df['appointment_time'] = None
                df['appointment_time_12h'] = None
        
        # Clean up booking IDs - remove any non-numeric entries
        if 'booking_id' in df.columns:
            df['booking_id'] = df['booking_id'].astype(str)
            # Filter out rows where booking_id is not a valid number
            df = df[df['booking_id'].str.isdigit()]
        
        # Filter out canceled appointments if desired (optional)
        if 'status' in df.columns:
            # Keep all statuses for now, but log the distribution
            status_counts = df['status'].value_counts()
            logger.info(f"Status distribution: {status_counts.to_dict()}")
        
        # Add metadata
        df['extracted_at'] = datetime.utcnow()
        df['data_date'] = target_date.date() if target_date else datetime.now().date()
        
        # Select final columns for dashboard
        final_columns = [
            'booking_id',
            'customer_name', 
            'email',
            'service_type',
            'appointment_date',
            'appointment_time',
            'appointment_time_12h',
            'appointment_datetime',
            'location_name',
            'location_id',
            'status',
            'is_google_booking',
            'extracted_at',
            'data_date'
        ]
        
        # Only include columns that exist
        available_columns = [col for col in final_columns if col in df.columns]
        result_df = df[available_columns] if available_columns else df
        
        # Remove any completely empty rows
        result_df = result_df.dropna(how='all')
        
        # Sort by appointment time if available
        if 'appointment_datetime' in result_df.columns:
            result_df = result_df.sort_values('appointment_datetime')
        elif 'booking_id' in result_df.columns:
            result_df = result_df.sort_values('booking_id')
        
        logger.info(f"Final processed DataFrame: {result_df.shape}")
        logger.info(f"Final columns: {list(result_df.columns)}")
        
        if not result_df.empty:
            logger.info(f"Sample row: {result_df.iloc[0].to_dict()}")
        
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
                # Set up authenticated session using cookie
                await self.setup_authenticated_session(page)
                
                # Navigate to discover page with 15-day data
                await self.navigate_to_discover(page, target_date)
                
                # Extract appointment data
                raw_appointments = await self.extract_appointment_data(page)
                
                # Check if we actually got data
                if not raw_appointments:
                    # Take final screenshot for debugging
                    await page.screenshot(path='final_no_data.png')
                    
                    logger.error("No appointment data was extracted from the page")
                    return {
                        'status': 'error',
                        'error': 'No appointment data found on the page',
                        'date': target_date.date().isoformat(),
                        'url': page.url
                    }
                
                # Process the data
                processed_df = self.process_appointment_data(raw_appointments, target_date)
                
                if not processed_df.empty:
                    # Save to Supabase
                    self.save_to_supabase(processed_df)
                    
                    logger.info("Daily scraping completed successfully")
                    return {
                        'status': 'success',
                        'records_processed': len(processed_df),
                        'raw_records_found': len(raw_appointments),
                        'date': target_date.date().isoformat(),
                        'url': page.url
                    }
                else:
                    logger.warning("Raw data found but no records after processing/filtering")
                    return {
                        'status': 'warning',
                        'records_processed': 0,
                        'raw_records_found': len(raw_appointments),
                        'date': target_date.date().isoformat(),
                        'message': 'Data found but no records for target date after filtering',
                        'url': page.url
                    }
                    
            except Exception as e:
                logger.error(f"Scraping failed: {e}")
                
                # Take final error screenshot
                try:
                    await page.screenshot(path='final_error.png')
                    current_url = page.url
                except:
                    current_url = "Unable to get URL"
                
                return {
                    'status': 'error',
                    'error': str(e),
                    'date': target_date.date().isoformat(),
                    'url': current_url
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
