# scraper.py - Kibana scraper with integrated login
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
        self.kibana_username = os.environ.get('KIBANA_USERNAME')
        self.kibana_password = os.environ.get('KIBANA_PASSWORD')
        self.supabase_url = os.environ.get('SUPABASE_URL')
        self.supabase_key = os.environ.get('SUPABASE_ANON_KEY')
        
        # Validate required environment variables
        if not all([self.kibana_base_url, self.kibana_username, self.kibana_password]):
            raise Exception("Missing required Kibana environment variables: KIBANA_BASE_URL, KIBANA_USERNAME, KIBANA_PASSWORD")
        
        if not all([self.supabase_url, self.supabase_key]):
            raise Exception("Missing required Supabase environment variables: SUPABASE_URL, SUPABASE_ANON_KEY")
        
        # Initialize Supabase client
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
    
    async def login_to_kibana(self, page):
        """Login to Kibana using username/password"""
        logger.info("=== Starting Kibana Login ===")
        
        # Step 1: Navigate to base URL
        logger.info(f"Step 1: Navigating to {self.kibana_base_url}")
        await page.goto(self.kibana_base_url, timeout=30000)
        await page.wait_for_load_state('networkidle', timeout=15000)
        await page.screenshot(path='login_step1_initial.png')
        
        current_url = page.url
        page_title = await page.title()
        logger.info(f"After navigation - URL: {current_url}, Title: {page_title}")
        
        # Step 2: Look for and click "Log in with Elasticsearch"
        logger.info("Step 2: Looking for Elasticsearch login option")
        elasticsearch_selectors = [
            'text="Log in with Elasticsearch"',
            ':has-text("Log in with Elasticsearch")',
            'button:has-text("Elasticsearch")',
            '[data-test-subj="loginCard-elasticsearch"]'
        ]
        
        elasticsearch_button = None
        for selector in elasticsearch_selectors:
            try:
                logger.info(f"Trying selector: {selector}")
                elasticsearch_button = await page.wait_for_selector(selector, timeout=3000)
                if elasticsearch_button:
                    logger.info(f"Found Elasticsearch button with: {selector}")
                    break
            except:
                logger.info(f"Selector {selector} not found")
                continue
        
        if elasticsearch_button:
            logger.info("Clicking Elasticsearch login button")
            await elasticsearch_button.click()
            await page.wait_for_load_state('networkidle', timeout=10000)
            await page.screenshot(path='login_step2_elasticsearch_click.png')
            
            current_url = page.url
            page_title = await page.title()
            logger.info(f"After Elasticsearch click - URL: {current_url}, Title: {page_title}")
        else:
            logger.warning("No Elasticsearch login button found - proceeding to username/password")
        
        # Step 3: Find and fill username
        logger.info("Step 3: Looking for username field")
        username_selectors = [
            'input[name="username"]',
            'input[type="email"]',
            'input[type="text"]',
            'input[placeholder*="username"]',
            'input[placeholder*="email"]',
            '#username',
            '#email'
        ]
        
        username_field = None
        for selector in username_selectors:
            try:
                logger.info(f"Trying username selector: {selector}")
                username_field = await page.wait_for_selector(selector, timeout=3000)
                if username_field:
                    logger.info(f"Found username field with: {selector}")
                    break
            except:
                logger.info(f"Username selector {selector} not found")
                continue
        
        if not username_field:
            await page.screenshot(path='login_step3_no_username.png')
            content = await page.content()
            with open('login_step3_page_content.html', 'w') as f:
                f.write(content)
            raise Exception("Could not find username field")
        
        # Fill username using Playwright's fill() method
        await username_field.fill(self.kibana_username)
        logger.info(f"Filled username: {self.kibana_username}")
        await page.screenshot(path='login_step3_username_filled.png')
        
        # Step 4: Find and fill password
        logger.info("Step 4: Looking for password field")
        password_selectors = [
            'input[type="password"]',
            'input[name="password"]',
            '#password'
        ]
        
        password_field = None
        for selector in password_selectors:
            try:
                logger.info(f"Trying password selector: {selector}")
                password_field = await page.wait_for_selector(selector, timeout=3000)
                if password_field:
                    logger.info(f"Found password field with: {selector}")
                    break
            except:
                logger.info(f"Password selector {selector} not found")
                continue
        
        if not password_field:
            await page.screenshot(path='login_step4_no_password.png')
            raise Exception("Could not find password field")
        
        # Fill password using Playwright's fill() method
        await password_field.fill(self.kibana_password)
        logger.info("Filled password")
        await page.screenshot(path='login_step4_password_filled.png')
        
        # Step 5: Submit the form
        logger.info("Step 5: Submitting login form")
        submit_selectors = [
            'button[type="submit"]',
            'input[type="submit"]',
            'button:has-text("Log in")',
            'button:has-text("Sign in")',
            'button:has-text("Login")',
            'form button'
        ]
        
        submit_button = None
        for selector in submit_selectors:
            try:
                logger.info(f"Trying submit selector: {selector}")
                submit_button = await page.wait_for_selector(selector, timeout=3000)
                if submit_button:
                    logger.info(f"Found submit button with: {selector}")
                    break
            except:
                logger.info(f"Submit selector {selector} not found")
                continue
        
        if submit_button:
            await submit_button.click()
            logger.info("Clicked submit button")
        else:
            logger.info("No submit button found, trying Enter key")
            await password_field.press('Enter')
        
        await page.screenshot(path='login_step5_after_submit.png')
        
        # Step 6: Wait and verify login success
        logger.info("Step 6: Verifying login success")
        await page.wait_for_timeout(5000)  # Wait 5 seconds for redirect
        
        # Check for login success over multiple attempts
        for i in range(3):
            await page.wait_for_timeout(3000)
            await page.screenshot(path=f'login_step6_check_{i+1}.png')
            
            current_url = page.url
            page_title = await page.title()
            logger.info(f"Login check {i+1} - URL: {current_url}, Title: {page_title}")
            
            # Look for Kibana success indicators
            success_indicators = [
                '[data-test-subj="kibanaChrome"]',
                '.euiHeader',
                'nav[aria-label="Primary"]',
                '.kbnAppWrapper'
            ]
            
            for indicator in success_indicators:
                try:
                    element = await page.wait_for_selector(indicator, timeout=2000)
                    if element:
                        logger.info(f"LOGIN SUCCESS: Found Kibana UI element: {indicator}")
                        await page.screenshot(path='login_success_final.png')
                        return True
                except:
                    continue
            
            # Check for error messages
            error_selectors = [
                '.error',
                '.alert-danger',
                '.euiCallOut--danger',
                ':has-text("Invalid")',
                ':has-text("incorrect")',
                ':has-text("failed")'
            ]
            
            for error_selector in error_selectors:
                try:
                    error_element = await page.wait_for_selector(error_selector, timeout=1000)
                    if error_element:
                        error_text = await error_element.inner_text()
                        logger.error(f"Login error found: {error_text}")
                        await page.screenshot(path='login_error_found.png')
                        raise Exception(f"Login failed: {error_text}")
                except:
                    continue
            
            # Check if still on login page
            if "login" in current_url.lower() or "auth" in current_url.lower():
                logger.warning(f"Still on login page: {current_url}")
            else:
                logger.info(f"Redirected to: {current_url}")
                # If we're not on login page anymore, assume success
                logger.info("Login appears successful - not on login page")
                await page.screenshot(path='login_success_by_redirect.png')
                return True
        
        # Final check
        current_url = page.url
        if "login" in current_url.lower() or "auth" in current_url.lower():
            logger.error("Login failed - still on login page after multiple attempts")
            await page.screenshot(path='login_failed_final.png')
            content = await page.content()
            with open('login_failed_content.html', 'w') as f:
                f.write(content)
            raise Exception(f"Login failed - still on login page: {current_url}")
        else:
            logger.info("Login completed - assuming success")
            return True
    
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
                            
                        # Process more rows but with a reasonable limit to prevent infinite loops
                        if i >= 500:
                            logger.info("Processed 500 rows, stopping to prevent timeout")
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
                
                # Filter for target date if specified (disabled to get all 15-day data)
                # if target_date:
                #     target_date_obj = target_date.date()
                #     df = df[df['appointment_date'] == target_date_obj]
                #     logger.info(f"Filtered to {len(df)} appointments for {target_date_obj}")
                
                # Log the date range of appointments found
                if not df['appointment_date'].isna().all():
                    date_range = f"{df['appointment_date'].min()} to {df['appointment_date'].max()}"
                    logger.info(f"Found appointments from {date_range} (total: {len(df)})")
                
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
        
        # Convert data_date to string to avoid JSON serialization issues
        df['data_date'] = df['data_date'].astype(str)
        
        # Select final columns for dashboard - INCLUDE ALL AVAILABLE COLUMNS
        final_columns = [
            # Primary identifiers
            'booking_id',
            
            # Customer information
            'customer_name',
            'first_name', 
            'last_name',
            'email',
            
            # Service information
            'service_type',
            'offering_name',
            
            # Location information
            'location_name',
            'location_business_name',
            'location_id',
            'location_business_id',
            
            # Status and booking type
            'status',
            'booking_status_label',
            'is_google_booking',
            
            # DateTime fields
            'appointment_datetime',
            'start_date_time',
            'appointment_date',
            'appointment_time', 
            'appointment_time_12h',
            'time_field',
            
            # Duration fields (for future use)
            'end_date_time',
            'duration_minutes',
            
            # Metadata
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
            # Launch browser with debugging options
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor'
                ]
            )
            
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            page = await context.new_page()
            
            try:
                # Step 1: Login to Kibana
                await self.login_to_kibana(page)
                logger.info("Login completed successfully")
                
                # Step 2: Navigate to discover page with 15-day data
                await self.navigate_to_discover(page, target_date)
                
                # Step 3: Extract appointment data
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
                
                # Step 4: Process the data
                processed_df = self.process_appointment_data(raw_appointments, target_date)
                
                if not processed_df.empty:
                    # Step 5: Save to Supabase
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
                    await page.screenshot(path='scraping_final_error.png')
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
