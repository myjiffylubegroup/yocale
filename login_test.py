# login_test.py - Simple script to test Kibana login
import os
import asyncio
from playwright.async_api import async_playwright
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KibanaLoginTester:
    def __init__(self):
        """Initialize with environment variables"""
        self.kibana_base_url = os.environ.get('KIBANA_BASE_URL')
        self.kibana_username = os.environ.get('KIBANA_USERNAME')
        self.kibana_password = os.environ.get('KIBANA_PASSWORD')
        
        if not all([self.kibana_base_url, self.kibana_username, self.kibana_password]):
            raise Exception("Missing required environment variables")
    
    async def test_login(self, page):
        """Test the complete login flow with detailed debugging"""
        logger.info("=== Starting Kibana Login Test ===")
        
        # Step 1: Navigate to base URL
        logger.info(f"Step 1: Navigating to {self.kibana_base_url}")
        await page.goto(self.kibana_base_url, timeout=30000)
        await page.wait_for_load_state('networkidle', timeout=15000)
        await page.screenshot(path='step1_initial_page.png')
        
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
            await page.screenshot(path='step2_after_elasticsearch_click.png')
            
            current_url = page.url
            page_title = await page.title()
            logger.info(f"After Elasticsearch click - URL: {current_url}, Title: {page_title}")
        else:
            logger.warning("No Elasticsearch login button found")
        
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
            await page.screenshot(path='step3_no_username_field.png')
            # Save page content for debugging
            content = await page.content()
            with open('step3_page_content.html', 'w') as f:
                f.write(content)
            raise Exception("Could not find username field")
        
        # Clear and fill username
        await username_field.clear()
        await username_field.fill(self.kibana_username)
        logger.info(f"Filled username: {self.kibana_username}")
        await page.screenshot(path='step3_username_filled.png')
        
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
            await page.screenshot(path='step4_no_password_field.png')
            raise Exception("Could not find password field")
        
        # Clear and fill password
        await password_field.clear()
        await password_field.fill(self.kibana_password)
        logger.info("Filled password")
        await page.screenshot(path='step4_password_filled.png')
        
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
        
        await page.screenshot(path='step5_after_submit.png')
        
        # Step 6: Wait and check for login success
        logger.info("Step 6: Checking for login success")
        await page.wait_for_timeout(5000)  # Wait 5 seconds for redirect
        
        # Take several screenshots over time to see what happens
        for i in range(3):
            await page.wait_for_timeout(3000)
            await page.screenshot(path=f'step6_check_{i+1}.png')
            
            current_url = page.url
            page_title = await page.title()
            logger.info(f"Check {i+1} - URL: {current_url}, Title: {page_title}")
            
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
                        logger.info(f"SUCCESS: Found Kibana UI element: {indicator}")
                        await page.screenshot(path='login_success.png')
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
                        await page.screenshot(path='login_error.png')
                        return False
                except:
                    continue
            
            # Check if still on login page
            if "login" in current_url.lower() or "auth" in current_url.lower():
                logger.warning(f"Still on login page: {current_url}")
            else:
                logger.info(f"Redirected to: {current_url}")
        
        # Final check
        current_url = page.url
        if "login" in current_url.lower() or "auth" in current_url.lower():
            logger.error("Login appears to have failed - still on login page")
            await page.screenshot(path='login_failed_final.png')
            # Save final page content
            content = await page.content()
            with open('login_failed_content.html', 'w') as f:
                f.write(content)
            return False
        else:
            logger.info("Login may have succeeded - not on login page")
            await page.screenshot(path='login_maybe_success.png')
            return True

async def main():
    """Main test function"""
    tester = KibanaLoginTester()
    
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
            success = await tester.test_login(page)
            
            if success:
                print("✅ LOGIN SUCCESS!")
                print("The login process completed successfully.")
            else:
                print("❌ LOGIN FAILED!")
                print("Check the debug screenshots and HTML files for details.")
                
        except Exception as e:
            print(f"❌ LOGIN ERROR: {e}")
            await page.screenshot(path='login_exception.png')
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
