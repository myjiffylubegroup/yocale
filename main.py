

# ===================================
# main.py - Main data extraction script
# ===================================

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import quote
import json
from supabase import create_client, Client
from io import StringIO
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CloudAppointmentExtractor:
    def __init__(self):
        """Initialize with environment variables"""
        self.kibana_base_url = os.environ.get('KIBANA_BASE_URL')
        self.supabase_url = os.environ.get('SUPABASE_URL')
        self.supabase_key = os.environ.get('SUPABASE_ANON_KEY')
        
        # Initialize Supabase client
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        
        # Initialize requests session
        self.session = requests.Session()
        
        # Add authentication - try multiple methods
        auth_token = os.environ.get('KIBANA_AUTH_TOKEN')
        session_cookie = os.environ.get('KIBANA_SESSION_COOKIE')
        
        if session_cookie:
            # Use session cookie authentication
            self.session.cookies.set('sid', session_cookie, domain='.aws.elastic-cloud.com')
            logger.info("Using session cookie authentication")
        elif auth_token:
            # Use bearer token authentication
            self.session.headers.update({'Authorization': f'Bearer {auth_token}'})
            logger.info("Using bearer token authentication")
        else:
            logger.warning("No authentication method provided")
    
    def extract_daily_appointments(self, target_date=None):
        """Extract appointments for a specific date"""
        if target_date is None:
            target_date = datetime.now()
            
        logger.info(f"Extracting appointments for {target_date.date()}")
        
        try:
            # Build the export URL with today's date
            export_url = self._build_daily_export_url(target_date)
            
            # Fetch data from Kibana
            response = self.session.get(export_url, timeout=60)
            response.raise_for_status()
            
            # Parse CSV response
            csv_data = StringIO(response.text)
            df = pd.read_csv(csv_data)
            
            logger.info(f"Retrieved {len(df)} raw records")
            
            # Process and clean the data
            processed_df = self._process_appointment_data(df, target_date)
            
            logger.info(f"Processed {len(processed_df)} appointment records")
            
            return processed_df
            
        except Exception as e:
            logger.error(f"Error extracting appointments: {e}")
            raise
    
    def _build_daily_export_url(self, target_date):
        """Build export URL for specific date"""
        # Calculate date range (start and end of target date)
        start_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=1)
        
        # Format dates for Elasticsearch
        start_str = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        end_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
        # Use your existing URL structure with updated dates
        base_url = f"{self.kibana_base_url}/api/reporting/generate/csv"
        
        # Modified version of your URL with dynamic dates
        url_template = f"""{base_url}?jobParams=%28browserTimezone%3AAmerica%2FLos_Angeles%2CconflictedTypesFields%3A%21%28%29%2Cfields%3A%21%28utcCreatedDateTime%2CbookingId%2Clocation.businessName%2Clocation.businessId%2Cclient.lastName%2CisGoogleBooking%2Coffering.name%2Cclient.firstName%2Cclient.email%2CbookingStatus.label%2CstartDateTime%29%2CindexPatternId%3Aa8e8cdc8-c993-429b-b378-d2bfa0589440%2CmetaFields%3A%21%28_source%2C_id%2C_type%2C_index%2C_score%29%2CobjectType%3Asearch%2CsearchRequest%3A%28body%3A%28_source%3A%28includes%3A%21%28utcCreatedDateTime%2CbookingId%2Clocation.businessName%2Clocation.businessId%2Cclient.lastName%2CisGoogleBooking%2Coffering.name%2Cclient.firstName%2Cclient.email%2CbookingStatus.label%2CstartDateTime%29%29%2Cfields%3A%21%28%28field%3AstartDateTime%2Cformat%3Adate_time%29%2C%28field%3AutcCreatedDateTime%2Cformat%3Adate_time%29%29%2Cquery%3A%28bool%3A%28filter%3A%21%28%28match_all%3A%28%29%29%2C%28range%3A%28utcCreatedDateTime%3A%28format%3Astrict_date_optional_time%2Cgte%3A%27{quote(start_str)}%27%2Clte%3A%27{quote(end_str)}%27%29%29%29%29%2Cmust%3A%21%28%29%2Cmust_not%3A%21%28%29%2Cshould%3A%21%28%29%29%29%2Cscript_fields%3A%28%29%2Csort%3A%21%28%28utcCreatedDateTime%3A%28order%3Aasc%2Cunmapped_type%3Aboolean%29%29%29%2Cstored_fields%3A%21%28utcCreatedDateTime%2CbookingId%2Clocation.businessName%2Clocation.businessId%2Cclient.lastName%2CisGoogleBooking%2Coffering.name%2Cclient.firstName%2Cclient.email%2CbookingStatus.label%2CstartDateTime%29%2Cversion%3A%21t%29%2Cindex%3Abooking%29%2Ctitle%3A%27Daily%20Appointments%20-%20{target_date.strftime('%Y-%m-%d')}%27%29"""
        
        return url_template
    
    def _process_appointment_data(self, df, target_date):
        """Process raw data into clean format"""
        if df.empty:
            return df
            
        # Create a copy to avoid modifying original
        processed_df = df.copy()
        
        # Rename columns
        column_mapping = {
            'client.firstName': 'first_name',
            'client.lastName': 'last_name', 
            'client.email': 'email',
            'offering.name': 'service_type',
            'startDateTime': 'appointment_datetime',
            'location.businessName': 'location_name',
            'location.businessId': 'location_id',
            'bookingStatus.label': 'status',
            'bookingId': 'booking_id'
        }
        
        processed_df = processed_df.rename(columns=column_mapping)
        
        # Combine first and last name
        processed_df['customer_name'] = (
            processed_df.get('first_name', '').fillna('') + ' ' + 
            processed_df.get('last_name', '').fillna('')
        ).str.strip()
        
        # Parse appointment datetime
        if 'appointment_datetime' in processed_df.columns:
            processed_df['appointment_datetime'] = pd.to_datetime(processed_df['appointment_datetime'])
            processed_df['appointment_date'] = processed_df['appointment_datetime'].dt.date
            processed_df['appointment_time'] = processed_df['appointment_datetime'].dt.strftime('%H:%M')
            processed_df['appointment_time_12h'] = processed_df['appointment_datetime'].dt.strftime('%I:%M %p')
        
        # Add metadata
        processed_df['extracted_at'] = datetime.utcnow()
        processed_df['data_date'] = target_date.date()
        
        # Select and order final columns
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
            'extracted_at',
            'data_date'
        ]
        
        # Only include columns that exist
        available_columns = [col for col in final_columns if col in processed_df.columns]
        
        return processed_df[available_columns].sort_values('appointment_datetime' if 'appointment_datetime' in processed_df.columns else 'booking_id')
    
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
                on_conflict='booking_id,data_date'  # Avoid duplicates
            ).execute()
            
            logger.info(f"Successfully saved {len(data_to_insert)} records to Supabase")
            return result
            
        except Exception as e:
            logger.error(f"Error saving to Supabase: {e}")
            raise
    
    def run_daily_extraction(self):
        """Main method to run daily extraction"""
        try:
            logger.info("Starting daily appointment extraction")
            
            # Extract today's appointments
            appointments_df = self.extract_daily_appointments()
            
            if not appointments_df.empty:
                # Save to Supabase
                self.save_to_supabase(appointments_df)
                
                logger.info("Daily extraction completed successfully")
                
                # Return summary
                return {
                    'status': 'success',
                    'records_processed': len(appointments_df),
                    'date': datetime.now().date().isoformat(),
                    'locations': appointments_df['location_name'].nunique() if 'location_name' in appointments_df.columns else 0
                }
            else:
                logger.info("No appointments found for today")
                return {
                    'status': 'success',
                    'records_processed': 0,
                    'date': datetime.now().date().isoformat(),
                    'message': 'No appointments found'
                }
                
        except Exception as e:
            logger.error(f"Daily extraction failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'date': datetime.now().date().isoformat()
            }

def main():
    """Main entry point for GitHub Actions"""
    extractor = CloudAppointmentExtractor()
    result = extractor.run_daily_extraction()
    
    print(f"Extraction result: {json.dumps(result, indent=2)}")
    
    # Exit with error code if extraction failed
    if result['status'] == 'error':
        exit(1)

if __name__ == "__main__":
    main()
