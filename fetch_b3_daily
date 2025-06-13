import requests
import json
import pandas as pd
from datetime import datetime
import time
import threading
from queue import Queue
import re

class TradeMateDataFetcher:
    """
    A class to fetch real-time trading data from B3's Trademate platform
    using Server-Sent Events (SSE) streaming.
    """
    
    def __init__(self):
        # API endpoint we discovered from network analysis
        self.api_url = "https://trademate.b3.com.br/api/get-activities"
        
        # Headers that exactly match what the browser sends
        # These are critical for the server to accept our connection
        self.headers = {
            'Accept': 'text/event-stream',  # Tell server we expect streaming data
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',  # Ensure we get fresh data
            'Connection': 'keep-alive',   # Maintain persistent connection
            'Host': 'trademate.b3.com.br',
            'Referer': 'https://trademate.b3.com.br/',  # Show we came from their site
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
            'dnt': '1',
            'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        
        # Storage for collected data
        self.data_queue = Queue()
        self.collected_data = []
        self.is_streaming = False
        
    def parse_sse_event(self, line):
        """
        Parse a single Server-Sent Event line.
        SSE format typically looks like: 'data: {"key": "value"}'
        """
        if line.startswith('data: '):
            try:
                # Remove 'data: ' prefix and parse JSON
                json_data = line[6:]  # Skip 'data: '
                return json.loads(json_data)
            except json.JSONDecodeError:
                # Some SSE events might not be JSON (like heartbeats)
                return None
        return None
    
    def stream_data(self, duration_seconds=60):
        """
        Connect to the SSE stream and collect data for specified duration.
        
        Args:
            duration_seconds (int): How long to collect data (default: 60 seconds)
        """
        print(f"Starting data collection for {duration_seconds} seconds...")
        print("Connecting to Trademate API...")
        
        try:
            # Make the streaming request
            # stream=True keeps connection open, timeout prevents hanging
            response = requests.get(
                self.api_url, 
                headers=self.headers, 
                stream=True,
                timeout=30
            )
            
            # Check if connection was successful
            if response.status_code != 200:
                print(f"Connection failed with status code: {response.status_code}")
                return
            
            print("Successfully connected! Receiving data...")
            self.is_streaming = True
            start_time = time.time()
            
            # Process the streaming response line by line
            for line in response.iter_lines(decode_unicode=True):
                # Check if we've reached our time limit
                if time.time() - start_time > duration_seconds:
                    print("Time limit reached. Stopping data collection.")
                    break
                
                if line:  # Skip empty lines
                    # Parse the SSE event
                    data = self.parse_sse_event(line)
                    if data:
                        # Add timestamp when we received this data
                        data['received_at'] = datetime.now().isoformat()
                        self.collected_data.append(data)
                        
                        # Show progress every 10 records
                        if len(self.collected_data) % 10 == 0:
                            print(f"Collected {len(self.collected_data)} records...")
            
        except requests.exceptions.RequestException as e:
            print(f"Connection error: {e}")
        except KeyboardInterrupt:
            print("Collection stopped by user.")
        finally:
            self.is_streaming = False
            print(f"Data collection completed. Total records: {len(self.collected_data)}")
    
    def convert_financial_value(self, value_dict):
        """
        Convert the nested financial value format (units + nanos) to a decimal.
        B3 uses this format for precise financial calculations.
        Example: {'units': '100', 'nanos': 500000000} = 100.50
        """
        if not isinstance(value_dict, dict) or 'units' not in value_dict:
            return 0.0
        
        units = float(value_dict.get('units', 0))
        nanos = float(value_dict.get('nanos', 0))
        
        # Nanos are billionths, so divide by 1,000,000,000 to get decimal portion
        return units + (nanos / 1_000_000_000)
    
    def format_time_for_display(self, iso_timestamp):
        """
        Convert ISO timestamp to the format shown in the table (HH:MM:SS,mmm)
        Example: '2025-06-11T11:00:47.851407200Z' becomes '11:00:47,851'
        """
        try:
            # Parse the ISO timestamp
            dt = datetime.fromisoformat(iso_timestamp.replace('Z', '+00:00'))
            # Format as HH:MM:SS,mmm (note: comma instead of period for milliseconds)
            return dt.strftime('%H:%M:%S') + str(dt.microsecond // 1000).zfill(3)
        except:
            return iso_timestamp  # Return original if parsing fails

    def format_date_for_display(self, iso_timestamp):
        """
        Convert ISO timestamp to the date format DD-MM-YYYY.
        """
        try:
            # Parse the ISO timestamp
            dt = datetime.fromisoformat(iso_timestamp.replace('Z', '+00:00'))
            return dt.strftime('%d-%m-%Y')
        except:
            return iso_timestamp  # Return original if parsing fails
    
    def get_dataframe(self):
        """
        Convert collected data to a pandas DataFrame matching the exact table structure:
        HORA, ATIVO, QTDE, PREÇO, VOL, TIPO, CANC
        """
        if not self.collected_data:
            print("No data collected yet. Run stream_data() first.")
            return pd.DataFrame()
        
        # Transform the raw API data into the table format you see on screen
        processed_records = []
        
        for record in self.collected_data:
            try:
                # Extract and transform each field to match the display table
                processed_record = {
                    'DATE': self.format_date_for_display(record.get('TradeTime', '')),
                    'HORA': self.format_time_for_display(record.get('TradeTime', '')),
                    'ATIVO': record.get('Ticker', ''),
                    'QTDE': int(record.get('Quantity', {}).get('units', 0)),
                    'PREÇO': self.convert_financial_value(record.get('PU', {})),
                    'VOL': self.convert_financial_value(record.get('Amount', {})),
                    'TIPO': record.get('TypeDescription', ''),
                    'CANC': '-'  # Default value, update if cancellation data is available
                }
                processed_records.append(processed_record)
            except Exception as e:
                # Skip records that can't be processed but continue with others
                print(f"Skipping malformed record: {e}")
                continue
        
        # Create DataFrame with the exact column structure
        df = pd.DataFrame(processed_records)
        
        # Ensure proper data types for analysis
        if not df.empty:
            df['QTDE'] = df['QTDE'].astype(int)
            df['PREÇO'] = df['PREÇO'].astype(float)
            df['VOL'] = df['VOL'].astype(float)
        
        print(f"Created DataFrame with {len(df)} rows matching table structure")
        print("Columns:", list(df.columns))
        
        return df
    
    def save_to_csv(self, filename="trademate_data.csv"):
        """
        Save collected data to CSV file for further analysis.
        """
        df = self.get_dataframe()
        if not df.empty:
            df.to_csv(filename, index=False)
            print(f"Data saved to {filename}")
        else:
            print("No data to save.")
    
    def get_latest_activities(self, limit=10):
        """
        Get the most recent trading activities.
        
        Args:
            limit (int): Number of latest records to return
        """
        if not self.collected_data:
            print("No data available. Run stream_data() first.")
            return []
        
        return self.collected_data[-limit:]
    
    def print_sample_data(self, num_samples=5):
        """
        Print a few sample records to see the data structure.
        """
        if not self.collected_data:
            print("No data collected yet.")
            return
        
        print(f"Sample of collected data (showing {min(num_samples, len(self.collected_data))} records):")
        for i, record in enumerate(self.collected_data[:num_samples]):
            print(f"Record {i+1}: {record}")

# Usage example for Google Colab
def main(instrument):
    """
    Main function demonstrating how to use the TradeMateDataFetcher.
    """
    # Create an instance of our data fetcher
    fetcher = TradeMateDataFetcher()
    
    # Collect data for 2 minutes (120 seconds)
    # You can adjust this based on how much data you need
    fetcher.stream_data(duration_seconds=120)
    
    # Show some sample data to understand the structure
    fetcher.print_sample_data()
    
    # Convert to DataFrame for analysis
    df = fetcher.get_dataframe()

    df = df[df['ATIVO'] == instrument]
    
    print(f"Filtered {len(df)} records for {instrument}")
    
    # Display basic information about the collected data
    if not df.empty:
        print("\nDataFrame Info:")
        print(df.info())
        print("\nFirst few rows:")
        print(df.head())
        
        # Save to CSV for later use
        fetcher.save_to_csv("atividades_do_dia.csv")
    
    return fetcher, df

# Alternative function for shorter data collection (good for testing)
def quick_test(instrument):
    """
    Quick test function to verify the connection works.
    Collects data for just 30 seconds and returns both fetcher and DataFrame.
    """
    print(f"Quick test for: {instrument}.")
    fetcher = TradeMateDataFetcher()
    fetcher.stream_data(duration_seconds=30)
    fetcher.print_sample_data()
    
    # Convert to the proper DataFrame format
    df = fetcher.get_dataframe()
    # filter by instrument
    df = df[df['ATIVO'] == instrument]
    if not df.empty:
        print("\nTransformed DataFrame (first 5 rows):")
        print(df.head())
    
    return fetcher, df

# If running this script directly
fetcher, df = main('CBIO')
