from datetime import datetime
import os
import pandas as pd
import requests
import threading
from py5paisa import FivePaisaClient
import pyotp
import settings


class Broker():
    """FivePaisa broker module. Interacts with FivePaisa API for trading. 
    """
    def __init__(self) -> None:
        self.broker_credentials = settings.broker_cred
        self.fetch_instruments()
        self.fetch_scrip_master
        self.login()
        self.scrip_names = ['SBIN', 'HDFCBANK']
        self.subscribe_scrips()

    
    def login(self):
        """Creates a session with the broker (5Paisa) using two factor authentication

        Returns:
            Bool: True for successful connection, False for failure
        """
        two_factor_creds={
            "APP_NAME": self.broker_credentials['APP_NAME'],
            "APP_SOURCE": self.broker_credentials['APP_SOURCE'],
            "USER_ID": self.broker_credentials['USER_ID'],
            "PASSWORD": self.broker_credentials['APP_PASSWORD'],
            "USER_KEY": self.broker_credentials['USER_KEY'],
            "ENCRYPTION_KEY": self.broker_credentials['ENCRYPTION_KEY']
        }

        try:
            self.client = FivePaisaClient(cred=two_factor_creds)
            self.client.get_totp_session(
                client_code=self.broker_credentials["CLIENT_CODE"],
                totp=pyotp.TOTP(self.broker_credentials["TOTP_CODE"]).now(),
                pin=self.broker_credentials["PIN"])
            
            if self.client.client_code in ["", "INVALID CODE"]:
                return False, "Login unsuccessful"
            return True, "Login successful"
    
        except Exception as e:
            return False, e
        
    def fetch_scrip_master(self):
        """Fetches scrip master table data.
        """
        file_path = 'scrip_master.csv'
        if os.path.exists(file_path):
            m_dt = datetime.fromtimestamp(os.path.getmtime(file_path))
            m_dt = m_dt.date()  # Extracting date
            
        if not os.path.exists(file_path) or m_dt != datetime.now().date():  # If file not exists or file was not modified today
            url = "https://images.5paisa.com/website/scripmaster-csv-format.csv"
            res = requests.get(url, allow_redirects=True)
            self.logger.info("Downloading Scrip master file")
            file = open(file_path, 'wb')
            file.write(res.content)
            file.close()
        
        file = open(file_path, 'r')
        self.instruments = pd.read_csv(file).astype(str)
        file.close()
        self.logger.info('Srcip master file loaded')

    def fetch_instruments(self):
        """Fetches master file for instruments.

        returns:
            bool: True for success, False for failure
            str: Error message, else None
        """
        file_path = os.path.join("./instruments.csv")
        if os.path.exists(file_path):
            m_dt = datetime.fromtimestamp(os.path.getmtime(file_path))
            m_dt = m_dt.date()  # Extracting date
            
        if not os.path.exists(file_path) or m_dt != datetime.now().date():  # If file not exists or file was not modified today
            url = "https://images.5paisa.com/website/scripmaster-csv-format.csv"
            try:
                res = requests.get(url, allow_redirects=True)
            except Exception as e:
                return False, e
            file = open(file_path, 'wb')
            file.write(res.content)
            file.close()
        
        file = open(file_path, 'r')
        self.instruments = pd.read_csv(file).astype(str)
        file.close()
        return True, None

    def fetch_historical_data(self, exchange_token:str, time_interval:str, from_dt:str, to_dt:str):
        """Fetches historical data for the provided scrip
        Args:
            exchange_token (int): Unique exchange code of the instrument
            time_interval (str): 1m, 5m, 10m, 15m, 30m, 60m, 1d
            from_dt (str): Starting date of the data (YYYY-MM-DD)
            to_dt (str): Ending date of the data (YYYY-MM-DD)

        Returns:
            bool: True for success, False for failure
            pd.DataFrame: Dataframe consisting ohlcv data
        """
        
        historical_data = self.client.historical_data(
            Exch = self.convert_value(from_type="Scripcode", to_type="Exch", value=exchange_token),
            ExchangeSegment = self.convert_value(from_type="Scripcode", to_type="ExchType", value=exchange_token),
            ScripCode = int(exchange_token),
            time = time_interval,
            From = from_dt,
            To = to_dt
        )
        return True, historical_data
    
    def convert_value(self, from_type:str, to_type:str, value:str) -> str:
        """Convert value by matching it from the instruments table

        Args:
            from_type (str): type of parameter to convert from
            to_type (str): type of parameter to convert to
            value (str): value of the parameter

        Returns:
            str: converted value of the parameter
        """
        return str(self.instruments[self.instruments[from_type] == str(value)][to_type].iloc[0])
    
    def subscribe_scrips(self):
        """Subscribes to the given scrips, and starts live streaming
        Args:
            scrip_names (list[str]): List containing scrip names
        """
        def on_message(ws, message):
            print(ws)
            print(message)
            self.message= message

        request_list = list()
        for scrip_name in self.scrip_names:
            scrip_code = self.convert_value(from_type="Name", to_type="Scripcode", value=scrip_name)
            request_list.append({
                "Exch": self.convert_value(from_type="Scripcode", to_type="Exch", value=scrip_code),
                "ExchType": self.convert_value(from_type="Scripcode", to_type="ExchType", value=scrip_code),
                "ScripCode": scrip_code
            })
            print(request_list)

        req_data = self.client.Request_Feed('mf','s',request_list)  # MarketFeedV3, Subscribe
        self.client.connect(req_data)
        self.live_feed_thread = threading.Thread(target=self.client.receive_data, args=(on_message,))
        self.live_feed_thread.start()
    

obj = Broker() 