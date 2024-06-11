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



# from py5paisa import FivePaisaClient
# cred={
#     "APP_NAME":"5P50333049",
#     "APP_SOURCE":"10195",
#     "USER_ID":"ZnSAVJJEvI6",
#     "PASSWORD":"vd6lp1F5xdv",
#     "USER_KEY":"JjC4uF1bOtL3DytCVRPY2LGCTwftajWj",
#     "ENCRYPTION_KEY":"MGvaEEd3qLIecO8GoK5DBnnvWFVmfjMP"
#     }

# #This function will automatically take care of generating and sending access token for all your API's

# client = FivePaisaClient(cred=cred)

# # New TOTP based authentication
# client.get_totp_session('Your ClientCode','TOTP from authenticator app','Your Pin')

# #Using Scrip Data :-

# client.place_order(OrderType='B',Exchange='N',ExchangeType='C', ScripData = 'ITC_EQ', Qty=1, Price=450)

# #Using Scrip Code :-

# #Sample For SL order (for order to be treated as SL order just pass StopLossPrice)
# client.place_order(OrderType='B',Exchange='N',ExchangeType='C', ScripCode = 1660, Qty=1, Price=350, IsIntraday=False, StopLossPrice=345)

# #Derivative Order
# client.place_order(OrderType='B',Exchange='N',ExchangeType='D', ScripCode = 57633, Qty=50, Price=1.5)




class OrderManager:
    """Order management class for placing, modifying, and cancelling orders."""
    def __init__(self, client: FivePaisaClient):
        self.client = client

    def place_order(self, scrip_code: str, quantity: int, price: float, order_type: str, exchange: str = "NSE", exchange_segment: str = "CASH"):
        """Places a new order."""
        try:
            order = {
                "Exch": exchange,
                "ExchType": exchange_segment,
                "ScripCode": scrip_code,
                "Qty": quantity,
                "Price": price,
                "OrderType": order_type,
                "AtMarket": False,
                "RemoteOrderID": 1,
                "ExchOrderID": 0,
                "DisQty": 0,
                "IsStopLossOrder": False,
                "StopLossPrice": 0,
                "IsIOCOrder": False,
                "IsIntraday": False,
                "AHPlaced": "N",
                "PublicIP": "192.168.1.1"
            }
            print(f"Placing order with parameters: {order}")
            response = self.client.place_order(
                Exchange = exchange,
                ExchangeType = exchange_segment,
                ScripCode = scrip_code,
                Qty = quantity,
                Price = price,
                OrderType = order_type
            )
            print(f"Order response: {response}")
            return response
        except Exception as e:
            return f"Failed to place order: {e}"

    def modify_order(self, order_id: str, scrip_code: str, quantity: int, price: float, order_type: str, exchange: str = "NSE", exchange_segment: str = "CASH"):
        """Modifies an existing order."""
        try:
            response = self.client.modify_order(
                Exch=exchange,
                ExchType=exchange_segment,
                ScripCode=scrip_code,
                Qty=quantity,
                Price=price,
                OrderType=order_type,
                ExchOrderID=order_id,
                AtMarket=False,
                IsStopLossOrder=False,
                StopLossPrice=0,
                IsIOCOrder=False,
                IsIntraday=False,
                AHPlaced="N",
                PublicIP="192.168.1.1"
            )
            return response
        except Exception as e:
            return f"Failed to modify order: {e}"

    def cancel_order(self, order_id: str, exchange: str = "NSE", exchange_segment: str = "CASH"):
        """Cancels an existing order."""
        try:
            response = self.client.cancel_order(
                Exch=exchange,
                ExchType=exchange_segment,
                ExchOrderID=order_id,
                RemoteOrderID=1
            )
            return response
        except Exception as e:
            return f"Failed to cancel order: {e}"

# Ensure that this module can be imported and used in other scripts
if __name__ == "__main__":
    broker = Broker()
    order_manager = OrderManager(broker.client)
    # Example usage:
    response = order_manager.place_order(scrip_code="500325", quantity=10, price=2500, order_type="BUY")
    print(response)

