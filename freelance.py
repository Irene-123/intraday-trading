

# from waitress import serve
from kiteconnect import KiteConnect
from dateutil import parser
from http import HTTPStatus
import hashlib,requests
from urllib.parse import parse_qs, urlparse

import requests,os
import json
import time
import datetime
import sqlite3
# new added for totp  on 01/08/21
import pyotp

import threading
import datetime, calendar
import math

from ast import Pass
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import inspect
from flask import Flask,request,jsonify,render_template
import datetime,json,requests
from dateutil.tz import gettz
import os
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///pmo.sqlite3'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS']=False

db = SQLAlchemy(app)
SQLALCHEMY_TRACK_MODIFICATIONS=False
# run_with_ngrok(app)
def object_as_dict(obj):
    return {c.key: getattr(obj, c.key)
            for c in inspect(obj).mapper.column_attrs}


class User_config(db.Model):
    id = db.Column( db.Integer, primary_key = True)
    userid = db.Column(db.String(50))  
    password = db.Column(db.String(200))
    pin = db.Column(db.String(10))
    api = db.Column(db.String(200))
    sec = db.Column(db.String(200))
    token = db.Column(db.String(200))
    qty = db.Column(db.String(200))
    token_date = db.Column(db.String(200))
    
    def __repr__(self):
        return self.userid

class Position(db.Model):
    id = db.Column( db.Integer, primary_key = True)
    userid = db.Column(db.String(200))
    orderid = db.Column(db.String(200))
    symboll = db.Column(db.String(200))
    qty = db.Column(db.String(10))
    sidee = db.Column(db.String(10))
    pricee = db.Column(db.String(200))
    timee = db.Column(db.DateTime, default=datetime.datetime.now)
    statuss = db.Column(db.String(200))
    
    def __repr__(self):
        return str(self.id)

with app.app_context():
    db.create_all()
    db.session.commit()

    if User_config.query.all()==[]:
        user=User_config(userid='##################',password='#############',pin='###########',api='###############',token='##################',qty='50',token_date="2022-06-02")
        db.session.add(user)
    db.session.commit()
def buy_sell(user,ticker,qty,side,recqty):
    global admin_user
    order_id = admin_user[user].place_order(variety=(admin_user[user].VARIETY_REGULAR),tradingsymbol=ticker,
                                exchange=admin_user[user].EXCHANGE_NFO,
                                transaction_type=side,
                                quantity=qty,
                                order_type=admin_user[user].ORDER_TYPE_MARKET,
                                product=(admin_user[user].PRODUCT_NRML))
    nowTime=datetime.datetime.now()
    time.sleep(3)
    orders=admin_user[user].orders()
    price=0
    status='NA'
    for i in orders:
        if str(i['order_id'])==str(order_id):
            price=i['average_price']
            status=i['status']
    conn = sqlite3.connect('pmo.sqlite3', timeout=10)
    cur=conn.cursor()
    count = cur.execute(F"INSERT INTO Position (userid,orderid,symboll,sidee,qty,pricee,statuss,'timee') VALUES('{user}','{str(order_id)}','{ticker}','{str(side)}','{str(recqty)}','{str(price)}','{str(status)}','{str(nowTime)}');")
    conn.commit()
    return order_id

def futureName():
    date_tdy=datetime.datetime.today().strftime("%Y,%m").split(',')
    year=int(date_tdy[0])
    month=int(date_tdy[1])
    daysInMonth = calendar.monthrange(year, month)[1]  
    dt = datetime.date(year, month, daysInMonth)
    offset = 4 - dt.isoweekday()
    if offset > 0: offset -= 7                  
    dt += datetime.timedelta(offset) 
    today = datetime.date.today()
    wexpiry = today + datetime.timedelta( (3-today.weekday()) % 7 )
    return 'BANKNIFTY'+wexpiry.strftime('%y%b').upper()+'FUT'

def nfutureName(nextt=None):
    date_tdy=datetime.datetime.today().strftime("%Y,%m").split(',')
    year=int(date_tdy[0])
    month=int(date_tdy[1])
    daysInMonth = calendar.monthrange(year, month)[1]  
    dt = datetime.date(year, month, daysInMonth)
    offset = 4 - dt.isoweekday()
    if offset > 0: offset -= 7                  
    dt += datetime.timedelta(offset) 
    today = datetime.date.today()
    wexpiry = today + datetime.timedelta( (3-today.weekday()) % 7 )
    if nextt:
        wexpiry=wexpiry+datetime.timedelta( 15 )
    return 'BANKNIFTY'+wexpiry.strftime('%y%b').upper()+'FUT'

def auto_login(user):
    global kite
    login_date=user.token_date
    tdydte=str(datetime.date.today())
    access_token=user.token
    LOGIN_TIME=parser.parse('08:50')
    if datetime.datetime.now() > LOGIN_TIME :
        if True:
                try:
                    if login_date != tdydte or  access_token==None or access_token=='' or access_token==' ':
                        token=get_kite_token(user.userid,user.password,user.tfa,user.api,user.sec)
                        user.token=token
                        user.token_date=str(datetime.date.today())
                    login_date=user.token_date
                    tdydte=str(datetime.date.today())
                    access_token=user.token
                    if login_date == tdydte and access_token != None and access_token!='' and access_token!=' ':
                        try:
                            kite = KiteConnect(api_key=user.api) 
                            kite.set_access_token(user.token)
                            return kite
                        except:
                            time.sleep(60)
                            auto_login(user)
                            
                except Exception as e:
                    print('login error ' , e)
            

kite=''

@app.route('/autoLogin')
def autoLogin():
    global kite,admin_user
    users=User_config.query.all()
    for user in users:
        try:
            kite=''
            print(datetime.datetime.now(),user,'autoLogin')
            kite=auto_login(user)
                
            if kite!='':
                print(str(kite.profile()['user_name']))
                admin_user[user.userid]=kite
        
        except Exception as e:
            print(datetime.datetime.now(),e,user,'autologinerror')

    return "done"


# User_config.query.all()[0].qty='50'
# db.session.commit()

# User_config.query.all()[0].token=''
# db.session.commit()

@app.route('/orders',methods =['POST'])
def orders():
    if request.method=='POST':
        try:
            dta=json.loads(request.get_data().decode())
            if 'token' in dta:
                if dta['token']==User_config.query.all()[0].sec:
                    side=dta['side'].upper()
                    ticker=futureName()
                    user=''
                    for user in admin_user:
                        try:
                            oldqty=int(Position.query.filter_by(userid=user).order_by(Position.id.desc()).first().qty)
                        except:
                            oldqty=int(User_config.query.filter_by(userid=user)[0].qty)
                        recqty=int(User_config.query.filter_by(userid=user)[0].qty)
                        qty=recqty

                        print(user,qty,ticker,side)
                        threading.Thread(target=buy_sell,args=(user,ticker,qty,side,recqty)).start()
                    print(datetime.datetime.now(),side)
            data=jsonify({"done":"done"})
            data.headers.add("Access-Control-Allow-Origin", "*")
            return data
        except Exception as e:
            print('eerr',e)
            return 'not done'


@app.route('/username/')
def username():
    global kite
    try:
        data=jsonify(kite.profile()['user_name'])
        data.headers.add("Access-Control-Allow-Origin", "*")
        return data
    except:
        data=jsonify('Not Logged In')
        data.headers.add("Access-Control-Allow-Origin", "*")
        return data

@app.route('/')
def home():
    return render_template('/index.html')

admin_user={}

@app.route('/getPosition/<id>',methods =['GET','POST'])
def getPosition(id):
    if id=='all':
        users=Position.query.order_by(Position.id.desc()).all()
        udict=[]
        for user in users:
            udict.append(object_as_dict(user))
        data=jsonify(udict)
        data.headers.add("Access-Control-Allow-Origin", "*")
        return  data

@app.route('/deletePosition/<id>')
def deletePosition(id):
    try:
        Position.query.filter_by(id=id).delete()
        db.session.commit()
    except:
        pass
    data=jsonify('done')
    return data

@app.route('/deleteUser/<id>')
def deleteUser(id):
    try:
        admin_user.pop(User_config.query.filter_by(id=id)[0].userid,None)
        User_config.query.filter_by(id=id).delete()
        db.session.commit()
    except:
        pass
    data=jsonify('done')
    return data


@app.route('/loginuser/<sec>')
def loginTempUerUrl(sec):
    global admin_user
    token=request.args['request_token']
    user_dta=User_config.query.filter_by(sec=sec)[0]
    kite = KiteConnect(api_key=user_dta.api)
    data = kite.generate_session(token, api_secret=user_dta.sec)
    access_token=data["access_token"]
    kite.set_access_token(access_token)
    user_dta.token_date=str(datetime.date.today())
    kite.set_access_token(access_token)
    user_dta.token=access_token
    db.session.commit()
    name="logged in successfully "+str(kite.profile()['user_name'])
    db.session.commit()
    if not admin_user:
        admin_user={}
    admin_user[user_dta.userid]=kite
    print("admin_user",admin_user)
    return name

@app.route('/logout/<id>')
def logout(id):
    global admin_user
    try:
        user_dta=User_config.query.filter_by(id=id)[0]
        admin_user.pop(user_dta.userid,None)
        return 'done'
    except Exception as e:
        print(e)
        return 'error'

@app.route('/loginonly/<id>')
def loginonly(id):
    global admin_user  
    if not admin_user:
        admin_user={}
    user_dta=User_config.query.filter_by(id=id)[0]
    kite = KiteConnect(api_key=user_dta.api)
    kite.set_access_token(user_dta.token)
    name=kite.profile()['user_name']
    admin_user[user_dta.userid]=kite
    data=jsonify(name)
    print("admin_user",admin_user)
    data.headers.add("Access-Control-Allow-Origin", "*")
    return  data

def get_kite_token(userId,password,tfa,api,sec):
    try:
        http_session = requests.Session()
        url="https://kite.zerodha.com/connect/login?v=3&api_key="+api
        ref_url=http_session.get(url).url
        url = "https://kite.zerodha.com/api/login"
        data = dict()
        data["user_id"] = userId
        data["password"] = password
        response = http_session.post(url=url, data=data)
        resp_dict = json.loads(response.content)
        if "message" in resp_dict.keys():
            resp_dict["err_message"] = resp_dict["message"]
            del resp_dict["message"]
        if response.status_code != HTTPStatus.OK:
            print(ConnectionError("Login failed!")) 

        url = "https://kite.zerodha.com/api/twofa"
        data = dict()
        data["user_id"] = userId
        data["request_id"] = resp_dict["data"]["request_id"]
        data["twofa_value"] = str(pyotp.TOTP(str(tfa)).now())
        response = http_session.post(url=url, data=data)
        resp_dict = json.loads(response.content)
        if "message" in resp_dict.keys():
            resp_dict["err_message"] = resp_dict["message"]
            del resp_dict["message"]

        if response.status_code != HTTPStatus.OK:
            raise ConnectionError("Two-factor authentication failed!")
        url = ref_url+"&skip_session=true"
        response = http_session.get(url=url, allow_redirects=False)
        reply="noreply"
        if response.status_code == 302:
            reply = response.headers["Location"]
            request_token=http_session.get(reply, allow_redirects=False)
        try:
            request_token=request_token.json()['request_token']
        except Exception as e:
            print(e)
            try:
                url=request_token.headers['location']
                
                parsed_url = urlparse(url)
                query_params = parse_qs(parsed_url.query)
                request_token=query_params['request_token'][0]
            except:
                pass
        h = hashlib.sha256(api.encode("utf-8") + request_token.encode("utf-8") + sec.encode("utf-8"))
        checksum = h.hexdigest()
        headers = {'X-Kite-Version': '3', 'User-Agent': 'Kiteconnect-python/3.9.4'}
        resp = requests.post("https://api.kite.trade/session/token",headers=headers, data={
            "api_key": api,
            "request_token": request_token,
            "checksum": checksum
        }).json()
    except Exception as e:
        print("eror in login kite",e)
    request_token=resp['data']['access_token']
    print("ak",request_token)
    return request_token


@app.route('/autoLoginGenerateToken/<id>')
def autoLoginGenerateToken(id):
    user_dta=User_config.query.filter_by(id=id)[0]
    try:
        user_dta.token=get_kite_token(user_dta.userid,user_dta.password,user_dta.pin,user_dta.api,user_dta.sec)
        user_dta.token_date=str(datetime.date.today())
        db.session.commit()
        print("akfj",user_dta.token)
        return 'done'
    except Exception as e:
        print('loggin error',e)
        print("Unable to get access token please do it manualy go to blow URL and login and copy paste url here")
        return f'not Logged In {e}'


@app.route('/getusers/<id>',methods =['GET','POST'])
def getusers(id):
    if request.method=='POST':
        if id=="new":
            data=json.loads(request.get_data().decode("utf-8"))
            db.session.add(User_config(**data))
            db.session.commit()
        else:
            data=json.loads(request.get_data().decode("utf-8"))
            User_config.query.filter_by(id=id).update(data)
            db.session.commit()
        data=jsonify({"done":"done"})
        data.headers.add("Access-Control-Allow-Origin", "*")
        return data
    if id=='all':
        users=User_config.query.all()
        udict=[]
        for user in users:
            udict.append(object_as_dict(user))
        udict2=[]
        for user in udict:
            if user['userid'] in admin_user:
                try:
                    user['name']=admin_user[user['userid']].profile()['user_name']
                except:
                    user['name']='Unable To Get User Name'
            else:
                user['name']='Not Logged In'
            udict2.append(user)
        data=jsonify(udict2)
    else:
        data=object_as_dict(User_config.query.filter_by(id=id)[0])
        data=jsonify(data)
        
    data.headers.add("Access-Control-Allow-Origin", "*")
    return  data

@app.route('/getLtp')
def getLtp():
    global admin_user  
    try:
        user=User_config.query.filter_by(id='1')[0]
        ltp = admin_user[user.userid].ltp('NSE:SBIN')['NSE:SBIN']['last_price']
        data=jsonify(ltp)
        data.headers.add("Access-Control-Allow-Origin", "*")
        return  data
    except Exception as e:
        return 'err'


def check_login():
    global admin_user
    expirydate='2024-04-24'
    changeTime='15:00'
    done=False

    while True:
        try:
            startTime=parser.parse('00:00')
            endTime=parser.parse('15:30')
            time.sleep(10)
            if startTime < datetime.datetime.now()<endTime:
                # if not admin_user:
                    try:
                        resp=requests.get('http://127.0.0.1:5000/getLtp').text
                        if resp=='err':
                            requests.get('http://127.0.0.1:5000/autoLogin')
                    except Exception as e:
                        print('autologin error',e)
                    changeTimen=parser.parse(changeTime)
                    if expirydate ==str(datetime.date.today()) and datetime.datetime.now()>changeTimen:
                        expirydate='2024-04-24'
                        ticker=futureName()
                        user=''
                        for user in admin_user:
                            try:
                                oldqty=int(Position.query.filter_by(userid=user).order_by(Position.id.desc()).first().qty)
                            except:
                                oldqty=int(User_config.query.filter_by(userid=user)[0].qty)

                            recqty=int(User_config.query.filter_by(userid=user)[0].qty)
                            qty=recqty/2
                            oside='BUY' if Position.query.filter_by(userid='FS9865').order_by(Position.id.desc()).first().sidee=='SELL' else 'SELL'
                            nside=Position.query.filter_by(userid='FS9865').order_by(Position.id.desc()).first().sidee
                            newticker=nfutureName('next')
                            print(user,int(qty),ticker,newticker,oside,nside)
                            buy_sell(user,ticker,int(qty),oside,recqty)
                            buy_sell(user,newticker,int(qty),nside,recqty)
                            
                        
            else:
                for user in admin_user.copy():
                    admin_user.pop(user,None)
        except Exception as e:
            print(e,'login check error')
            time.sleep(10)


if __name__== "__main__":
    
    threading.Thread(target=check_login).start()
    # serve(app, port=5000)
    app.run(debug=True)





# {"side":"buy","token":"5cqc22bwdtsq646fhjc6g0f4lj07s42d"}

    #  https://8145-2409-4052-4e94-19cd-893e-4e0e-c54d-3f54.ngrok.io/orders



# import sqlite3
# conn = sqlite3.connect('pmo.sqlite3', timeout=10)
# cur=conn.cursor()

# cur.execute("UPDATE User_config SET token_date='2022-06-02' WHERE id='2'")

# conn.commit()




