from datetime import datetime
import pandas as pd
from flask import Flask, render_template
from flask import request, session
import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import wget
import os
import glob
from dotenv import dotenv_values

app = Flask(__name__)
config = dotenv_values(".env")
app.secret_key = "hello"
os.environ["aws_access_key_id"] = config["aws_access_key_id"]
os.environ["aws_secret_access_key"] = config["aws_secret_access_key"]
os.environ["region_name"] = config["region_name"]

dynamodb = boto3.resource('dynamodb', aws_access_key_id=os.getenv("aws_access_key_id"),
                          aws_secret_access_key=os.getenv("aws_secret_access_key"),
                          region_name=os.getenv("region_name"))
                          # aws_session_token=os.getenv("aws_session_token"),
                          # )

def upload_file(file_name, bucket):

    s3_client = boto3.client('s3', aws_access_key_id=os.getenv("aws_access_key_id"),
                          aws_secret_access_key=os.getenv("aws_secret_access_key"))
                          # aws_session_token=os.getenv("aws_session_token"))
    s3_client.upload_file(file_name, bucket, file_name.split('/')[-1])


import uuid

def my_random_string(string_length=10):
    """Returns a random string of length string_length."""
    random = str(uuid.uuid4()) # Convert UUID format to a Python string.
    random = random.upper() # Make all characters uppercase.
    random = random.replace("-","") # Remove the UUID '-'.
    return random[0:string_length] # Return the random string.


@app.route('/')
def root():
    if 'CurrentActiveUser' in session:
        return render_template('index.html',
                               userlog = "logout",
                               userlogimage = "log-out",
                               userlogtext = " Logout")
    else:
        return render_template('index.html',
                               userlog="login",
                               userlogimage="log-in",
                               userlogtext=" Login")

@app.route('/forum')
def forum():
    if 'CurrentActiveUser' in session:
        email = session["CurrentActiveUser"]

        tablesubscribe = dynamodb.Table('subscribe')
        responsesubscribe = pd.DataFrame(tablesubscribe.scan()['Items'])
        emptydflist = []
        if 'email' in responsesubscribe:
            usersubscribe = responsesubscribe.loc[responsesubscribe.email == email, ['title', 'count']]

            # usersubscribetranspose = usersubscribe.T.to_dict().values()

            try:
                for i, j in zip(usersubscribe.title,usersubscribe.loc[:, 'count']):
                    table = dynamodb.Table('music')
                    response = table.query(
                        KeyConditionExpression=Key('title').eq(i)
                    )
                    if len(response['Items']) > 0:
                        response['Items'][0]['count'] = str(j)
                        emptydflist.append(response['Items'][0])
            except:
                pass

        return render_template('forum.html',
                               user_name=session["CurrentActiveUserName"],
                               musicdict=emptydflist,
                               )
    else:
        return render_template('notification.html',
                               notification="Please Login to Access this Page",
                               userlog="login",
                               userlogimage="log-in",
                               userlogtext=" Login"
                               )
@app.route('/deletedata', methods=['GET', 'POST'])
def deletedata():
    req = request.form
    countmusic = req.get("countmusic")
    email = session["CurrentActiveUser"]
    tablesubscribe = dynamodb.Table('subscribe')
    print(countmusic)
    response = tablesubscribe.delete_item(
        Key={
            'count': countmusic
        })

    responsesubscribe = pd.DataFrame(tablesubscribe.scan()['Items'])
    emptydflist = []
    if 'email' in responsesubscribe:
        usersubscribe = responsesubscribe.loc[responsesubscribe.email == email, ['title', 'count']]

        # usersubscribetranspose = usersubscribe.T.to_dict().values()

        try:
            for i, j in zip(usersubscribe.title, usersubscribe.loc[:, 'count']):
                table = dynamodb.Table('music')
                response = table.query(
                    KeyConditionExpression=Key('title').eq(i)
                )
                if len(response['Items']) > 0:
                    response['Items'][0]['count'] = str(j)
                    emptydflist.append(response['Items'][0])
        except:
            pass

    return render_template('forum.html',
                           user_name=session["CurrentActiveUserName"],
                           musicdict=emptydflist,
                           )

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == "POST":

        req = request.form
        email = str.lower(req.get("email"))
        password = req.get("password")

        table = dynamodb.Table('login')

        response = table.query(
            KeyConditionExpression=Key('email').eq(email)
        )

        if len(response["Items"]) < 1:
            return render_template('notification.html',
                                   notification = "Email or Password is Invalid",
                                   userlog="login",
                                   userlogimage="log-in",
                                   userlogtext=" Login"
                                   )
        elif response['Items'][0]['password'] != password:
            return render_template('notification.html',
                                   notification = "Email or Password is Invalid",
                                   userlog="login",
                                   userlogimage="log-in",
                                   userlogtext=" Login"
                                   )
        else:
            session["CurrentActiveUser"] = email
            session["CurrentActiveUserName"] = response['Items'][0]['user_name']
            if 'CurrentActiveUser' in session:
                tablesubscribe = dynamodb.Table('subscribe')
                responsesubscribe = pd.DataFrame(tablesubscribe.scan()['Items'])
                emptydflist = []
                if 'email' in responsesubscribe:
                    usersubscribe = responsesubscribe.loc[responsesubscribe.email == email, ['title', 'count']]

                    # usersubscribetranspose = usersubscribe.T.to_dict().values()

                    try:
                        for i, j in zip(usersubscribe.title, usersubscribe.loc[:, 'count']):
                            table = dynamodb.Table('music')
                            response = table.query(
                                KeyConditionExpression=Key('title').eq(i)
                            )
                            if len(response['Items']) > 0:
                                response['Items'][0]['count'] = str(j)
                                emptydflist.append(response['Items'][0])
                    except:
                        pass

                return render_template('forum.html',
                                       user_name=session["CurrentActiveUserName"],
                                       musicdict=emptydflist)

    else:
        return render_template('login.html')



@app.route('/register', methods=['GET', 'POST'])
def register():
    table = dynamodb.Table('login')
    if request.method == "POST":
        req = request.form

        #Getting Form data
        email = str.lower(req.get("email"))
        password = str(req.get("password"))
        user_name = str.lower(req.get("user_name"))


        response = table.query(
            KeyConditionExpression=Key('email').eq(email)
        )

        if len(response["Items"]) < 1:
            table.put_item(
                Item={
                    'email': email,
                    'user_name': user_name,
                    'password': password,
                }
            )
            return render_template('login.html')

        else:
            return render_template('notification.html',
                                   notification="The email already exists",
                                   userlog="login",
                                   userlogimage="log-in",
                                   userlogtext=" Login"
                                   )

    if 'CurrentActiveUser' in session:
        return render_template('register.html',
                               userlog = "logout",
                               userlogimage = "log-out",
                               userlogtext = " Logout")
    else:
        return render_template('register.html',
                               userlog="login",
                               userlogimage="log-in",
                               userlogtext=" Login")



@app.route('/createtable', methods=['GET', 'POST'])
def createtable():
    table_list = boto3.client('dynamodb', aws_access_key_id=os.getenv("aws_access_key_id"),
                              aws_secret_access_key=os.getenv("aws_secret_access_key"),
                              region_name=os.getenv("region_name")).list_tables()["TableNames"]
                # aws_session_token=os.getenv("aws_session_token")).list_tables()["TableNames"]

    if "music1" not in table_list:
        table = dynamodb.create_table(
            TableName='music1',
            KeySchema=[
                {
                    'AttributeName': "title",
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'title',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5000,
                'WriteCapacityUnits': 5000
            }
        )

        # # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName='music1')

        with open('static/data/a2.json') as f:
            data = json.load(f)

        for entry in data['songs']:
            table.put_item(
                Item={
                    'title': entry['title'],
                    'artist': entry['artist'],
                    'year': entry['year'],
                    'web_url': entry['web_url'],
                    'img_url': entry['img_url'],
                }
            )
            imagename = wget.download(entry["img_url"], out="static/songimage")
            upload_file(imagename, "demo-s3810585")

        files = glob.glob('static/songimage/*')
        for f in files:
            os.remove(f)

        if 'CurrentActiveUser' in session:
            return render_template('index.html',
                                   userlog="logout",
                                   userlogimage="log-out",
                                   userlogtext=" Logout")
        else:
            return render_template('index.html',
                                   userlog="login",
                                   userlogimage="log-in",
                                   userlogtext=" Login")
    else:
        if 'CurrentActiveUser' in session:
            return render_template('notification.html',
                                   notification="Table already exist",
                                   userlog="logout",
                                   userlogimage="log-out",
                                   userlogtext=" Logout")
        else:
            return render_template('notification.html',
                                   notification="Table already exist",
                                   userlog="login",
                                   userlogimage="log-in",
                                   userlogtext=" Login")

@app.route('/subscribeartist', methods=['GET', 'POST'])
def subscribeartist():
    if request.method == "POST":
        table = dynamodb.Table('music')
        req = request.form
        title = req.get("title")
        email = session["CurrentActiveUser"]
        # print(title)
        tablesubscribe = dynamodb.Table('subscribe')
        tablesubscribe.put_item(
                Item={
                    'count': my_random_string(),
                    'email': email,
                    'title': str(title),
                }
            )

        responsesubscribe = pd.DataFrame(tablesubscribe.scan()['Items'])

        # usersubscribetranspose = usersubscribe.T.to_dict().values()
        emptydflist = []
        if 'email' in responsesubscribe:
            usersubscribe = responsesubscribe.loc[responsesubscribe.email == email, ['title', 'count']]

            # usersubscribetranspose = usersubscribe.T.to_dict().values()

            try:
                for i, j in zip(usersubscribe.title, usersubscribe.loc[:, 'count']):
                    table = dynamodb.Table('music')
                    response = table.query(
                        KeyConditionExpression=Key('title').eq(i)
                    )
                    if len(response['Items']) > 0:
                        response['Items'][0]['count'] = str(j)
                        emptydflist.append(response['Items'][0])
            except:
                pass


        return render_template('forum.html',
                               user_name=session["CurrentActiveUserName"],
                               musicdict=emptydflist)


@app.route('/queryprocess', methods=['GET', 'POST'])
def queryprocess():
    if request.method == "POST":
        # files = glob.glob('static/froms3/*')
        # for f in files:
        #     os.remove(f)
        s3 = boto3.resource('s3', aws_access_key_id=os.getenv("aws_access_key_id"),
                          aws_secret_access_key=os.getenv("aws_secret_access_key"))
                          # aws_session_token=os.getenv("aws_session_token"))
        storage = s3.Bucket('demo-s3810585')
        req = request.form
        title = req.get("title")
        year = req.get("year")
        artist = req.get("artist")
        table = dynamodb.Table('music')
        response = pd.DataFrame(table.scan()['Items'])

        if len(title) > 0:
            response = response.loc[response.title == title, :]
        if len(year) > 0:
            response = response.loc[response.year == year, :]
        if len(artist) > 0:
            response = response.loc[response.artist == artist, :]

        response = response.T.to_dict().values()
        for i in response:
            storage.download_file(i["img_url"].split('/')[-1], 'static/froms3/'+i["img_url"].split('/')[-1])

        if len(response) < 1:
            if 'CurrentActiveUser' in session:
                return render_template('notification.html',
                                       notification="No result is retrieved",
                                       userlog="logout",
                                       userlogimage="log-out",
                                       userlogtext=" Logout")
            else:
                return render_template('notification.html',
                                       notification="No result is retrieved",
                                       userlog="login",
                                       userlogimage="log-in",
                                       userlogtext=" Login")
        else:
            return render_template('subscribe.html',
                                   resultlist = response,
                                   userlog="login",
                                   userlogimage="log-in",
                                   userlogtext=" Login")


@app.route('/logout', methods=['GET', 'POST'])
def logout():
    session.pop("CurrentActiveUser", None)
    session.pop("CurrentActiveUserName", None)
    return render_template('login.html')


if __name__ == '__main__':

    app.run(host='0.0.0.0', port=8080, debug=True)