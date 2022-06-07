import tweepy
from googleapiclient.discovery import build
from gcloud import storage
from google.cloud import storage as storage1
from google.cloud import secretmanager

client = secretmanager.SecretManagerServiceClient()
project_id = "tweets-listening-tool"
bucket_name = "tweets_listener"
csv_filename = 'robot_mascot_tweets.csv'
request = {
    "name": f"projects/766760202834/secrets/twitter_config_secret/versions/1"}
response = client.access_secret_version(request)
twitter_secret_key = response.payload.data.decode("UTF-8")
request = {"name": f"projects/766760202834/secrets/robot_mascot_gs_id/versions/1"}
response = client.access_secret_version(request)
googlesheet_id = response.payload.data.decode("UTF-8")


def fetchTweets(name_val):
    auth_key = eval(twitter_secret_key)
    auth = tweepy.OAuthHandler(auth_key["api_key"], auth_key["api_key_secret"])
    auth.set_access_token(auth_key["access_token"],
                          auth_key["access_token_secret"])
    api = tweepy.API(auth)
    try:
        return tweepy.Cursor(api.user_timeline, screen_name=name_val, count=100, tweet_mode="extended").items(200)
    except tweepy.errors.BadRequest:
        return 0


def save_in_gs(handles, keywords):
    service = build('sheets', 'v4')

    # Call the Sheets API
    sheet = service.spreadsheets()
    data = []
    for handle in handles:
        if "/" in handle:
            handle = handle.split('/')
            pass
        elif "@" in handle:
            handle = handle.split('@')
            pass
        else:
            handle = handle.split('/')
        cursor = fetchTweets(handle[-1])
        if cursor != 0:
            try:
                for i in cursor:
                    for keyword in keywords:
                        test_list = i.full_text.split(" ")
                        word = keyword.split(" ")
                        flag = False
                        for test in range(len(test_list)):
                            if len(word) > 1:
                                if word[0].lower() in test_list[test].lower():
                                    try:
                                        if word[1].lower() == test_list[test+1].lower():
                                            if "@" not in test_list[test]:
                                                flag = True
                                        elif ("".join(word)).lower() in test_list[test].lower():
                                            if "@" not in test_list[test]:
                                                flag = True
                                    except:
                                        if ("".join(word)).lower() in test_list[test].lower():
                                            if "@" not in test_list[test]:
                                                flag = True
                            else:
                                if word[0].lower() in test_list[test].lower():
                                    if "@" not in test_list[test]:
                                        flag = True
                        if(flag == True):
                            print((i.user.screen_name))
                            x = (i.created_at)
                            day = x.strftime("%d")
                            month = x.strftime("%b")
                            year = x.strftime("%Y")
                            date = str(day) + "th " + \
                                str(month) + " " + str(year)
                            data.append([i.user.screen_name, date, keyword, i.full_text,
                                        'https://twitter.com/twitter/statuses/'+str(i.id)])
            except:
                print(i, " No or unexpected value returned")
                pass
        del(cursor)
    print(data)
    request = sheet.values().append(spreadsheetId=googlesheet_id,
                                    range="Output!A2:E2", valueInputOption="USER_ENTERED",
                                    insertDataOption="INSERT_ROWS", body={"values": data}).execute()
    print("Done")


def main(request):
    service = build('sheets', 'v4')

    # Call the Sheets API
    sheet = service.spreadsheets()
    keywords = []
    result = sheet.values().get(spreadsheetId=googlesheet_id,
                                range="Input!A2:A").execute()
    names = result.get('values', [])
    name_str = ""
    for name in names:
        name_str = name_str + str(''.join(name) + "\n")

    client = storage.Client(project=project_id)

    result = sheet.values().get(spreadsheetId=googlesheet_id,
                                range="Input!B2:B").execute()
    words = result.get('values', [])
    for word in words:
        keywords.append(''.join(word))

    storage_client = storage1.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(csv_filename)
    gs_file = blob.download_as_text()
    try:
        f = gs_file.split("\n")
        f.pop()
    except Exception as error:
        error_string = str(error)
        print(error_string)
    name_list = name_str.split(("\n"))
    name_list.pop()
    new_list = list(set(name_list) - set(f))
    new_str = gs_file
    if len(new_list) != 0:
        for name in new_list:
            new_str = new_str + str(''.join(name) + "\n")
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(csv_filename)
        blob.upload_from_string(new_str)
        save_in_gs(new_list, keywords)
    return("Done")
