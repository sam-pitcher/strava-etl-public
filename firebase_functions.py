import time
import base64
import hashlib

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from strava_sdk import get_tokens_with_refresh_token

cred = credentials.Certificate("strava-firebase.json")

firebase_admin.initialize_app(cred)
# you can use this when setting the service account key as:
# export GOOGLE_APPLICATION_CREDENTIALS="/home/user/Downloads/service-account-file.json"
# default_app = firebase_admin.initialize_app()
db = firestore.client()

def register_user(username, password):
    password_bytes = password.encode('UTF-8')
    password_hash = base64.b64encode(hashlib.md5(password_bytes).digest())
    password_hash_str = str(password_hash)[1:]
    doc_ref = db.collection(u'users').document(username)
    doc_ref.set({
        u'username': username,
        u'password_hash': password_hash_str
    })
    return(None)

def get_all_users():
    users_ref = db.collection(u'users').stream()
    user_username_list = []
    for user in users_ref:
        user_username_list.append(user.id)
    return user_username_list

def get_current_attributes(username, password_hash=None):
    if password_hash is None:
        users = db.collection(u'users').where(u'username', u'==', username).stream()
    else:
        users = db.collection(u'users').where(u'username', u'==', username).where(u'password_hash', u'==', password_hash).stream()
    this_user = {}
    for user in users:
        this_user = user.to_dict()
    return(this_user)

def add_user_strava_code(username, strava_code):
    this_user = get_current_attributes(username)
    this_user['strava_code'] = strava_code
    doc_ref = db.collection(u'users').document(username)
    doc_ref.set(this_user)
    return None

def update_user_strava_codes(username, access_token, refresh_token, expires_at):
    this_user = get_current_attributes(username)
    this_user['access_token'] = access_token
    this_user['refresh_token'] = refresh_token
    this_user['expires_at'] = expires_at
    doc_ref = db.collection(u'users').document(username)
    doc_ref.set(this_user)
    print('Tokens updated.')
    return None

def get_access_token(username):
    users = db.collection(u'users').where(u'username', u'==', username).stream()
    for user in users:
        user_dict = user.to_dict()
        try:
            strava_code = user_dict['strava_code']
            access_token = user_dict['access_token']
            refresh_token = user_dict['refresh_token']
            expires_at = user_dict['expires_at']
        except:
            strava_code = None
            access_token = None
            refresh_token = None
            expires_at = 0

        print(strava_code)
        print(f'{user.id} => {user.to_dict()}')

    time_now = time.time()
    print(f'Time now (epoch): {time_now}')
    print(f'Current expires at (epoch): {expires_at}')
    print(f'Current access token: {access_token}')
    print(f'Current refresh token: {refresh_token}')

    if(expires_at<time_now):
        print('Access Token is not valid, need to refresh token...')
        access_token, refresh_token, expires_at = get_tokens_with_refresh_token(refresh_token)
        print(f'New expires at (epoch): {expires_at}')
        print(f'New access token: {access_token}')
        print(f'New refresh token: {refresh_token}')
        update_user_strava_codes(username, access_token, refresh_token, expires_at)
    else:
        print('The tokens are valid. No need to update...')

    return(access_token)

# may need to rewrite this function!
def check_user(username, password):
    password_bytes = password.encode('UTF-8')
    password_hash = base64.b64encode(hashlib.md5(password_bytes).digest())
    password_hash_str = str(password_hash)[1:]
    this_user = get_current_attributes(username, password_hash_str)
    if len(this_user) == 0:
        is_user = False
        print(f"{username} doesn't exists as a user or the password is incorrect")
    # Need to change this incase of more than one user!
    elif len(this_user) > 0:
        is_user = True
        print(f"{username} can sign in successfully")
    else:
        is_user = False
        print('There are more than one user in the table with that username')
    return(is_user)

def check_user_exists(username):
    this_user = get_current_attributes(username)
    if len(this_user) == 0:
        user_exists = False
        print(f"{username} doesn't exist, so can be created")
    elif len(this_user) > 0:
        user_exists = True
        print(f"{username} already exists, so cannot be created")
    else:
        user_exists = False
        print('User can be created')

    return(user_exists)

def is_connected_to_strava(username):
    this_user = get_current_attributes(username)
    try:
        if len(this_user['strava_code']) > 3:
            return True
        else:
            return False
    except:
        return False