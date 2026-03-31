import mailbox

import sqlite3
import time
#import ssl
#import urllib.request, urllib.parse, urllib.error
#from urllib.parse import urljoin
#from urllib.parse import urlparse
import re
from datetime import datetime, timedelta


################################################
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Not all systems have this so conditionally define parser
try:
    import dateutil.parser as parser
except:
    pass

def parsemaildate(md) :
    # See if we have dateutil
    try:
        pdate = parser.parse(tdate)
        test_at = pdate.isoformat()
        return test_at
    except:
        pass

    # Non-dateutil version - we try our best

    pieces = md.split()
    notz = " ".join(pieces[:4]).strip()

    # Try a bunch of format variations - strptime() is *lame*
    dnotz = None
    for form in [ '%d %b %Y %H:%M:%S', '%d %b %Y %H:%M:%S',
        '%d %b %Y %H:%M', '%d %b %Y %H:%M', '%d %b %y %H:%M:%S',
        '%d %b %y %H:%M:%S', '%d %b %y %H:%M', '%d %b %y %H:%M' ] :
        try:
            dnotz = datetime.strptime(notz, form)
            break
        except:
            continue

    if dnotz is None :
        # print 'Bad Date:',md
        return None

    iso = dnotz.isoformat()

    tz = "+0000"
    try:
        tz = pieces[4]
        ival = int(tz) # Only want numeric timezone values
        if tz == '-0000' : tz = '+0000'
        tzh = tz[:3]
        tzm = tz[3:]
        tz = tzh+":"+tzm
    except:
        pass

    return iso+tz

    
conn = sqlite3.connect('gmail_content.sqlite')
cur = conn.cursor()

mbox = mailbox.mbox('Inbox-002.mbox')

cur.execute('''CREATE TABLE IF NOT EXISTS Messages
    (id INTEGER UNIQUE, email TEXT, sent_at TEXT UNIQUE,
     subject TEXT)''')

     
# Pick up where we left off
start = None
cur.execute('SELECT max(id) FROM Messages' )
try:
    row = cur.fetchone()
    if row is None :
        start = 0
    else:
        start = row[0]
except:
    start = 0

if start is None : start = 0

many = 0
count = 0
for message in mbox:
    if( many < 1 ) :
        conn.commit()
        sval = input('How many messages:')
        if ( len(sval) < 1 ) : break
        many = int(sval)

    start = start + 1
    cur.execute('SELECT id FROM Messages WHERE id=?', (start,) )
    try:
        row = cur.fetchone()
        if row is not None : continue
    except:
        row = None

    many = many - 1

    count = count + 1
    
    email=message['from']
    email=re.findall('\S+?@\S+',email)
    if len(email)==1:
        email = email[0]
        email=email.lower()
        email = email.replace("<","")
        email = email.replace(">","")
        email = email.replace("\"","")
    else: continue
    
    sent_at=message['date']
    try:
        date = parser.parse(sent_at)
        sent_at=date.isoformat()    
        #sent_at=sent_at[:26]
    except:
        continue
        
    #print(" ",sent_at)
    
    subject=message['subject']
    #if len(subject) == 1 : subject = subject[0].strip().lower()
    
    print("   ",email,sent_at,subject)
    
    try:
        cur.execute('''INSERT OR IGNORE INTO Messages (id, email, sent_at, subject)
        VALUES (?, ?, ?, ?)''', ( start, email, sent_at, subject))
    except:
        continue
        
    if count % 50 == 0 : conn.commit()
    if count % 100 == 0 : time.sleep(1)

conn.commit()

cur.close()