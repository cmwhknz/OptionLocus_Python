import os
import urllib
import zipfile
import time
import csv
import sys
import requests
import json
import MySQLdb


import smtplib, os
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders

op_day = 1

def send_mail(files=[]):
    
    From = 
    To = 
    
    msg = MIMEMultipart()
    msg['From'] = From
    msg['To'] = To
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = '20' + currentTime

    msg.attach( MIMEText('Sample') )

    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="{0}"'.format(os.path.basename(f)))
        msg.attach(part)

    smtp = smtplib.SMTP('smtp.gmail.com', 587)
    #if isTls: smtp.starttls()
    smtp.starttls()
    smtp.login()
    smtp.sendmail(From, To, msg.as_string())
    smtp.quit()


def getunzipped(theurl, thedir):
  name = os.path.join(thedir, 'temp.zip')
  try:
    name, hdrs = urllib.urlretrieve(theurl, name)
  except IOError, e:
    print "Can't retrieve %r to %r: %s" % (theurl, thedir, e)
    return False
  try:
    z = zipfile.ZipFile(name)
  except zipfile.error, e:
    print "Bad zipfile (from %r): %s" % (theurl, e)
    return False
  for n in z.namelist():
    dest = os.path.join(thedir, n)
    destdir = os.path.dirname(dest)
    if not os.path.isdir(destdir):
      os.makedirs(destdir)
    data = z.read(n)
    f = open(dest, 'w')
    f.write(data)
    f.close()
  z.close()
  os.unlink(name)
  return True

from datetime import datetime, timedelta

yesterday = datetime.now() - timedelta(days=op_day)
currentTime = yesterday.strftime("%y%m%d")
currentMonth = yesterday.strftime("%y%m")

dirdir = "/home/pi/Desktop/HKEXCSV"
#files=["hsif", "hsio", "vhsf", "hhif", "hhio", "dqe"]
files=["hsif","hsio", "dqe"]
extPath=[dirdir + '/Futures/',dirdir + '/Options/',dirdir + '/Stocks/']
filePath = []
i = 0

for f in files:
    urlurl = "https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/" + f + currentTime + ".zip" 
    status = getunzipped(urlurl, extPath[i])
    filePath.append(extPath[i] + f + currentTime + ".csv")
    i = i+1

connection = MySQLdb.connect( )
cursor = connection.cursor()



#Futures Reading
    
c = open(extPath[0] + 'hsif' + currentTime + ".csv",'rt')
hsifuturecsvpath = "/home/pi/Desktop/HKEXCSV/Parse/HSIFuture/20" + currentTime + "hsifutures.csv"
f = open(hsifuturecsvpath,"wb")
try:
    i=0
    j=0               
    reader = csv.reader(c)
    writer = csv.writer(f)
    for row in reader:
        i=i+1
        if i == 17:
            if row[1] == 'EXPIRED':
                #print row[1]
                j=6
            else:
                j=5
        j = j - 1
        if j > 0 and j < 5:
            contractMonth = datetime.strptime(row[0], '%b-%y').strftime('%Y%m')
            date = '20' + currentTime
            db_id = '20' + currentTime + datetime.strptime(row[0], '%b-%y').strftime('%m')
            db_open = row[6]
            db_high = row[7]
            db_low = row[8]
            db_close = row[10]
            if row[11] == '-':
                db_change = 0
            else:
                db_change = row[11]
            db_volume = row[9]
            db_grossio = row[15]
            if row[16] == '-':
                db_iochange = 0
            else:
                db_iochange = row[16]
            #apiURL= "http://parentrol.com/ohayouapp/techjt/hsif.php?id="+db_id+"&date="+date+"&month=20"+currentMonth+"&contractmonth="+contractMonth+"&open="+db_open+"&high="+db_high+"&low="+db_low+"&close="+db_close+"&change="+db_change+"&volume="+db_volume+"&grossio="+db_grossio+"&iochange="+db_iochange
            #print db_id
            writer.writerow((db_id,date,"20"+currentMonth,contractMonth,db_open,db_high,db_low,db_close,db_change,db_volume,db_grossio,db_iochange))
finally:
    c.close
    #print r
    print 'HSI Futures Done'



#Options Reading
    
c = open(extPath[1] + 'hsio' + currentTime + ".csv",'rt')
hsioptioncsvpath = "/home/pi/Desktop/HKEXCSV/Parse/HSIOption/20" + currentTime + "hsioptions.csv"
f = open(hsioptioncsvpath,"wb")
try:
    reader = csv.reader(c)
    writer = csv.writer(f)
    i = 0
    j = 0
    for row in reader:
        i = i + 1
        if (row) and i < 420:
            try:
                time = datetime.strptime(row[0], '%b-%y').strftime('%Y%m')
                dateCell = True
            except ValueError:
                #print 'Not Value'
                dateCell = False
                j = j + 1
                #print j
            if dateCell:
                date = '20' + currentTime
                contractMonth = time
                db_strike = row[1]
                db_cp = row[2]
                db_id = '20' + currentTime + datetime.strptime(row[0], '%b-%y').strftime('%m') + db_strike + db_cp
                db_close = row[6]
                db_grossoi = row[10]
                db_oichange = row[11]
                db_iv = row[8]
                db_volume = row[9]
                writer.writerow((db_id,date,contractMonth,db_strike,db_cp,db_close,db_grossoi,db_oichange,db_iv,db_volume))
                #apiURL = 'http://parentrol.com/ohayouapp/techjt/hsio.php?id='+db_id+'&date='+date+'&contractmonth='+contractMonth+'&strike='+db_strike+'&cp='+db_cp+'&close='+db_close+'&grossoi='+db_grossoi+'&oichange='+db_oichange+'&iv='+db_iv+'&volume='+db_volume
                #r = requests.get(apiURL)
                #print(json.loads(r.text))
                #print j
            if j == 30:
                #print db_id
                break

finally:
    c.close
    print 'HSI Options Done'

#update current daye
apiURL="http://parentrol.com/ohayouapp/techjt/update_current_data.php?date=20"+currentTime+"&contractmonth=20"+currentMonth+"&close="+db_close
r = requests.get(apiURL)


#Stocks Reading

c = open(extPath[2] + 'dqe' + currentTime + ".csv",'rt')
stockoptioncsvpath = "/home/pi/Desktop/HKEXCSV/Parse/StockOption/20" + currentTime + "stockoptions.csv"
stockclosesvpath = "/home/pi/Desktop/HKEXCSV/Parse/StockClose/20" + currentTime + "stockclose.csv"
f = open(stockoptioncsvpath,"wb")
g = open(stockclosesvpath,"wb")
try:
    reader = csv.reader(c)
    writer = csv.writer(f)
    closewriter = csv.writer(g)
    i = 0
    j = 0
    k = -1
    stockcodes = []
    stocknames = []
    stockcode = False
    stockname = False
    
    for row in reader:
        i = i + 1
        j = j - 1
        if (row):
            if stockcode:
                if row[0] == "":
                    stockcode = False
                else:
                    stockcodes.append(row[0])
                    stocknames.append(row[1])
                    #print row[0] + '  ' + row[1]   
            elif row[0] == "HKATS CODE":
                stockcode = True;
            elif row[0] == "CONTRACT":
                k = k + 1
                j = 2
                #print stockcodes[k]
            elif row[0] == "CLASS":
                k = -1
            elif k >= 0:
                if not (row[0] == '') and (j == 0):
                    stockclose= row[3].split("$",1)[1]
                    stockclose = float(stockclose)
                    #print stockclose
                    closewriter.writerow((stockcodes[k],stockclose))
                if not (row[0] == '') and (j < 0):
                    date = '20' + currentTime
                    contractMonth = datetime.strptime(row[0], '%b%y').strftime('%Y%m')
                    strike = row[1]
                    cp = row[2]
                    close = row[6]
                    if row[7] == '-':
                        closechange = 0
                    else:
                        closechange = float(row[7])
                    iv = row[8]
                    volume = row[9]
                    grossoi = row[10]
                    if row[11] == '-':
                        oichange = 0
                    else:
                        oichange = row[11]
                    db_id = date + contractMonth + stockcodes[k] + strike + cp
                    #print db_id + ' ' + stockcodes[k] + ' ' + date + ' ' + contractMonth + ' ' + strike + ' ' + cp + ' ' + close + ' ' + str(closechange) + ' ' + iv + ' ' + volume + ' ' + grossoi + ' ' + oichange
                    writer.writerow((db_id,stockcodes[k],date,contractMonth,strike,cp,close,closechange,iv,volume,grossoi,oichange))
finally:
    c.close
    f.close()
    g.close()
    print "Stock Option Done"
    print "Stock Close Done"
    query = "LOAD DATA LOCAL INFILE '" + stockoptioncsvpath + "' INTO TABLE stockoption FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n'"
    cursor.execute(query)
    query = "LOAD DATA LOCAL INFILE '" + hsifuturecsvpath + "' INTO TABLE hsif FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n'"
    cursor.execute(query)
    query = "LOAD DATA LOCAL INFILE '" + hsioptioncsvpath + "' INTO TABLE hsio FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n'"
    cursor.execute(query)
    query = "DELETE FROM stockclose WHERE 1"
    cursor.execute(query)
    query = "LOAD DATA LOCAL INFILE '" + stockclosesvpath + "' INTO TABLE stockclose FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n'"
    cursor.execute(query)
    connection.commit()
    #print 'Options Done'
cursor.close()
connection.close
#send_mail(filePath)
#print "Download Done"
print "mail sent"

