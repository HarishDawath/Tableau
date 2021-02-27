import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.lead import Lead

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from tableauhyperapi import HyperProcess, Telemetry, Connection, UnclosedObjectWarning, CreateMode, NOT_NULLABLE, NULLABLE, SqlType,TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName
import sys
from time import sleep
import smtplib
import email
from zipfile import ZipFile
from datetime import datetime, timedelta
import sys
import os

mailcontent= " "
################################  For Increment Load ##############################
start_date=str((datetime.today() - timedelta(days=3)).date())
end_date=str(((datetime.today()).date())) #This dates are configured based on US timings (10pm)
################################  For Full load ##############################
#start_date="2021-01-31" # format YYYY-MM-DD
#end_date="2021-02-15"  # format YYYY-MM-DD
################################ Hard Coded Details ############################
Accounts=[] #Provide list of accounts created in FB page
my_app_id = '' #Provide the application id created on Facebook 
my_app_secret = '' #Provide application secret code
my_access_token = '' #Provide the access token which can generated from Facebook Graph Explorer


def LogFileWrite(message):
    global mailcontent
    try:
        LogFile = open("\Facebook\Facebook_log_file.txt", "a") 
    except Exception as ex:
        print("Error opening Log File. Exiting.\nLog File: " + LogFile)
        print(str(ex))
        exit(1)
    #if message == "---------------------------Script End-------------------------":
    #    LogFile.write(datetime.datetime.now().strftime("%m/%d/%y %H:%M:%S") + ": " + message + "\n\n")
    LogFile.write(datetime.now().strftime("%m/%d/%y %H:%M:%S") + ": " + message + "\n")
    mailcontent += str(datetime.now().strftime("%m/%d/%y %H:%M:%S") + ": " + message + "\n")
    LogFile.flush()

def  SendEmailMessage():
    EmailMessageTo="" #Provide Email addresses to which mail id script should send a mail
    EmailServer = "" #Provide the Email server name 
    EmailMessageFrom = "" #Provide the email address from which mail id script should send a mail
    if mailcontent != None and EmailMessageFrom != None and EmailMessageTo != None and EmailServer != None:
        Message = email.message.EmailMessage()
        Message["Subject"] = "Facebook Campaign Data extraction and publishing to server" + str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        Message["From"] = EmailMessageFrom
        Message["To"] = EmailMessageTo
        Message.set_content(str(mailcontent))
        try:
            with smtplib.SMTP(EmailServer) as SMTPServer:
                SMTPServer.send_message(Message)
        except Exception as ex:
            LogFileWrite("Failed to send E-mail "+ str(ex))
def Backup():
    try:
        till_date=str(((datetime.today() - timedelta(days=2)).date() if ((datetime.today() - timedelta(days=1)).date().weekday())!='Sunday' else (datetime.today() - timedelta(days=4)).date()))
        zipfilname= "\Facebook\Backup\Facebook_Campaigns_data_till_"+ till_date +".zip" 
        zipObj = ZipFile(zipfilname, 'w')
    # Add multiple files to the zip
        zipObj.write('E:\Facebook\Facebook_campaigns.hyper') 
        #zipObj.write('E:\Facebook\Report Files\Report.csv')
        zipObj.close()
        LogFileWrite("Successfully completed Back up of previous files at "+zipfilname)
        #sys.exit()
    except Exception as e:
        LogFileWrite("Uable to Backup the file due to Error" +str(e))
        SendEmailMessage()
        sys.exit()

def FB_data_request(my_app_id, my_app_secret, my_access_token,Accounts,start_date,end_date):
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
    
    fields = ['account_id','account_name','campaign_id','campaign_name','clicks','date_start','frequency','impressions','reach','spend']
    params = {'time_range':{'since': start_date,'until': end_date},'time_increment':'1'}
    FB_df=pd.DataFrame()
    account_id=[]
    account_name=[]
    campaign_id=[]
    campaign_name=[]
    clicks=[]
    date_start=[]
    frequency=[]
    impressions=[]
    
    reach=[]
    spend=[]
    for account in Accounts:
        my_account = AdAccount('act_'+account)
        campaigns = my_account.get_campaigns()._queue
        
        print(len(campaigns))
        for campaign in campaigns:
            print(campaign.get_id())
            
            #print(campaign)
            campinsights=campaign.get_insights(fields=fields,params=params)
            #print(campaign.get('full_view_reach'))
            print(len(campinsights))
            for inscampaign in campinsights:
                account_id.append(inscampaign['account_id'])
                account_name.append(inscampaign['account_name'])
                campaign_id.append(inscampaign['campaign_id'])
                campaign_name.append(inscampaign['campaign_name'])
                clicks.append(inscampaign['clicks'])
                date_start.append(inscampaign['date_start'])
                frequency.append(inscampaign['frequency'])
                impressions.append(inscampaign['impressions'])
                reach.append(inscampaign['reach'])
                spend.append(inscampaign['spend'])
    FB_df['Account Id']= account_id
    FB_df['Account Name']= account_name
    FB_df['Campaign Id']= campaign_id
    FB_df['Campaign Name']= campaign_name
    FB_df['Clicks']= clicks
    FB_df['Date']= date_start
    FB_df['Frequency']= frequency
    FB_df['Impressions']= impressions
    FB_df['Reach']= reach
    FB_df['Spend']= spend
    
    #FB_df.to_csv('example.csv')
    print(FB_df.head())
    return(FB_df)


def Full_refresh(result):
    LogFileWrite("Running Full refresh")
    try:
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyperprocess:
            print("The HyperProcess has started.")
            LogFileWrite("The HyperProcess has started.")
            print(hyperprocess.is_open)
            if hyperprocess.is_open==True:
                with Connection(hyperprocess.endpoint, 'Facebook_campaigns.hyper', CreateMode.CREATE_AND_REPLACE,) as connection: 
                    if connection.is_open==True:
                        print("The connection to the Hyper file is open.")
                        LogFileWrite("The connection to the Hyper file is open.")
                        connection.catalog.create_schema('Extract')
                        DataTable = TableDefinition(TableName('Extract','Campaign_data'),[
                        ############Below Columns are extracted from Report data API
                        TableDefinition.Column('Row_ID', SqlType.big_int()),
                        TableDefinition.Column('Inserted Date', SqlType.date()),
                        TableDefinition.Column('Date', SqlType.date()),
                        TableDefinition.Column('Account Id', SqlType.varchar(50)),
                        TableDefinition.Column('Account Name', SqlType.text()),
                        TableDefinition.Column('Campaign Id', SqlType.varchar(50)),
                        TableDefinition.Column('Campaign Name', SqlType.text()),
                        TableDefinition.Column('Impressions', SqlType.big_int()),
                        TableDefinition.Column('Clicks', SqlType.big_int()),
                        TableDefinition.Column('Reach', SqlType.big_int()),
                        TableDefinition.Column('Spend', SqlType.double()),
                        TableDefinition.Column('Frequency', SqlType.double()),
                        ])
                        print("The table is defined.")
                        LogFileWrite("Successfully Facebook Campaign Table is defined")
                        connection.catalog.create_table(DataTable)
                       # print(Campaign_df["Id"].dtype)
                        #print(range(len(Campaign_df["Id"])))
                        
                        with Inserter(connection, TableName('Extract','Campaign_data')) as inserter:
                            inserted_rows=1
                            row_id=1
                            for i in range(0,len(result["Campaign Id"])):
                                #print(str(result.loc[i,"CampaignId"]))
                                #print(result.loc[i,"Date"])
                                inserter.add_row([
                                int(row_id),
                                datetime.today(),
                                (datetime.strptime(result.loc[i,"Date"], '%Y-%m-%d')),
                                #(datetime.date(result.loc[i,"Date"])),#, "%Y-%m-%d")),
                                str(result.loc[i,"Account Id"]),
                                str(result.loc[i,"Account Name"]),
                                str(result.loc[i,"Campaign Id"]),
                                str(result.loc[i,"Campaign Name"]),
                                int(result.loc[i,"Impressions"]),
                                int(result.loc[i,"Clicks"]),
                                int(result.loc[i,"Reach"]),
                                float(result.loc[i,"Spend"]),
                                float(result.loc[i,"Frequency"])
                                ])
                                #print("instered")
                                row_id=row_id+1
                                inserted_rows=inserted_rows+1
                            inserter.execute()
                            print("Instered Rows are " +str(inserted_rows))
                            LogFileWrite("Instered Rows are " +str(inserted_rows))
                        table_name=TableName('Extract','Campaign_data')
                        Delet_query=f"DELETE FROM {table_name} WHERE " +'"'+ 'Row_ID'+'"'+" NOT IN("
                        Delet_query+="SELECT MAX("+'"'+'Row_ID'+'"'+f") FROM {table_name} "
                        Delet_query+="GROUP BY " +'"'+'Date'+'",'+'"'+'Campaign Id'+'",'+'"'+'Campaign Name'+'",'
                        Delet_query+='"'+'Account Id'+'",'+'"'+'Impressions'+'",'
                        Delet_query+='"'+'Clicks'+'",'+'"'+'Account Name'+'",'+'"'+'Reach'+'",'+'"'+'Spend'+'",'
                        Delet_query+='"'+'Frequency'+'")'
                        #print(Delet_query)
                        
                        connection.execute_command(Delet_query)
                        print("Deleted Duplicate rows")
                        LogFileWrite("Successfully deleted Duplicate rows")
                    else:
                        print("unable to open connection to hyper file")
                        LogFileWrite("unable to open connection to hyper file")
                if connection.is_open==True:
                    connection.close()
                    print("Connection to Hyper file closed")
                    LogFileWrite("Connection to Hyper file closed")
                else:
                    print("Connection to Hyper file closed")
                    LogFileWrite("Connection to Hyper file closed")
                    print("Connection is open or closed" + str(connection.is_open))
            else:
                print("Unable to start the Hyper process ")
                LogFileWrite("Unable to start the Hyper process ")
        if hyperprocess.is_open==True:
            hyperprocess.close()
            print("Forcefully shutted down the Hyper Process")
            LogFileWrite("Forcefully shutted down the Hyper Process")
        else:
            print("Hyper process is shutted down")
            LogFileWrite("Hyper process is shutted down")
            print("Connection is open or closed" + str(connection.is_open))
            print("process is open or closed" + str(hyperprocess.is_open))
    except HyperException as ex:
        LogFileWrite("There is exception in starting Tableau Hyper Process. Exiting...")
        LogFileWrite(str(ex))
        connection.close()
        hyperprocess.close()
        SendEmailMessage()
        sys.exit()
  

def Incremental_refresh(result):
    try:
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyperprocess:
            #print("The HyperProcess has started.")
            LogFileWrite("The HyperProcess has started.")
            print(hyperprocess.is_open)
            if hyperprocess.is_open==True:
                with Connection(hyperprocess.endpoint, 'Facebook_campaigns.hyper', CreateMode.NONE,) as connection: 
                    if connection.is_open==True:
                        print("The connection to the Hyper file is open.")
                        LogFileWrite("The connection to the Hyper file is open.")
                        
                        LogFileWrite("Successfully connected to Facebook Campaign data Table ")
                       # print(Campaign_df["Id"].dtype)
                        #print(range(len(result["Id"])))
                        table_name=TableName('Extract','Campaign_data')
                        max_rowid_query="SELECT MAX("+'"'+'Row_ID'+'"'+f") FROM {table_name}"
                        row_id=connection.execute_scalar_query(max_rowid_query)
                        row_id=row_id+1
                        #print(row_id)
                        with Inserter(connection, TableName('Extract','Campaign_data')) as inserter:
                            inserted_rows=1
                            for i in range(0,len(result["Campaign Id"])):
                                #print(result.loc[i,"Date"])
                                inserter.add_row([
                                int(row_id),
                                datetime.today(),
                                (datetime.strptime(result.loc[i,"Date"], '%Y-%m-%d')),
                                
                                str(result.loc[i,"Account Id"]),
                                str(result.loc[i,"Account Name"]),
                                str(result.loc[i,"Campaign Id"]),
                                str(result.loc[i,"Campaign Name"]),
                                int(result.loc[i,"Impressions"]),
                                int(result.loc[i,"Clicks"]),
                                int(result.loc[i,"Reach"]),
                                float(result.loc[i,"Spend"]),
                                float(result.loc[i,"Frequency"])
                                ])
                                #print("instered")
                                #i=i+1
                                inserted_rows=inserted_rows+1
                                row_id=row_id+1
                            inserter.execute()
                            #print("Instered Rows are " +str(inserted_rows))
                            LogFileWrite("Successfully rows are Instered")
                        table_name=TableName('Extract','Campaign_data')
                        Delet_query=f"DELETE FROM {table_name} WHERE " +'"'+ 'Row_ID'+'"'+" NOT IN("
                        Delet_query+="SELECT MAX("+'"'+'Row_ID'+'"'+f") FROM {table_name} "
                        Delet_query+="GROUP BY " +'"'+'Date'+'",'+'"'+'Campaign Id'+'",'+'"'+'Campaign Name'+'",'
                        Delet_query+='"'+'Account Id'+'",'+'"'+'Impressions'+'",'
                        Delet_query+='"'+'Clicks'+'",'+'"'+'Account Name'+'",'+'"'+'Reach'+'",'+'"'+'Spend'+'",'
                        Delet_query+='"'+'Frequency'+'")'
                        #print(Delet_query)
                        connection.execute_command(Delet_query)
                        print("Deleted Duplicate rows")
                        LogFileWrite("Successfully deleted Duplicate rows")                            
                    else:
                        print("unable to open connection to hyper file")
                        LogFileWrite("unable to open connection to hyper file")
                if connection.is_open==True:
                    connection.close()
                    print("Connection to Hyper file closed")
                    LogFileWrite("Connection to Hyper file closed")
                else:
                    print("Connection to Hyper file closed")
                    LogFileWrite("Connection to Hyper file closed")
                    #print("Connection is open or closed" + str(connection.is_open))
            else:
                print("Unable to start the Hyper process ")
                LogFileWrite("Unable to start the Hyper process ")
        if hyperprocess.is_open==True:
            hyperprocess.close()
            print("Forcefully shutted down the Hyper Process")
            LogFileWrite("Forcefully shutted down the Hyper Process")
        else:
            print("Hyper process is shutted down")
            LogFileWrite("Hyper process is shutted down")
            #print("Connection is open or closed" + str(connection.is_open))
            #print("process is open or closed" + str(hyperprocess.is_open))
    except HyperException as ex:
        LogFileWrite("There is exception in starting Tableau Hyper Process. Exiting...")
        LogFileWrite(str(ex))
        connection.close()
        hyperprocess.close()
        SendEmailMessage()
        sys.exit()

if __name__ == '__main__':
    LogFileWrite("###############################      SCRIPT STARTED    ################################")
    LogFileWrite("Start Date is " +str(start_date))
    LogFileWrite("End Date is " +str(end_date))
    till_date=str((datetime.today() - timedelta(days=1)).date() if str((datetime.today()).date().weekday())!='6' else (datetime.today() - timedelta(days=3)).date())
    zipfilname= "\Facebook\Backup\Facebook_data_till_"+ till_date +".zip"
    if os.path.isfile(zipfilname)==True:
        #print("Backup file already exists" +str(zipfilname))
        LogFileWrite("Backup file already exists" +str(zipfilname))
    else:
        print("Backup process started")
        LogFileWrite("Backup process started")
        Backup() 
    
    Facebook_df=FB_data_request(my_app_id, my_app_secret, my_access_token,Accounts,start_date,end_date)
    
    Incremental_refresh(Facebook_df) #Comment this line if running for Full refresh
    #Full_refresh(Facebook_df) # Uncomment this line if running for Full refresh 
    #print(result.head())
    Filepath= str(os.getcwd())+"\Facebook_campaigns.hyper"
    Login_cmd = "tabcmd login --server https://tableau.com/ --username  --password  --site " #Provide Server address, credentials and site name
    LogFileWrite("Login command is " + "'"+Login_cmd+"'")
    
    Command="tabcmd publish "+'"'+ "\Facebook\Facebook_campaigns.hyper"+'"' +" --overwrite --name "+'"'+ "Facebook_campaigns"+'"'+" --site  --project "+'"'+""+'"'
    LogFileWrite("Publish command is " +"'"+Command +"'")
    print(Command)
    
    try:
        LogFileWrite("Logging in to Tableau server")
        login=os.system(Login_cmd)
        if login ==0:
            LogFileWrite("Successfully logged into server")
        else:
            LogFileWrite("Unable to login to mentioned server using given credentials. Please check login command")
            LogFileWrite("---------------------------Script End-------------------------\n\n")
            SendEmailMessage()
            exit() 
    except OSError as e:
        LogFileWrite("Error encountered while logging in to Tableau server")
        print (e)
        LogFileWrite(str(e))
        LogFileWrite("---------------------------Script End-------------------------\n\n")
        SendEmailMessage()
        exit(1)
    try:
        LogFileWrite("Publishing file to server")
        Publish=os.system(Command)
        #LogFileWrite(str(Publish))
        if Publish ==0:
            LogFileWrite("Successfully published to server")
        else:
            LogFileWrite("Unable to publish the file. Please check Execution command")
            LogFileWrite("---------------------------Script End-------------------------\n\n")
            SendEmailMessage()
            exit()    
    except OSError as e:
        LogFileWrite("Error encountered while publishing file in to Tableau server")
        print (e)
        LogFileWrite(str(e))
        LogFileWrite("---------------------------Script End-------------------------\n\n")
        SendEmailMessage()
        exit(1)
    LogFileWrite("Sctipt executed successfully")
    SendEmailMessage()
    sys.exit()

#Incremental_refresh(FB_df)
#SendEmailMessage()