import smtplib 
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import math
import json
import time
import datetime as DT
import glob
import sys
import logging
import requests
import pandas as pd
import os
from os.path import basename
import numpy as np

def send_email(sender, receiver, subject, text, username, password, files=None):
    '''This function is designed to send an email via gmail
    =============PARAMETERS OF INTEREST=============
    sender: Email address from sender
    receiver: Target email address in a list.
    subject: Subject text
    text: Text message
    username: Email address sender
    password: Account password
    =============DEFAULTS=============
    None
    =============ASSUMPTIONS=============
    None 
    '''
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ", ".join(receiver)
    #msg['Cc'] = cc

    part1 = MIMEText(text, 'html')
    # Attach parts into message container
    msg.attach(part1)
    if files is not None:
        for f in files or []:
            with open(f, "rb") as fil:
                part = MIMEApplication(
                    fil.read(),
                    Name=basename(f)
                )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)
    # Sending the email
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.ehlo()
        server.starttls()
        server.login(username, password)
        server.sendmail(sender, receiver, msg.as_string())
        server.quit()
    except Exception as error:
        return {'Success': False,
                'Message': f'Error sending message - {error}'}
    return {'Success': True}

def email_log(sender, receiver, subject, text, username, password, logging, level='error', files=None, identifier = ''):
    '''
    This function writes an email and a log, both for error, successful or info.
    '''
    if level == 'error':
        logging.error(text)
        send_email(sender, receiver, f'{identifier} ERROR '+subject, text, username, password, files)
    elif level == 'success':
        logging.info(text)
        send_email(sender, receiver, f'{identifier} SUCCESS - '+subject, text, username, password, files)
    else:
        logging.info(text)
        send_email(sender, receiver, f'{identifier} INFO - '+subject, text, username, password, files)


def reltio_authenticate(token=None, use_token=True, username=None, password=None,):
    """This function generates an access token for Reltio DB.
    =============PARAMETERS OF INETERST=============
    session: Open session via requests lib. requests.Session()
    username: Reltio username
    password: Reltio password
    =============DEFAULTS=============
    None
    =============ASSUMPTIONS=============
    None
    """
    url = "https://auth.reltio.com/oauth/token"
    if use_token:
        querystring = {"grant_type": "client_credentials"}
        headers = {"Authorization": f"Basic {token}", "Content-Type": "application/x-www-form-urlencoded"}
    else:
        querystring = {"username": username, "password": password, "grant_type": "password"}
        headers = {"Authorization": "Basic cmVsdGlvX3VpOm1ha2l0YQ=="}
    try:
        response = requests.request("POST", url, headers=headers, params=querystring)
    except Exception as error:
        return {
            "Success": False,
            "Message": f"Error getting Reltio token: \
                    {error.__class__.__name__} - {error}",
        }
    response_json = response.json()
    try:
        access_token = response_json["access_token"]
    except Exception as error:
        return {
            "Success": False,
            "Message": f"Access token not found: \
                    {error.__class__.__name__} - {error}",
        }
    return {"Success": True, "Token": access_token}


def entities_post(token, r360_url, json_load, batch_size, logging=None, partial_overide=False):
    """This function pushes entities via api POST functionalities.
    =============PARAMETERS OF INETERST=============
    session: Open session via requests lib. requests.Session()
    username: Reltio username
    password: Reltio password
    r360_url: Reltio360 url for target tenant.
    json_load: json object containing the entities and its attributes to be pushed to Reltio.
    batch_size: Number of entities sent per post
    =============DEFAULTS=============
    None
    =============ASSUMPTIONS=============
    None
    """
    auth_resp = reltio_authenticate(token)
    if auth_resp["Success"]:
        reltio_token = auth_resp["Token"]
    else:
        return {
            "Success": False,
            "Message": f'Error authenticating - {auth_resp["Message"]}',
        }
    url = f"{r360_url}/entities?returnUriOnly=true"
    if partial_overide:
        url = url + "&options=partialOverride"
    headers = {
        "Authorization": "Bearer " + reltio_token,
        "Content-Type": "application/json",
    }
    number_batches = math.ceil(len(json_load) / batch_size)
    batch_number = 0
    failed_pushes = 0
    bad_pushes = []
    while batch_number < number_batches:
        if logging is not None:
            pct_completion = str(round((batch_number*100 / number_batches),2))
            print(f'{pct_completion}%')
        temp_json_load = json_load[batch_number * batch_size : (batch_number + 1) * batch_size]# : 
        temp_json_load_rdy = json.dumps(temp_json_load, default=lambda o: o.item() if isinstance(o, np.generic) else o)
        try:
            response = requests.request(
                "POST", url, headers=headers, data=temp_json_load_rdy, timeout=120
            )
        except:
            try:
                time.sleep(10)
                auth_resp = reltio_authenticate(token)
                reltio_token = auth_resp["Token"]
                headers = {
                    "Authorization": "Bearer " + reltio_token,
                    "Content-Type": "application/json",
                }
                response = requests.request(
                    "POST", url, headers=headers, data=temp_json_load_rdy, timeout=200
                )
            except Exception as error:
                return {
                    "Success": False,
                    "Message": f"Error post crash - pushing entities to Reltio360 {error}",
                }
        if not response.ok:
            try:
                print("Starting 10 sec")
                time.sleep(10)
                auth_resp = reltio_authenticate(token)
                reltio_token = auth_resp["Token"]
                headers = {
                    "Authorization": "Bearer " + reltio_token,
                    "Content-Type": "application/json",
                }
                response = requests.request(
                    "POST", url, headers=headers, data=temp_json_load_rdy, timeout=200
                )
            except Exception as error:
                return {
                    "Success": False,
                    "Message": f"Error post crash - pushing entities to Reltio360 {error}",
                }
        if not response.ok:
            return {
                "Success": False,
                "Message": f"Error pushing entities to Reltio360 {response} - {response.text}",
            }
        response_json = json.loads(response.text)
        for status in response_json:
            if not status["successful"]:
                try:
                    message = status['errors']['errorMessage']
                except:
                    message = 'No given reason'
                bad_pushes.append(
                    f"Bad push batch between {batch_number*batch_size}:{(batch_number+1)*batch_size} - Reason: {message} \n"
                )
                failed_pushes += 1
        batch_number += 1
        time.sleep(4)
    print('100% Complete')
    return {
        "Success": True,
        "Message": f"Successfully pushed {len(json_load)-failed_pushes} out of {len(json_load)} entities to Reltio360 - {response.status_code}. {bad_pushes}",
        "Failed_Pushes": failed_pushes
    }


def main(event, context):
    """
    Script designed to pull employee data from Okta and push it into Reltio360
    """
    # Read environment variables
    try:
        r360_url = os.environ["r360_url"]
        env = os.environ["env"]
        pms_url = os.environ["pms_url"]
        receiver = os.environ["TO_ADDRESS"].split(",")
        reltio_token = os.environ["RELTIO_TOKEN"]
        gmail_sender = os.environ["GMAIL_SENDER"]
        gmail_pass = os.environ["GMAIL_PASS"]
        gmail_user = os.environ["GMAIL_USER"]
        pms_auth = os.environ["pms_auth"]
        pms_username = os.environ["PMS_USERNAME"]
        pms_pass = os.environ["PMS_PASS"]
        sf_url = os.environ["sf_url"]
        sf_client_id = os.environ["sf_client_id"]
        sf_client_secret = os.environ["sf_client_secret"]
        sf_username = os.environ["sf_username"]
        sf_pass = os.environ["sf_pass"]
        sequence = os.environ["sequence"]
    except Exception as error:
        return {
            'statusCode': 400,
            'body': f"Error: Can't find os environment variables: {error}"
        }
    folder_path = ""
    project_name = env + ' SFDC To Reltio360'
        
    print("-------------------------------STARTING SFDC JOB-------------------------------")
    
    def add_pop(body_in, profile_x, body_x):
        if profile_x is None or profile_x == '':
            body_in["attributes"].pop(body_x)
        else:
            body_in["attributes"][body_x][0]["value"] = profile_x
        return body_in
    
    def add_pop_package(body_in, profile_x, body_x):
        if profile_x is None or profile_x == '':
            body_in["value"].pop(body_x)
        else:
            body_in["value"][body_x][0]["value"] = profile_x
        return body_in
    
    # Compose SOQL query for Salesforce data extraction
    line1="SELECT+Id,Name,SBQQ__Account__c,SBQQ__Account__r.Name,SBQQ__Account__r.Type,SBQQ__Account__r.Region__c,SBQQ__Account__r.Account_Owner_text__c,   SBQQ__Account__r.accountIntentScore6sense__c,SBQQ__Account__r.Engaged__c,SBQQ__Account__r.accountBuyingStage6sense__c,SBQQ__Account__r.accountProfileScore6sense__c,SBQQ__Account__r.Engaged_Contact_Count__c,SBQQ__Account__r.IsDeleted,SBQQ__Account__r.accountProfileFit6sense__c,SBQQ__Account__r.Tenant_ID__c,SBQQ__Account__r.Reltio_Target_Account__c,SBQQ__Account__r.Partner_Type__c,SBQQ__Account__r.Partner_Tier_Level__c,SBQQ__Account__r.Partner_Sub_Type__c,SBQQ__Account__r.Partner_Intel__c,SBQQ__Account__r.ParentId,SBQQ__Account__r.OwnerId,SBQQ__Account__r.CreatedDate,SBQQ__Account__r.Buying_Stage__c,SBQQ__Account__r.Allbound_ID__c,SBQQ__Account__r.CurrencyIsoCode,SBQQ__Account__r.Territory__c,SBQQ__Account__r.Paired_BDR__r.Name,SBQQ__Account__r.Site,SBQQ__Account__r.CSM__r.Name,SBQQ__Account__r.Renewal_Representative__r.Name,SBQQ__Account__r.NumberOfEmployees,SBQQ__Account__r.Market_Segmentation__c,SBQQ__Account__r.Industry,SBQQ__Account__r.Sub_Industry__c,SBQQ__Account__r.Target_Account__c,SBQQ__Account__r.Phone,SBQQ__Account__r.Website,SBQQ__Account__r.PO_Required__c,SBQQ__Account__r.SBQQ__RenewalPricingMethod__c,SBQQ__Account__r.SBQQ__RenewalModel__c,SBQQ__Account__r.Support_Level__c,SBQQ__Account__r.AnnualRevenue,SBQQ__Account__r.Original_Contract_Date__c,SBQQ__Account__r.Total_Subscription_ACV__c,SBQQ__Account__r.BillingCity,SBQQ__Account__r.BillingCountry,SBQQ__Account__r.BillingCountryCode,SBQQ__Account__r.BillingPostalCode,SBQQ__Account__r.BillingState,SBQQ__Account__r.BillingStateCode,SBQQ__Account__r.BillingStreet,SBQQ__Contract__c,SBQQ__Contract__r.ContractNumber,SBQQ__Contract__r.SBQQ__Opportunity__r.Name,SBQQ__Contract__r.SBQQ__Opportunity__r.Industry__c,SBQQ__Contract__r.SBQQ__Opportunity__r.Cloud_Platform_Tenant__c,SBQQ__Contract__r.SBQQ__Opportunity__r.Tenant_Deployment_Region2__c,SBQQ__Contract__r.SBQQ__Opportunity__r.Market_Segment__c,SBQQ__Contract__r.SBQQ__Opportunity__r.Procurement_Channel__c,SBQQ__Contract__r.SBQQ__Opportunity__r.Product_Family__c,SBQQ__Contract__r.RecordType.Name,"
    line2="SBQQ__Contract__r.Status,SBQQ__Contract__r.StartDate,SBQQ__Contract__r.EndDate,SBQQ__Contract__r.Termination_Date__c,SBQQ__Contract__r.CustomerSignedDate,SBQQ__Contract__r.BillingCity,SBQQ__Contract__r.BillingCountry,SBQQ__Contract__r.BillingCountryCode,SBQQ__Contract__r.BillingPostalCode,SBQQ__Contract__r.BillingState,SBQQ__Contract__r.BillingStateCode,SBQQ__Contract__r.HIPAA_Finance_Data__c,SBQQ__Contract__r.International_Business2__c,SBQQ__Contract__r.Renewal_ACV__c,SBQQ__Contract__r.X12_Month_Renewal_Price_w_o_Uplift__c,SBQQ__Contract__r.Auto_Renewal__c,SBQQ__Contract__r.CP_Overage_Price__c,SBQQ__Contract__r.CP_Overage_Qty__c,SBQQ__Contract__r.PO_Required__c,SBQQ__Contract__r.ActivatedDate,SBQQ__Contract__r.Pooled_APIs__c,SBQQ__Contract__r.Pooled_Profiles__c,SBQQ__Contract__r.Pooled_RIH__c,SBQQ__Contract__r.Pooled_RSUs__c,SBQQ__QuoteLine__r.SBQQ__RenewedSubscription__c,Active__c,SBQQ__Quantity__c,Product_Quantity__c,SBQQ__ProductName__c,Product_Code__c,SBQQ__SubscriptionStartDate__c,SBQQ__SubscriptionEndDate__c,SBQQ__ProductSubscriptionType__c,SBQQ__Bundled__c,Quote__c,Primary_Product__c,Domain_Name2__c,SBQQ__RootId__c,Root_Id_FX__c,OwnerId,SBQQ__ListPrice__c,SBQQ__NetPrice__c,SBQQ__CustomerPrice__c,SBQQ__BundledQuantity__c,SBQQ__SubscriptionPricing__c,SBQQ__ProrateMultiplier__c, Consolidated_Profiles__c, Reltio_Storage_Unit_RSU__c, RSU_Quantity__c, Usage_Data_API__c, RIH_Tasks__c, Package_Mapping__c, Data_Domain__c, Other_Data_Domain__c, SBQQ__RevisedSubscription__c, Total_RSU_Quantity__c, Agentflow_Credits__c, Agentflow_Credits_Managed__c FROM SBQQ__Subscription__c+WHERE+Active__c=TRUE+AND+SBQQ__Account__r.Type in ('Customer','Partner') +AND+SBQQ__Contract__r.RecordType.Name='Order Form'"
    soql=line1+line2

    url=f'{sf_url}/services/oauth2/token?format=json'
    body = {'grant_type': 'password', 'client_id': sf_client_id,
            'client_secret': sf_client_secret, 'username': sf_username,
            'password': sf_pass}
    try:
        x = requests.post(url, body)
    except Exception as error:
        email_log(gmail_sender, receiver, project_name, f"Error: Can't get SF token: {error}", gmail_user, gmail_pass, logging)
        print(f"Error: Can't get SF token: {error}")
        return {
            'statusCode': 400,
            'body': f"Error: Can't get SF token: {error}"
        }

    if x.ok:
        x=x.json()
        sf_token = x['access_token']
    else:
        email_log(gmail_sender, receiver, project_name, "Error: SF Credentials", gmail_user, gmail_pass, logging)
        print("Error: SF Credentials")
        return {
            'statusCode': 400,
            'body': "Error: SF Credentials"
        }
        
    url = f"{sf_url}/services/data/v58.0/query/?q={soql}"
    done = False
    i=0
    y={}
    while not done:
        if i!=0:
            url = sf_url + y['nextRecordsUrl']
        headers = {'Authorization': f'Bearer {sf_token}', 'Content-Type': 'application/json'}
        try:
            y = requests.get(url, headers=headers)
        except Exception as error:
            email_log(gmail_sender, receiver, project_name, f"Error: Can't pull data from SF: {error}", gmail_user, gmail_pass, logging)
            print(f"Error: Can't pull data from SF: {error}")
            return {
            'statusCode': 400,
            'body': f"Error: Can't pull data from SF: {error}"
        }
        y = y.json()
        temp_df = y['records']
        if i==0:
            full_df = pd.json_normalize(temp_df)
        else:
            full_df = pd.concat([full_df, pd.json_normalize(temp_df)])
        done = y['done']
        i+=1
    
    
    full_payload = []
    i = 0
    for account in full_df['SBQQ__Account__c'].drop_duplicates():
        temp_df = full_df[full_df['SBQQ__Account__c']==account]
        with open(f"{folder_path}account.json", encoding="utf-8") as file:
            payload = json.load(file).copy()
        body = payload[0]
        i += 1
        body["crosswalks"][0]["value"] = account
        body["crosswalks"][1]["value"] = account
        body = add_pop(body, temp_df['SBQQ__Account__r.Name'].iloc[0], 'Name')
        body = add_pop(body, temp_df['SBQQ__Account__r.Type'].iloc[0], 'OrganizationType')
        body = add_pop(body, temp_df['SBQQ__Account__r.Region__c'].iloc[0], 'AccountRegion')
        body = add_pop(body, temp_df['SBQQ__Account__r.Territory__c'].iloc[0], 'AccountTerritory')
        body = add_pop(body, temp_df['SBQQ__Account__r.Website'].iloc[0], 'WebsiteURL')
        body = add_pop(body, temp_df['SBQQ__Account__r.Original_Contract_Date__c'].iloc[0], 'OriginalContractDate')
        body = add_pop(body, temp_df['SBQQ__Account__r.Industry'].iloc[0], 'Industry')
        body = add_pop(body, temp_df['SBQQ__Account__r.Sub_Industry__c'].iloc[0], 'subIndustry')

        body = add_pop(body, temp_df['SBQQ__Account__r.Site'].iloc[0], 'Site')
        body = add_pop(body, temp_df['SBQQ__Account__r.Market_Segmentation__c'].iloc[0], 'MarketSegmentation')
        body = add_pop(body, temp_df['SBQQ__Account__r.Target_Account__c'].iloc[0], 'TargetAccount')
        body = add_pop(body, temp_df['SBQQ__Account__r.PO_Required__c'].iloc[0], 'PoRequired')
        body = add_pop(body, temp_df['SBQQ__Account__r.SBQQ__RenewalPricingMethod__c'].iloc[0], 'RenewalPricingMethod')
        body = add_pop(body, temp_df['SBQQ__Account__r.SBQQ__RenewalModel__c'].iloc[0], 'RenewalModel')
        body = add_pop(body, temp_df['SBQQ__Account__r.Support_Level__c'].iloc[0], 'SupportLevel')
        body = add_pop(body, float(temp_df['SBQQ__Account__r.accountIntentScore6sense__c'].iloc[0]), 'accountIntentScore6sense')
        body = add_pop(body, bool(temp_df['SBQQ__Account__r.Engaged__c'].iloc[0]), 'Engaged')
        body = add_pop(body, temp_df['SBQQ__Account__r.accountBuyingStage6sense__c'].iloc[0], 'accountBuyingStage6sense')
        body = add_pop(body, float(temp_df['SBQQ__Account__r.accountProfileScore6sense__c'].iloc[0]), 'accountProfileScore6sense')
        body = add_pop(body, int(temp_df['SBQQ__Account__r.Engaged_Contact_Count__c'].iloc[0]), 'EngagedContactCount')
        body = add_pop(body, bool(temp_df['SBQQ__Account__r.IsDeleted'].iloc[0]), 'IsDeleted')
        body = add_pop(body, temp_df['SBQQ__Account__r.accountProfileFit6sense__c'].iloc[0], 'accountProfileFit6sense')
        body = add_pop(body, temp_df['SBQQ__Account__r.Tenant_ID__c'].iloc[0], 'TenantID')
        body = add_pop(body, bool(temp_df['SBQQ__Account__r.Reltio_Target_Account__c'].iloc[0]), 'ReltioTargetAccount')
        body = add_pop(body, temp_df['SBQQ__Account__r.Partner_Type__c'].iloc[0], 'PartnerType')
        body = add_pop(body, temp_df['SBQQ__Account__r.Partner_Tier_Level__c'].iloc[0], 'PartnerTierLevel')
        body = add_pop(body, temp_df['SBQQ__Account__r.Partner_Sub_Type__c'].iloc[0], 'PartnerSubType')
        body = add_pop(body, bool(temp_df['SBQQ__Account__r.Partner_Intel__c'].iloc[0]), 'PartnerIntel')
        body = add_pop(body, temp_df['SBQQ__Account__r.ParentId'].iloc[0], 'ParentId')
        body = add_pop(body, temp_df['SBQQ__Account__r.OwnerId'].iloc[0], 'OwnerId')
        body = add_pop(body, temp_df['SBQQ__Account__r.CreatedDate'].iloc[0], 'CreatedDate')
        body = add_pop(body, temp_df['SBQQ__Account__r.Buying_Stage__c'].iloc[0], 'BuyingStage')
        body = add_pop(body, temp_df['SBQQ__Account__r.Allbound_ID__c'].iloc[0], 'AllboundID')
        body = add_pop(body, temp_df['SBQQ__Account__r.CurrencyIsoCode'].iloc[0], 'CurrencyIsoCode')


        
        body["attributes"]["Total_Subscription_ACV__gc"][0]["value"]['Total_Subscription_ACV__gc'][0]["value"] = str(temp_df['SBQQ__Account__r.Total_Subscription_ACV__c'].iloc[0])
        body["attributes"]["Total_Subscription_ACV__gc"][0]["value"]['SalesRevenueCurrency'][0]["value"] = temp_df['SBQQ__Account__r.CurrencyIsoCode'].iloc[0]
        body["attributes"]["Phone"][0]["value"]["Number"][0]["value"] = temp_df['SBQQ__Account__r.Phone'].iloc[0]
        if pd.isna(temp_df['SBQQ__Account__r.NumberOfEmployees'].iloc[0]):
            body["attributes"].pop("EmployeeDetails")
        else:
            body["attributes"]["EmployeeDetails"][0]["value"]["NumberOfEmployees"][0]["value"] = int(temp_df['SBQQ__Account__r.NumberOfEmployees'].iloc[0])
        body["attributes"]["SalesForceID"][0]["value"] = account
        body["attributes"]["KeyFinancialFiguresOverview"][0]["value"]["SalesRevenueAmount"][0]["value"] = str(temp_df['SBQQ__Account__r.AnnualRevenue'].iloc[0])
        body["attributes"]["KeyFinancialFiguresOverview"][0]["value"]["SalesRevenueCurrencyCode"][0]["value"] = temp_df['SBQQ__Account__r.CurrencyIsoCode'].iloc[0]
        body["attributes"]["Address"][0]["value"]["AddressLine1"][0]["value"] = temp_df['SBQQ__Account__r.BillingStreet'].iloc[0]
        body["attributes"]["Address"][0]["value"]["Country"][0]["value"] = temp_df['SBQQ__Account__r.BillingCountry'].iloc[0]
        body["attributes"]["Address"][0]["value"]["StateProvince"][0]["value"] = temp_df['SBQQ__Account__r.BillingState'].iloc[0]
        body["attributes"]["Address"][0]["value"]["City"][0]["value"] = temp_df['SBQQ__Account__r.BillingCity'].iloc[0]
        if temp_df['SBQQ__Account__r.BillingPostalCode'].iloc[0] is not None:
            body["attributes"]["Address"][0]["value"]["Zip"][0]["value"]['Zip5'][0]['value'] = temp_df['SBQQ__Account__r.BillingPostalCode'].iloc[0].split('-')[0]
            body["attributes"]["Address"][0]["value"]["Zip"][0]["value"]['Zip4'][0]['value'] = temp_df['SBQQ__Account__r.BillingPostalCode'].iloc[0].split('-')[-1]
        else:
            body["attributes"]["Address"][0]["value"].pop("Zip")
        body["attributes"]["Address"][0]["value"]["ISO3166-2"][0]["value"] = temp_df['SBQQ__Account__r.BillingCountryCode'].iloc[0]
        body["attributes"]["Address"][0]["refRelation"]["crosswalks"][0]["value"] = account
        contract_list = []
        for contract in temp_df['SBQQ__Contract__c']:
            with open(f"{folder_path}contract_account.json", encoding="utf-8") as file:
                contract_account = json.load(file).copy()
            temp_contract_account = contract_account[0]
            temp_contract_df = temp_df[temp_df['SBQQ__Contract__c']==contract]
            temp_contract_account["refEntity"]["crosswalks"][0]["value"] = contract
            temp_contract_account["refEntity"]["crosswalks"][1]["value"] = contract
            temp_contract_account['value']['ContractName'][0]['value'] = temp_contract_df['SBQQ__Contract__r.SBQQ__Opportunity__r.Name'].iloc[0]
            temp_contract_account["refRelation"]["crosswalks"][0]["value"] = account + '_' + contract
            contract_list.append(temp_contract_account)
        body['attributes']['Contract'] = contract_list
        full_payload.append(body)
        
    try:
        post_result = entities_post(
            token=reltio_token,
            r360_url=r360_url,
            json_load=full_payload,
            batch_size=30,
            partial_overide=False,
            logging=logging
        )
    except Exception as error:
        email_log(gmail_sender, receiver, project_name, f"Error posting the entities to Reltio Accounts. Code crash. - {error}", gmail_user, gmail_pass, logging)
        print(f"Error posting the entities to Reltio Accounts. Code crash. - {error}")
        return {
            'statusCode': 400,
            'body': f"Error posting the entities to Reltio Accounts. Code crash. - {error}"
        }
    else:
        post_reltio_res_acc = post_result["Message"]
        if post_result["Success"]:
            print(post_reltio_res_acc)
        else:
            email_log(gmail_sender, receiver, 'ERROR - '+ project_name, post_reltio_res_acc, gmail_user, gmail_pass, logging)
            return {
            'statusCode': 400,
            'body': post_reltio_res_acc
        }

    # Select all rows where SBQQ__RootId__c is missing (NaN) -- these are subscriptions without a root ID. Keep only rows where SBQQ__RootId__c is present (not NaN)
    empty_rid = full_df[full_df['SBQQ__RootId__c'].isna()]
    full_df = full_df[full_df['SBQQ__RootId__c'].notna()]
    full_df = full_df.reset_index().drop(columns=['index'])

    # Add a row number (RN) within each group of SBQQ__RootId__c, sorted ascending
    full_df['RN'] = (
        full_df.sort_values(['SBQQ__RootId__c'], ascending=True)
        .groupby(['SBQQ__RootId__c'])
        .cumcount() + 1
    )

    # Mark product names for missing root IDs as "AMENDED". Assign a fallback root ID from Root_Id_FX__c for those missing SBQQ__RootId__c
    empty_rid['SBQQ__ProductName__c'] = empty_rid['SBQQ__ProductName__c'].astype(str) + ' - AMENDED'
    empty_rid['SBQQ__RootId__c'] = empty_rid['Root_Id_FX__c'].astype(str)

    # Concatenate the amended rows back into the main DataFrame
    full_df = pd.concat([full_df, empty_rid])

    # Forward-fill and backward-fill Package_Mapping__c within each SBQQ__RootId__c group
    full_df["Package_Mapping__c"] = (
        full_df.groupby("SBQQ__RootId__c")["Package_Mapping__c"]
        .transform(lambda x: x.ffill().bfill())
    )

    full_df = full_df.reset_index().drop(columns=['index'])

    # Prepare to handle revised subscriptions (Chrysalis):
    # 1. Get unique pairs of revised subscription ID and quantity
    revised_subs = full_df[['SBQQ__RevisedSubscription__c', 'SBQQ__Quantity__c']].drop_duplicates()

    # 2. Keep only rows where the revised subscription ID is present
    revised_subs = revised_subs[revised_subs['SBQQ__RevisedSubscription__c'].notna()]

    # 3. Rename the revised subscription column to 'Id'
    revised_subs = revised_subs.rename(columns={'SBQQ__RevisedSubscription__c': 'Id'})

    # 4. Add all original subscriptions (Id, Quantity) to the revised_subs DataFrame
    revised_subs = pd.concat([revised_subs, full_df[['Id', 'SBQQ__Quantity__c']]], ignore_index=True)

    # 5. Sum the quantities for each subscription ID
    revised_subs = revised_subs.groupby('Id', as_index=False)['SBQQ__Quantity__c'].sum()

    # 6. Identify revised subscriptions that are still active (quantity != 0)
    revised_subs_in = revised_subs[revised_subs['SBQQ__Quantity__c'] != 0]

    # 7. Identify revised subscriptions that should be excluded (quantity == 0)
    revised_subs_out = revised_subs[revised_subs['SBQQ__Quantity__c'] == 0]

    # 8. Remove rows from full_df where the revised subscription is in the "out" list
    full_df = full_df[~full_df['SBQQ__RevisedSubscription__c'].isin(revised_subs_out['Id'].unique())]

    # 9. Keep only rows in full_df whose Id is in the "in" list (active revised subscriptions)
    full_df = full_df[full_df['Id'].isin(revised_subs_in['Id'].unique())]
    
    full_df = full_df.fillna('')
    full_payload = []
    i = 0
    for contract in full_df['SBQQ__Contract__c'].drop_duplicates():
        temp_df = full_df[full_df['SBQQ__Contract__c']==contract]
        with open(f"{folder_path}contract.json", encoding="utf-8") as file:
            payload = json.load(file).copy()
        body = payload[0]
        i += 1
        body["crosswalks"][0]["value"] = contract
        body["attributes"]["ContractID18Char"][0]["value"] = contract
        body["attributes"]["sfContractLink"][0]["value"] = f'https://reltio.lightning.force.com/lightning/r/Contract/{contract}/view'
        body = add_pop(body, temp_df['SBQQ__Contract__r.ContractNumber'].iloc[0], 'ContractNumber')
        body = add_pop(body, temp_df['SBQQ__Contract__r.SBQQ__Opportunity__r.Name'].iloc[0], 'ContractName')
        body = add_pop(body, temp_df['SBQQ__Contract__r.RecordType.Name'].iloc[0], 'ContractType')
        body = add_pop(body, temp_df['SBQQ__Contract__r.Status'].iloc[0], 'ContractStatus')
        body = add_pop(body, str(temp_df['SBQQ__Contract__r.Renewal_ACV__c'].iloc[0]), 'renewalAcv')
        body = add_pop(body, temp_df['SBQQ__Contract__r.SBQQ__Opportunity__r.Procurement_Channel__c'].iloc[0], 'ProcurementChannel')
        body = add_pop(body, temp_df['SBQQ__Contract__r.ActivatedDate'].iloc[0], 'ActivatedDate')
        body = add_pop(body, temp_df['SBQQ__Contract__r.StartDate'].iloc[0], 'ContractStartDate')
        body = add_pop(body, temp_df['SBQQ__Contract__r.EndDate'].iloc[0], 'ContractEndDate')
        body = add_pop(body, temp_df['SBQQ__Contract__r.HIPAA_Finance_Data__c'].iloc[0], 'HIPAAFinance')
        body = add_pop(body, temp_df['SBQQ__Contract__r.International_Business2__c'].iloc[0], 'InternationalBusiness')
        body = add_pop(body, temp_df['SBQQ__Contract__r.SBQQ__Opportunity__r.Industry__c'].iloc[0], 'Industry')
        body = add_pop(body, temp_df['SBQQ__Contract__r.SBQQ__Opportunity__r.Market_Segment__c'].iloc[0], 'MarketSegment')
        body = add_pop(body, temp_df['SBQQ__Contract__r.SBQQ__Opportunity__r.Cloud_Platform_Tenant__c'].iloc[0], 'CloudProvider')
        body = add_pop(body, temp_df['SBQQ__Contract__r.SBQQ__Opportunity__r.Tenant_Deployment_Region2__c'].iloc[0], 'TenantDeploymentRegion')
        body = add_pop(body, temp_df['SBQQ__Contract__r.SBQQ__Opportunity__r.Product_Family__c'].iloc[0], 'productFamily')
        body = add_pop(body, temp_df['SBQQ__Contract__r.Pooled_APIs__c'].iloc[0], 'pooledApi')
        body = add_pop(body, temp_df['SBQQ__Contract__r.Pooled_Profiles__c'].iloc[0], 'pooledCP')
        body = add_pop(body, temp_df['SBQQ__Contract__r.Pooled_RIH__c'].iloc[0], 'pooledRih')
        body = add_pop(body, temp_df['SBQQ__Contract__r.Pooled_RSUs__c'].iloc[0], 'pooledRsu')
        contract_detail_list = []
        base_package_list_full_temp = []
        mult_rootid_list = list(temp_df[pd.to_numeric(temp_df['RN'], errors='coerce') >= 2]['SBQQ__RootId__c'].drop_duplicates())
        for root_id in temp_df['SBQQ__RootId__c'].drop_duplicates():
            if root_id not in mult_rootid_list:
                with open(f"{folder_path}contract_detail_list.json", encoding="utf-8") as file:
                    contract_detail = json.load(file).copy()
                temp_contract_detail = contract_detail[0]
                temp_contract_detail_df = temp_df[temp_df['SBQQ__RootId__c']==root_id]
                temp_contract_detail["value"]["Product_Name"][0]["value"] = temp_contract_detail_df['SBQQ__ProductName__c'].iloc[0]
                temp_contract_detail["value"]["Product_Quantity"][0]["value"] = int(temp_contract_detail_df['SBQQ__Quantity__c'].iloc[0])
                temp_contract_detail["value"]["RootID"][0]["value"] = temp_contract_detail_df['SBQQ__RootId__c'].iloc[0]
                temp_contract_detail["value"]["Start_Date"][0]["value"] = temp_contract_detail_df['SBQQ__SubscriptionStartDate__c'].iloc[0]
                temp_contract_detail["value"]["End_Date"][0]["value"] = temp_contract_detail_df['SBQQ__SubscriptionEndDate__c'].iloc[0]
                contract_detail_list.append(temp_contract_detail)
            else:
                base_package_list = []
                temp_contract_detail_df = temp_df[temp_df['SBQQ__RootId__c']==root_id]
                temp_contract_detail_df = temp_contract_detail_df.reset_index()
                all_usage_row = temp_contract_detail_df[temp_contract_detail_df['Id']==root_id]
                # try:
                #     max_iloc = int(temp_contract_detail_df['Usage_Data_API__c'].idxmax())
                # except:
                #     max_iloc = 0
                if not all_usage_row.empty:
                    cp_usage_rows = pd.DataFrame({'SBQQ__ProductName__c': 'ROR CP Entitlement', 'SBQQ__Quantity__c': str(int(float(all_usage_row['Consolidated_Profiles__c'].iloc[0] or 0))), 'SBQQ__RootId__c': all_usage_row['SBQQ__RootId__c'].iloc[0], 'Package_Mapping__c': all_usage_row['Package_Mapping__c'].iloc[0], 'Domain_Name2__c': all_usage_row['Domain_Name2__c'].iloc[0], 'Product_Code__c': all_usage_row['Product_Code__c'].iloc[0], 'SBQQ__SubscriptionStartDate__c': all_usage_row['SBQQ__SubscriptionStartDate__c'].iloc[0], 'SBQQ__SubscriptionEndDate__c': all_usage_row['SBQQ__SubscriptionEndDate__c'].iloc[0]}, index=[0])
                    rsu_usage_rows = pd.DataFrame({'SBQQ__ProductName__c': 'ROR RSU Storage Entitlement', 'SBQQ__Quantity__c': str(int(float(all_usage_row['Reltio_Storage_Unit_RSU__c'].iloc[0] or 0))), 'SBQQ__RootId__c': all_usage_row['SBQQ__RootId__c'].iloc[0], 'Package_Mapping__c': all_usage_row['Package_Mapping__c'].iloc[0], 'Domain_Name2__c': all_usage_row['Domain_Name2__c'].iloc[0], 'Product_Code__c': all_usage_row['Product_Code__c'].iloc[0], 'SBQQ__SubscriptionStartDate__c': all_usage_row['SBQQ__SubscriptionStartDate__c'].iloc[0], 'SBQQ__SubscriptionEndDate__c': all_usage_row['SBQQ__SubscriptionEndDate__c'].iloc[0]}, index=[0])
                    total_rsu_usage_rows = pd.DataFrame({'SBQQ__ProductName__c': 'ROR Total RSU Entitlement', 'SBQQ__Quantity__c': str(int(float(all_usage_row['Total_RSU_Quantity__c'].iloc[0] or 0))), 'SBQQ__RootId__c': all_usage_row['SBQQ__RootId__c'].iloc[0], 'Package_Mapping__c': all_usage_row['Package_Mapping__c'].iloc[0], 'Domain_Name2__c': all_usage_row['Domain_Name2__c'].iloc[0], 'Product_Code__c': all_usage_row['Product_Code__c'].iloc[0], 'SBQQ__SubscriptionStartDate__c': all_usage_row['SBQQ__SubscriptionStartDate__c'].iloc[0], 'SBQQ__SubscriptionEndDate__c': all_usage_row['SBQQ__SubscriptionEndDate__c'].iloc[0]}, index=[0])
                    agentflow_credit_rows = pd.DataFrame({'SBQQ__ProductName__c': 'ROR Agentflow Credits Entitlement', 'SBQQ__Quantity__c': str(int(float(all_usage_row['Agentflow_Credits__c'].iloc[0] or 0))), 'SBQQ__RootId__c': all_usage_row['SBQQ__RootId__c'].iloc[0], 'Package_Mapping__c': all_usage_row['Package_Mapping__c'].iloc[0], 'Domain_Name2__c': all_usage_row['Domain_Name2__c'].iloc[0], 'Product_Code__c': all_usage_row['Product_Code__c'].iloc[0], 'SBQQ__SubscriptionStartDate__c': all_usage_row['SBQQ__SubscriptionStartDate__c'].iloc[0], 'SBQQ__SubscriptionEndDate__c': all_usage_row['SBQQ__SubscriptionEndDate__c'].iloc[0]}, index=[0])
                    agentflow_mngd_credit_rows = pd.DataFrame({'SBQQ__ProductName__c': 'ROR Agentflow Credits Managed Entitlement', 'SBQQ__Quantity__c': str(int(float(all_usage_row['Agentflow_Credits_Managed__c'].iloc[0] or 0))), 'SBQQ__RootId__c': all_usage_row['SBQQ__RootId__c'].iloc[0], 'Package_Mapping__c': all_usage_row['Package_Mapping__c'].iloc[0], 'Domain_Name2__c': all_usage_row['Domain_Name2__c'].iloc[0], 'Product_Code__c': all_usage_row['Product_Code__c'].iloc[0], 'SBQQ__SubscriptionStartDate__c': all_usage_row['SBQQ__SubscriptionStartDate__c'].iloc[0], 'SBQQ__SubscriptionEndDate__c': all_usage_row['SBQQ__SubscriptionEndDate__c'].iloc[0]}, index=[0])                    
                    rsu_unit_usage_rows = pd.DataFrame({'SBQQ__ProductName__c': 'ROR RSU Quantity', 'SBQQ__Quantity__c': str(int(float(all_usage_row['RSU_Quantity__c'].iloc[0] or 0))), 'SBQQ__RootId__c': all_usage_row['SBQQ__RootId__c'].iloc[0], 'Package_Mapping__c': all_usage_row['Package_Mapping__c'].iloc[0], 'Domain_Name2__c': all_usage_row['Domain_Name2__c'].iloc[0], 'Product_Code__c': all_usage_row['Product_Code__c'].iloc[0], 'SBQQ__SubscriptionStartDate__c': all_usage_row['SBQQ__SubscriptionStartDate__c'].iloc[0], 'SBQQ__SubscriptionEndDate__c': all_usage_row['SBQQ__SubscriptionEndDate__c'].iloc[0]}, index=[0])
                    api_usage_rows = pd.DataFrame({'SBQQ__ProductName__c': 'ROR API Usage Entitlement', 'SBQQ__Quantity__c': str(int(float(all_usage_row['Usage_Data_API__c'].iloc[0] or 0))), 'SBQQ__RootId__c': all_usage_row['SBQQ__RootId__c'].iloc[0], 'Package_Mapping__c': all_usage_row['Package_Mapping__c'].iloc[0], 'Domain_Name2__c': all_usage_row['Domain_Name2__c'].iloc[0], 'Product_Code__c': all_usage_row['Product_Code__c'].iloc[0], 'SBQQ__SubscriptionStartDate__c': all_usage_row['SBQQ__SubscriptionStartDate__c'].iloc[0], 'SBQQ__SubscriptionEndDate__c': all_usage_row['SBQQ__SubscriptionEndDate__c'].iloc[0]}, index=[0])
                    rih_usage_rows = pd.DataFrame({'SBQQ__ProductName__c': 'ROR RIH Tasks Entitlement', 'SBQQ__Quantity__c': str(int(float(all_usage_row['RIH_Tasks__c'].iloc[0] or 0))), 'SBQQ__RootId__c': all_usage_row['SBQQ__RootId__c'].iloc[0], 'Package_Mapping__c': all_usage_row['Package_Mapping__c'].iloc[0], 'Domain_Name2__c': all_usage_row['Domain_Name2__c'].iloc[0], 'Product_Code__c': all_usage_row['Product_Code__c'].iloc[0], 'SBQQ__SubscriptionStartDate__c': all_usage_row['SBQQ__SubscriptionStartDate__c'].iloc[0], 'SBQQ__SubscriptionEndDate__c': all_usage_row['SBQQ__SubscriptionEndDate__c'].iloc[0]}, index=[0])
                    root_id_rows = pd.DataFrame({'SBQQ__ProductName__c': 'Package Link', 'SBQQ__Quantity__c': 0.0, 'SBQQ__RootId__c': root_id, 'sf_package_link': f'https://reltio.lightning.force.com/lightning/r/SBQQ__Subscription__c/{root_id}/view', 'Package_Mapping__c': all_usage_row['Package_Mapping__c'].iloc[0], 'Domain_Name2__c': all_usage_row['Domain_Name2__c'].iloc[0], 'Product_Code__c': all_usage_row['Product_Code__c'].iloc[0], 'SBQQ__SubscriptionStartDate__c': all_usage_row['SBQQ__SubscriptionStartDate__c'].iloc[0], 'SBQQ__SubscriptionEndDate__c': all_usage_row['SBQQ__SubscriptionEndDate__c'].iloc[0]}, index=[0])
                    data_domain_rows = pd.DataFrame({'SBQQ__ProductName__c': 'ROR Data Domain', 'SBQQ__Quantity__c': 0.0, 'SBQQ__RootId__c': root_id, 'Package_Mapping__c': all_usage_row['Package_Mapping__c'].iloc[0], 'Domain_Name2__c': all_usage_row['Domain_Name2__c'].iloc[0], 'SBQQ__SubscriptionStartDate__c': all_usage_row['SBQQ__SubscriptionStartDate__c'].iloc[0], 'SBQQ__SubscriptionEndDate__c': all_usage_row['SBQQ__SubscriptionEndDate__c'].iloc[0], 'Data_Domain__c': all_usage_row['Data_Domain__c'], 'Product_Code__c': all_usage_row['Product_Code__c'].iloc[0], 'Other_Data_Domain__c': all_usage_row['Other_Data_Domain__c']}, index=[0])
                    temp_contract_detail_df = pd.concat([temp_contract_detail_df, cp_usage_rows], ignore_index=True)
                    temp_contract_detail_df = pd.concat([temp_contract_detail_df, rsu_usage_rows], ignore_index=True)
                    temp_contract_detail_df = pd.concat([temp_contract_detail_df, rsu_unit_usage_rows], ignore_index=True)
                    temp_contract_detail_df = pd.concat([temp_contract_detail_df, total_rsu_usage_rows], ignore_index=True)
                    temp_contract_detail_df = pd.concat([temp_contract_detail_df, agentflow_credit_rows], ignore_index=True)
                    temp_contract_detail_df = pd.concat([temp_contract_detail_df, agentflow_mngd_credit_rows], ignore_index=True)                    
                    temp_contract_detail_df = pd.concat([temp_contract_detail_df, api_usage_rows], ignore_index=True)
                    temp_contract_detail_df = pd.concat([temp_contract_detail_df, rih_usage_rows], ignore_index=True)
                    temp_contract_detail_df = pd.concat([temp_contract_detail_df, root_id_rows], ignore_index=True)
                    temp_contract_detail_df = pd.concat([temp_contract_detail_df, data_domain_rows], ignore_index=True)
                else:
                    temp_contract_detail_df['sf_package_link'] = ''
                temp_contract_detail_df = temp_contract_detail_df.fillna('')
                for i in range(len(temp_contract_detail_df)):
                    with open(f"{folder_path}base_package_list.json", encoding="utf-8") as file:
                        temp_contract_detail = json.load(file).copy()
                    temp_contract_detail = add_pop_package(temp_contract_detail, temp_contract_detail_df['SBQQ__ProductName__c'].iloc[i], 'Product_Name')
                    temp_contract_detail = add_pop_package(temp_contract_detail, str(int(float(temp_contract_detail_df['SBQQ__Quantity__c'].iloc[i] or 0))), 'Product_Quantity')
                    temp_contract_detail = add_pop_package(temp_contract_detail, temp_contract_detail_df['SBQQ__RootId__c'].iloc[i], 'RootID')
                    temp_contract_detail = add_pop_package(temp_contract_detail, temp_contract_detail_df['SBQQ__SubscriptionStartDate__c'].iloc[i], 'Start_Date')
                    temp_contract_detail = add_pop_package(temp_contract_detail, temp_contract_detail_df['SBQQ__SubscriptionEndDate__c'].iloc[i], 'End_Date')
                    temp_contract_detail = add_pop_package(temp_contract_detail, temp_contract_detail_df['Domain_Name2__c'].iloc[i], 'Domain_Name')
                    temp_contract_detail = add_pop_package(temp_contract_detail, temp_contract_detail_df['sf_package_link'].iloc[i], 'sf_package_link')
                    temp_contract_detail = add_pop_package(temp_contract_detail, temp_contract_detail_df['Package_Mapping__c'].iloc[i], 'packageMapping')
                    temp_contract_detail = add_pop_package(temp_contract_detail, temp_contract_detail_df['Data_Domain__c'].iloc[i], 'dataDomain')
                    temp_contract_detail = add_pop_package(temp_contract_detail, temp_contract_detail_df['Product_Code__c'].iloc[i], 'productCode')
                    temp_contract_detail = add_pop_package(temp_contract_detail, temp_contract_detail_df['Other_Data_Domain__c'].iloc[i], 'dataDomainOtherDetail')
                    base_package_list.append(temp_contract_detail)
                with open(f"{folder_path}base_package_list_full.json", encoding="utf-8") as file:
                    base_package_list_full = json.load(file).copy()
                base_package_list_full[0]["value"]["Subscription_details"] = base_package_list
                base_package_list_full = ','.join(str(v) for v in base_package_list_full).replace('\'', '\"').replace('None', '""')
                base_package_list_full_temp.append(json.loads(base_package_list_full))
        if len(contract_detail_list)!=0:
            body['attributes']['Contract_details'] = contract_detail_list
        else:
            body['attributes'].pop('Contract_details')
        if len(base_package_list_full_temp)!=0:
            body['attributes']['Base_Package_Details'] = base_package_list_full_temp
        else:
            body['attributes'].pop('Base_Package_Details')
        full_payload.append(body)

    try:
        post_result = entities_post(
            token=reltio_token,
            r360_url=r360_url,
            json_load=full_payload,
            batch_size=30,
            partial_overide=False,
            logging=logging
        )
    except Exception as error:
        email_log(gmail_sender, receiver, project_name, f"Error posting the entities to Reltio contracts. Code crash. - {error}", gmail_user, gmail_pass, logging)
        print(f"Error posting the entities to Reltio contracts. Code crash. - {error}")
        return {
            'statusCode': 400,
            'body': f"Error posting the entities to Reltio contracts. Code crash. - {error}"
        }
    else:
        post_reltio_res_con = post_result["Message"]
        if post_result["Success"]:
            print(post_reltio_res_con)
        else:
            email_log(gmail_sender, receiver, 'ERROR - '+ project_name, post_reltio_res_con, gmail_user, gmail_pass, logging)
            return {
            'statusCode': 400,
            'body': post_reltio_res_con
        }
        
        
    url = f'https://auth.reltio.com/oauth/token?username={pms_username}&password={pms_pass}&grant_type=password'
    headers = {'Authorization': pms_auth, 'Content-Type': 'application/x-www-form-urlencoded'}
    try:
        x = requests.get(url, headers=headers)
    except Exception as error:
        email_log(gmail_sender, receiver,  project_name, f"Error: Can't get PMS token: {error}", gmail_user, gmail_pass, logging)
        print(f"Error: Can't get PMS token: {error}")
        return {
            'statusCode': 400,
            'body': f"Error: Can't get PMS token: {error}"
        }
    if x.ok:
        x = x.json()
    else:
        print(f'Error getting pms token: {x.status_code}')
        email_log(gmail_sender, receiver, 'ERROR' + project_name, f'Error getting pms token: {x.status_code}', gmail_user, gmail_pass, logging)
        return {
            'statusCode': 400,
            'body': f'Error getting pms token: {x.status_code}'
        }
    pms_token = x['access_token']
    
    url = f'{pms_url}/contracts/bulkFetch'
    headers = {'Authorization': f'Bearer {pms_token}', 'Content-Type': 'application/json'}
    try:
        x = requests.post(url, json={'accountIds': list(full_df['SBQQ__Account__c'].drop_duplicates())}, headers=headers)
    except Exception as error:
        email_log(gmail_sender, receiver, project_name, f"Error: Can't pull data from PMS Bulk fetch: {error}", gmail_user, gmail_pass, logging)
        print(f"Error: Can't pull data from PMS Bulk fetch: {error}")
        return {
            'statusCode': 400,
            'body': f"Error: Can't pull data from PMS Bulk fetch: {error}"
        }
    if x.ok:
        json_file = x.json()
    else:
        print(f'Error: {x.status_code}')
        return {
            'statusCode': 400,
            'body': f'Error getting pms bulk fetch api pull: {x.status_code}'
        }
    df = pd.json_normalize(json_file)
    a = pd.json_normalize(df['packages'])
    full_response_df = pd.DataFrame(columns=list(pd.json_normalize(a[0]).columns))
    for i in list(a.columns):
        b = pd.json_normalize(a[i])
        full_response_df = pd.concat([full_response_df, b])
        
    full_response_df = full_response_df[full_response_df['isActive']==True]
    package_type = full_response_df[['packageType', 'subscriptionId']]
    full_response_df = pd.json_normalize(full_response_df['mdmTenants'])
    tenant_df = pd.DataFrame(columns=list(pd.json_normalize(full_response_df[0]).columns))
    for i in list(full_response_df.columns):
        b = pd.json_normalize(full_response_df[i])
        tenant_df = pd.concat([tenant_df, b])

    required_cols = [
    'tenantId', 'tenantPurpose', 'reltioEnv', 'deploymentCloud', 'deploymentRegion',
    'packageId', 'contractId', 'salesConfig.subscriptionName', 'salesConfig.subscriptionId', 'salesConfig.startDate', 'salesConfig.endDate'
    ]
    for col in required_cols:
        if col not in tenant_df.columns:
            tenant_df[col] = ''
    tenant_df = tenant_df[required_cols]
    tenant_df = tenant_df.drop_duplicates()
    tenant_df = tenant_df.dropna(how='all')
    tenant_df = tenant_df[tenant_df['tenantId'].notna()]
    tenant_df = tenant_df.sort_values(['contractId', 'tenantId', 'packageId'])
    tenant_df = tenant_df.reset_index()
    tenant_df = pd.merge(tenant_df, package_type, how='left', left_on='packageId', right_on='subscriptionId')
    tenant_df = tenant_df[['tenantId','tenantPurpose', 'reltioEnv', 'deploymentCloud', 'deploymentRegion', 'packageId', 'contractId', 'packageType', 'salesConfig.subscriptionName', 'salesConfig.subscriptionId', 'salesConfig.startDate', 'salesConfig.endDate']]
    tenant_df = pd.merge(tenant_df, full_df[['SBQQ__Account__c', 'SBQQ__Contract__c', 'SBQQ__Account__r.Name', 'SBQQ__Contract__r.SBQQ__Opportunity__r.Name']].drop_duplicates(), how='inner', left_on='contractId', right_on='SBQQ__Contract__c')
    tenant_df = tenant_df.drop_duplicates()
    
    # tenant_df['RN'] = tenant_df.sort_values(['tenantId'], ascending=True).groupby(['tenantId']).cumcount() + 1
    # duplicate_list = tenant_df[tenant_df['RN']==2]['tenantId']
    # a=tenant_df[tenant_df['tenantId'].isin(duplicate_list)]
    # a = a.drop(columns=['RN'])
    # a = a.drop(columns=['packageId'])
    # a=a.drop_duplicates()
    #a.to_excel('C:/Users/EliMaruani/Desktop/pms_error.xlsx')

    # Identify duplicate tenant IDs and sending an email alert
    duplicate_tenants = tenant_df.loc[tenant_df['tenantId'].duplicated(keep=False), 'tenantId'].unique()
    duplicate_tenants = duplicate_tenants.tolist()
    if len(duplicate_tenants) > 0:
        email_log(gmail_sender, receiver, project_name, f'WARNING: Duplicate tenant IDs found:\n {duplicate_tenants}', gmail_user, gmail_pass, logging)
    
    tenant_json = tenant_df.to_json(orient='records')
    tenant_json = json.loads(tenant_json)
    
    full_payload = []
    i = 0
    for profile in tenant_json:
        i += 1
        with open(f"{folder_path}tenant.json", encoding="utf-8") as file:
            temp_tenant_detail = json.load(file).copy()
        body = temp_tenant_detail[0]
        body["crosswalks"][0]["value"] = profile['tenantId']
        body["crosswalks"][1]["value"] = profile['tenantId']
        body["crosswalks"][0]["url"] = '/'+profile['tenantId']
        body["crosswalks"][1]["url"] = '/'+profile['tenantId']
        body["attributes"]["TenantID"][0]["value"] = profile['tenantId']
        body = add_pop(body, profile['tenantPurpose'] + ' - ' + profile['SBQQ__Account__r.Name'], 'shortDescription')
        body["attributes"]["TenantType"][0]["value"] = 'MDM'
        body["attributes"]["TenantRecordType"][0]["value"] = 'Customer'
        body = add_pop(body, profile['reltioEnv'], 'Environment')
        body = add_pop(body, profile['salesConfig.subscriptionName'], 'subscriptionName')
        body = add_pop(body, profile['salesConfig.subscriptionId'], 'subscriptionId')
        body = add_pop(body, profile['salesConfig.startDate'], 'subStartDate')
        body = add_pop(body, profile['salesConfig.endDate'], 'subEndDate')
        body = add_pop(body, profile['deploymentCloud'], 'TenantdeploymentCloud')
        body = add_pop(body, profile['deploymentRegion'], 'TenantdeploymentRegion')
        body["attributes"]["ownedByReltioDept"][0]["value"] = 'Customer Success'
        body = add_pop(body, profile['packageId'], 'Root_ID')
        body["attributes"]["EndUserCustomer"][0]["value"]["Name"][0]["value"] = profile['SBQQ__Account__r.Name']
        body["attributes"]["EndUserCustomer"][0]["refEntity"]["crosswalks"][0]["value"] = profile['SBQQ__Account__c']
        body["attributes"]["EndUserCustomer"][0]["refEntity"]["crosswalks"][1]["value"] = profile['SBQQ__Account__c']
        body["attributes"]["EndUserCustomer"][0]["refRelation"]["crosswalks"][0]["value"] = profile['SBQQ__Account__c']+'_'+profile['tenantId']
        if profile['packageType'].upper() == 'BASE':
            body["attributes"]["BasePackageContract"][0]["refEntity"]['crosswalks'][0]["value"] = profile["contractId"]
            body["attributes"]["BasePackageContract"][0]["refEntity"]['crosswalks'][1]["value"] = profile["contractId"]
            body["attributes"]["BasePackageContract"][0]["refRelation"]['crosswalks'][0]["value"] = profile["contractId"]+'_'+profile['tenantId']
            body["attributes"]["BasePackageContract"][0]["value"]["ContractName"][0]["value"] = profile["SBQQ__Contract__r.SBQQ__Opportunity__r.Name"]
            body["attributes"].pop('AdditionalContract')
        else:
            body["attributes"]["AdditionalContract"][0]["refEntity"]['crosswalks'][0]["value"] = profile["contractId"]
            body["attributes"]["AdditionalContract"][0]["refEntity"]['crosswalks'][1]["value"] = profile["contractId"]
            body["attributes"]["AdditionalContract"][0]["refRelation"]['crosswalks'][0]["value"] = profile["contractId"]+'_'+profile['tenantId']
            body["attributes"]["AdditionalContract"][0]["value"]["ContractName"][0]["value"] = profile["SBQQ__Contract__r.SBQQ__Opportunity__r.Name"]
            body["attributes"].pop('BasePackageContract')
        full_payload.append(body)
    print('Starting entities POST to Reltio')
    try:
        post_result = entities_post(
            token=reltio_token,
            r360_url=r360_url,
            json_load=full_payload,
            batch_size=30,
            partial_overide=False,
            logging=logging
        )
    except Exception as error:
        email_log(gmail_sender, receiver, project_name, f"Error posting the entities to Reltio tenant. Code crash. - {error}", gmail_user, gmail_pass, logging)
        print(f"Error posting the entities to Reltio tenant. Code crash. - {error}")
        return {
            'statusCode': 400,
            'body': f"Error posting the entities to Reltio tenant. Code crash. - {error}"
        }
    else:
        post_reltio_res_ten = post_result["Message"]
        if post_result["Success"]:
            print(post_reltio_res_ten)
        else:
            email_log(gmail_sender, receiver,  project_name, post_reltio_res_ten, gmail_user, gmail_pass, logging)
            return {
            'statusCode': 400,
            'body': post_reltio_res_ten
            }

    final_message = "Successfully pushed all SFDC and PMS Usage data to Reltio.   <br> <br> Accounts:" + post_reltio_res_acc + "<br> <br> Contracts: " + post_reltio_res_con + "<br> <br> Tenants: " + post_reltio_res_ten + "<br> <br>"
    email_log(gmail_sender, receiver, project_name, final_message, gmail_user, gmail_pass, logging, 'success', None, sequence)
    return {
            'statusCode': 202,
            'body': final_message
        }
