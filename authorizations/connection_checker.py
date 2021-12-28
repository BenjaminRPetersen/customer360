# -*- coding: utf-8 -*-
"""
Title: Check Connections

Created on Mon Dec 27 13:18:12 2021

@author: benjaminrpetersen
"""
from salesforce_client import client
from datetime import date
from eloqua import s, retries, client_name, username, password
from redshift import conn

"""

ELOQUA CONNECTION

"""
try:
   s.get('https://secure.p03.eloqua.com/api/bulk/2.0/accounts/fields',
   auth=(client_name+'\\'+username, password),
   headers = {'Content-Type':'application/json'})
   webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAAwmb61yg/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=KrRVYvemQViCh0OOOUD1hPJDG7Sj-0_IyYFySoeqJok%3D'
   webhook_json = '''{
                        "cards": [
                            {
                                "sections": [
                                    {
                                        "widgets": [
                                        {
                                                    "image": { "imageUrl": "https://iconape.com/wp-content/files/wy/346550/png/346550.png" }
                                              },
                                            {
                                                "keyValue": {
                                                    "topLabel": "Eloqua API Connection",
                                                    "content": "'''+str(date.today())+'''",
                                                    "contentMultiline": "false",
                                                    "bottomLabel": "Successful",
                                                    "button": {
                                                        "textButton": {
                                                           "text": "LOGIN TO ELOQUA",
                                                           "onClick": {
                                                               "openLink": {
                                                                    "url": "https://login.eloqua.com/?ReturnUrl=%2FMain.aspx&CheckFrame=false"
                                                                }
                                                            }
                                                          }
                                                     }
                                                 }
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }'''
   e_post = s.post(webhook_url, webhook_json)
except Exception as err:
   webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAAwmb61yg/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=KrRVYvemQViCh0OOOUD1hPJDG7Sj-0_IyYFySoeqJok%3D'
   webhook_json = '''{
                        "cards": [
                            {
                                "sections": [
                                    {
                                        "widgets": [
                                        {
                                                    "image": { "imageUrl": "https://img.favpng.com/23/25/4/computer-icons-error-scalable-vector-graphics-clip-art-png-favpng-hcvtJDbpweJSSTVcjz5rYAxBQ.jpg" }
                                              },
                                            {
                                                "keyValue": {
                                                    "topLabel": "Eloqua API Connection",
                                                    "content": "'''+str(date.today())+''' '''+str(err)+'''",
                                                    "contentMultiline": "false",
                                                    "bottomLabel": "Failure",
                                                    "button": {
                                                        "textButton": {
                                                           "text": "LOGIN TO ELOQUA",
                                                           "onClick": {
                                                               "openLink": {
                                                                    "url": "https://login.eloqua.com/?ReturnUrl=%2FMain.aspx&CheckFrame=false"
                                                                }
                                                            }
                                                          }
                                                     }
                                                 }
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }'''
   e_post = s.post(webhook_url, webhook_json)


"""

SALESFORCE CONNECTION

""" 
try:
    sf = client.sobjects.query("   SELECT	Id\
                                                                , Name\
                                                                , FirstName\
                                                                , LastName\
                                                                , Manager_Name__c\
                                                                , UserRoleId\
                                                                , CompanyName\
                                                                , Division\
                                                                , Department\
                                                                , Title\
                                                                , DelegatedApproverId\
                                                                , LastLoginDate\
                                                                , CreatedDate\
                                                                , CreatedById\
                                                                , LastModifiedDate\
                                                                , LastModifiedById\
                                                                , ContactId\
                                                                , AccountId\
                                                                , Address\
                                                    FROM	User\
                                                    LIMIT   1")
    webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAAwmb61yg/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=KrRVYvemQViCh0OOOUD1hPJDG7Sj-0_IyYFySoeqJok%3D'
    webhook_json = '''{
                        "cards": [
                            {
                                "sections": [
                                    {
                                        "widgets": [
                                        {
                                                    "image": { "imageUrl": "https://www.salesforce.com/news/wp-content/uploads/sites/3/2021/05/Salesforce-logo.jpg" }
                                              },
                                            {
                                                "keyValue": {
                                                    "topLabel": "Salesforce API Connection",
                                                    "content": "'''+str(date.today())+'''",
                                                    "contentMultiline": "false",
                                                    "bottomLabel": "Successful",
                                                    "button": {
                                                        "textButton": {
                                                           "text": "LOGIN TO SALESFORCE",
                                                           "onClick": {
                                                               "openLink": {
                                                                    "url": "https://fas.lightning.force.com/lightning/page/home"
                                                                }
                                                            }
                                                          }
                                                     }
                                                 }
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }'''
    sf_post = s.post(webhook_url, webhook_json) 
except Exception as err:
    webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAAwmb61yg/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=KrRVYvemQViCh0OOOUD1hPJDG7Sj-0_IyYFySoeqJok%3D'
    webhook_json = '''{
                        "cards": [
                            {
                                "sections": [
                                    {
                                        "widgets": [
                                        {
                                                    "image": { "imageUrl": "https://img.favpng.com/23/25/4/computer-icons-error-scalable-vector-graphics-clip-art-png-favpng-hcvtJDbpweJSSTVcjz5rYAxBQ.jpg" }
                                              },
                                            {
                                                "keyValue": {
                                                    "topLabel": "Salesforce API Connection",
                                                    "content": "'''+str(date.today())+''' '''+str(err)+'''",
                                                    "contentMultiline": "false",
                                                    "bottomLabel": "Failure",
                                                    "button": {
                                                        "textButton": {
                                                           "text": "LOGIN TO SALESFORCE",
                                                           "onClick": {
                                                               "openLink": {
                                                                    "url": "https://fas.lightning.force.com/lightning/page/home"
                                                                }
                                                            }
                                                          }
                                                     }
                                                 }
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }'''
    sf_post = s.post(webhook_url, webhook_json)

"""

REDSHIFT CONNECTION

"""

try:
    red = conn.execute("select count(*) ct from customer360.s_salesforce_user").fetchall()[0][0]
    webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAAwmb61yg/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=KrRVYvemQViCh0OOOUD1hPJDG7Sj-0_IyYFySoeqJok%3D'
    webhook_json = '''{
                        "cards": [
                            {
                                "sections": [
                                    {
                                        "widgets": [
                                        {
                                                    "image": { "imageUrl": "https://cdn2.iconfinder.com/data/icons/amazon-aws-stencils/100/Database_copy_Amazon_RedShift-512.png" }
                                              },
                                            {
                                                "keyValue": {
                                                    "topLabel": "Redshift Database Connection",
                                                    "content": "'''+str(date.today())+'''",
                                                    "contentMultiline": "false",
                                                    "bottomLabel": "Successful",
                                                    "button": {
                                                        "textButton": {
                                                           "text": "LOGIN TO REDSHIFT",
                                                           "onClick": {
                                                               "openLink": {
                                                                    "url": "https://rs-dev-tst1.prod-lde.bsp.gsa.gov:5439/edw"
                                                                }
                                                            }
                                                          }
                                                     }
                                                 }
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }'''
    red_post = s.post(webhook_url, webhook_json) 
except Exception as err:
    webhook_url = 'https://chat.googleapis.com/v1/spaces/AAAAwmb61yg/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=KrRVYvemQViCh0OOOUD1hPJDG7Sj-0_IyYFySoeqJok%3D'
    webhook_json = '''{
                        "cards": [
                            {
                                "sections": [
                                    {
                                        "widgets": [
                                        {
                                                    "image": { "imageUrl": "https://img.favpng.com/23/25/4/computer-icons-error-scalable-vector-graphics-clip-art-png-favpng-hcvtJDbpweJSSTVcjz5rYAxBQ.jpg" }
                                              },
                                            {
                                                "keyValue": {
                                                    "topLabel": "Redshift Database 
                                                    Connection",
                                                    "content": "'''+str(date.today())+''' '''+str(err)+'''",
                                                    "contentMultiline": "false",
                                                    "bottomLabel": "Failure",
                                                    "button": {
                                                        "textButton": {
                                                           "text": "LOGIN TO REDSHIFT",
                                                           "onClick": {
                                                               "openLink": {
                                                                    "url": "https://rs-dev-tst1.prod-lde.bsp.gsa.gov:5439/edw"
                                                                }
                                                            }
                                                          }
                                                     }
                                                 }
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }'''
    red_post = s.post(webhook_url, webhook_json)
