# customer360
Business Intelligence Warehouse for Customer and Stakeholder Engagement at GSA

## Prerequisites
In order to properly utilize this code you must first set up environment variables to connect to various datasets and write to the Amazon redshift database.

### Environment Variables
On the local machine where this code is stored you will need to set up the following environment variables in order to run the data_integration.py file.

* ELOQUA
* ELOQUA_PASSWORD
* ELOQUA_USERNAME

* REDSHIFT_PASSWORD
* REDSHIFT_USERNAME

* SALESFORCE_DOMAIN
* SALESFORCE_PASSWORD
* SALESFORCE_SECURITY_TOKEN
* SALESFORCE_USERNAME

* SALT1
* SALT2

## Folders
### authorizations
Programs that grant authorization to systems used in the intelligence warehouse within CASE.

### bi-data-warehouse
Programs that run database operations, all of which run in a specific order that is a representation of all GSA data that can be blended together based on customer information.

### data-integerations
Programs that pull data from source systems and insert and update data in the intelligence warehouse within CASE.

### logging
A series of csv log files that represent events when the scheduler and the schema builder programs run.

### scheduler
A program that runs the data integeration ultimately making up the source tables of the intelligence data warehouse within CASE.

### schema-builder
A program that runs the intelligence layer blending together all GSA systems.

### tools
A list of tools developed by the GSA Data Analytics and Research team within CASE.




