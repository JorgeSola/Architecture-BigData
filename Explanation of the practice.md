# BigData architecture practice.
## Explanation of the practice.
### Main final objective.
Give information in a very visual way to customers who are looking for a home in airbnb. Provide data to facilitate the choice of housing.

Register in a database all the useful information of Airbnb to obtain interesting information, such as trends in each country, types of housing most requested, months of increased activity, countries where there is more activity, average price per type of housing ... .

And finally, give information in an orderly manner to customers who are interested in having airbnb data that are ordered. For example, give access to our database so they can search and read the information in exchange for money.

### Diagram.  Architecture BigData.
The original idea is to mount a BigData architecture whose processes work automatically. 

The idea is to use tools given by google coud to automate the whole process. 

For this, it is necessary a bucket, a SQL DB (postgresql in this case) and the use of function cloud, scheduler cloud and a cluster that turns on only when necessary.

The process is divided in three parts:
 - Block 1. Extract Data.
	 -	 Download whole airbnb .csv.
	 - Python code: Get the main information of the .csv .
	 - Save the news .csv processed in our bucket in Google Cloud Storage.

 - Block 2. Transform Data.
	 - Get the .csv files from the bucket and give them the correct format to save them in SQL.
 - Block 3. Load Data.
	 - Create the new tables or replace them if already exists.
	 - Load data in the news tables.

I have created two diagrams. 
In the first diagram, a worker has to intervene. This is the model I have actually created. I let you the link here: [Keepcoding Big Data architecture](https://miro.com/app/board/o9J_kpEGHfw=/)

The second diagram is an optimized process. Better than the previous one.

The main objective of this process is to automate everything and avoid the need for worker intervention. I let you the link here: [Optimized process Keepcoding Big Data architecture](https://miro.com/app/board/o9J_klcc680=/).


###	Start point. Airbnb dataset.
Airbnb let us get the information about all the listenings, reviews, comments about the houses that appear on the platform.

We can find all the datasets in this link: [airbnb dataset](https://public.opendatasoft.com/explore/dataset/airbnb-listings/export/?disjunctive.host_verifications&disjunctive.amenities&disjunctive.features&q=Madrid&dataChart=eyJxdWVyaWVzIjpbeyJjaGFydHMiOlt7InR5cGUiOiJjb2x1bW4iLCJmdW5jIjoiQ09VTlQiLCJ5QXhpcyI6Imhvc3RfbGlzdGluZ3NfY291bnQiLCJzY2llbnRpZmljRGlzcGxheSI6dHJ1ZSwiY29sb3IiOiJyYW5nZS1jdXN0b20ifV0sInhBeGlzIjoiY2l0eSIsIm1heHBvaW50cyI6IiIsInRpbWVzY2FsZSI6IiIsInNvcnQiOiIiLCJzZXJpZXNCcmVha2Rvd24iOiJyb29tX3R5cGUiLCJjb25maWciOnsiZGF0YXNldCI6ImFpcmJuYi1saXN0aW5ncyIsIm9wdGlvbnMiOnsiZGlzanVuY3RpdmUuaG9zdF92ZXJpZmljYXRpb25zIjp0cnVlLCJkaXNqdW5jdGl2ZS5hbWVuaXRpZXMiOnRydWUsImRpc2p1bmN0aXZlLmZlYXR1cmVzIjp0cnVlfX19XSwidGltZXNjYWxlIjoiIiwiZGlzcGxheUxlZ2VuZCI6dHJ1ZSwiYWxpZ25Nb250aCI6dHJ1ZX0%3D&location=16,41.38377,2.15774&basemap=jawg.streets).
There two ways to download the information. Using the ApiKey or the easier way, download a .csv file.

The first objetive was, analyze the dataset and see which was the main information. 

So, i made a list with the main fields and i saw that i could divide the information in: global information of the houses, personal information of each house and secondary information which could be interested for the costumer.

I decided which files should belong to each table. And I analyzed if that structure would be optimal when saving the information in SQL.

In the  python code, i download all the dataset and i separate the information of the main .csv in this three parts. Creating three .csv: 

 - Airbnb_listenings_mod: 
	 - Host ID. 
	 - Host Name. 
	 - Host Neighbourhood. 
	 - Host Listings Count.
	 - City. 
	 - Country. 
	 - Property Type.
	 -  Room Type.
	 - Price.
	 - Security Deposit.
	 - Cleaning Fee.
	 - Number of Reviews. 
	 - Review Scores Value. 
	 - Cancellation Policy.
	 
 - airbnb_secondary_information:
	 -	Host ID.
	 - Street.
	 - Zipcode.
	 - Country Code.
	 - Latitude.
	 - Longitude.
	 - Bedrooms. 
	 - Beds.
	 - Weekly Price.
	 - Monthly Price.
	 - Minimum Nights. 
	 - Review Scores Accuracy.
	 - Review Scores Cleanliness. 
	 - Review Scores Checkin.
	 - Review Scores Communication.
	 - Review Scores Location.
 - flat information
	 - Host ID.
	 - TV.
	 - Cable TV.
	 - Kitchen.
	 - Smoking allowed.
	 - Pets allowed.
	 - Heating.
	 - Washer.
	 - Dryer.
	 - 24-hour check-in.

I decided not create a NoSQL database. I think it was unncessary, it's enaught to have the .csv files in the bucket. But could possible create a collection in mongodb for example.

###	Second point. Python code. ETL.
All the process. Since download the airbnb_dataset.csv until save the data in SQL, it has done with python.
The best way to execute this part, it would be with a function cloud, which execute the scipt when an scheduler uses an url to 
So, the process consists of:

 1. Download the airbnb_dataset.csv using the library 'request'.

 2.  Clean the information and keep only the main data using pandas. With pandas i can:
	 - Create serveral .csv files. One for each information type.
	 - Process and change the structure of the data if it's necessary.
	 - Delete the rows which contains null values. 
	 - Write news .csv to keep them in the bucket.
	 
 3. Save the news .csv files in the bucket. You have to connect with you Google Cloud Storage. There are several ways. You can use a key.json or google.auth or with apiKey.
	  
 4. Create new tables in our SQL Database.If the tables don't exists, the script create the tables. And if the tables already exists, I create temporary tables that are the same as the tables that already exist. Then I load the new data into the temporary tables and finally I replace the temporary tables with the old ones. 

You only have to connect to your DB by means of the host, user, password and name of the database. 
At the end, the objective is save the original information in the bucket and the transform information in the SQL Database and upload all the information each month.

###	Give visualization to the data.
Give visualization using a web page, tableau. Make Dashboards that may interest customers or even the airbnb itself.

Provide airbnb data well structured to companies / customers who may be interested in accessing the DB directly.
