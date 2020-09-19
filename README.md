# BigData architecture practice.
## How to execute the main.py.
### Google Cloud.
This script need to be connected with google cloud storage and SQL Cloud (Postgresql).

So first, you have to create and [account](https://accounts.google.com/signup/v2/webcreateaccount?service=cloudconsole&continue=https%3A%2F%2Fconsole.cloud.google.com%2Fgetting-started%3Fproject%3Dpractical-mason-288819&hl=en_US&dsh=S1577473564%3A1600542203083545&gmb=exp&biz=false&flowName=GlifWebSignIn&flowEntry=SignUp&nogm=true) in Google Cloud.

If you already have your account created. The next step is to [create a bucket](https://cloud.google.com/storage/docs/quickstart-console) in Google Cloud Stores and [get the json Api](https://cloud.google.com/storage/docs/json_api).

Here there is a web page with information about the diferents way to connect your bucket with python: [connection with the bucket](https://google-auth.readthedocs.io/en/latest/user-guide.html). I'd used **Application default credentials**.

Secondly, you have to [create a SQL Postgresql](https://cloud.google.com/sql/docs/mysql/create-manage-databases) in google cloud SQL.


### Arguments
There are four arguments:
 - --postgresql-host.
 - --postgresql-user.
 - --postgresql-password.
 - --postgresql-db.

The execute script must be:
```
python main.py --postgresql-host [host database] --postgresql-user [user database] --postgresql-password [password] --postgresql-db [database name]
```

### Aditional information.
In this repository there is a .sql file where it's the script to create the tables in the SQL Database. It's not necessary execute this code in a SQL Terminal because the python script already execute this code.

The file config.py only shows an other way to connect the bucket with python.

There is a requirements.txt file to install al the libraries.