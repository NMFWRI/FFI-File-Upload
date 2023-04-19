Author: Corey Beinhart  
Contact: corey@nmhu.edu

This python program functions to migrate FFI data stored in XML files into our PostgreSQL database.

The code will parse XML files using the FFIFile and XMLFrame classes. These are built out to be fairly generalized in
the case that anyone wants to expand on this functionality. XMLFrame is a class that turns XML files into pandas DataFrames for easy handling and mimics many behaviors of the DataFrame. FFIFile is basically a collection of XMLFrames that also 
contains some metadata regarding the file. 

As it goes now, it will iterate through a directory and write each XML file into the FFI database. The XML file is loaded as a collection of DataFrames essentially, then the primary keys for each data frame are queried against what is already in your FFI database, and filters the XML file based on that. So instead of just skipping a file, the program will filter all the data out.

This is a slightly inefficient approach, but file names can change and I wasn't sure the most robust way to handle this, since I wanted it to be able to handle files of the same admin unit, but with variable data (something that FFI can't natively handle)

# Use

In order to use this, create a config.ini file in the base directory for this code. Since FFI servers are built on top of SQL Server, the template for a SQL server connection is as below:

[SQLServer1]
type = SQLServer
driver = mssql+pyodbc
server = 
database = 
user = 
password = 

where all of the blank fields should be filled out as the is relevant by the user, since it will change based on whether
or not the connection is local or remote.

Then, in the xml_to_rdb.py file, change the 'path' variable to the directory where your data set is.

# Future
In the near future, I would like to build a simple GUI for this so no one has to look at the code.

If you have any questions or requests, please reach out to me.
