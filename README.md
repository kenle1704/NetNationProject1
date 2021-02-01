# NetNationProject1

This project i builded out as a whole class in Linux, i found this easier to run and testing but it is not look really nice thus i create another project in esclipse to split the java file to multiple class 

to run this project you only have to run
java -cp lib/gson-2.8.6.jar:lib/mysql-connector-java-8.0.23.jar src/Partner_Report_Importer.java > output.txt

Make sure you update database cridential 
URL,username,password as your database set up

you will find the output data in output.txt


My database is a little bit different with requirement because i could not create usage column in chargeable table that i decide to use my own name convension 

Make sure your domains table will have domain column is unique 
