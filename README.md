#Bigdata Engineer Test
##Instructions

Clone the repository
You will find some example input files under src/main/resources. All input files are TSV files.
The file src/main/resources/sale.tsv provides some examples of output you can use as test cases.
Navigate to src/main/scala/com/tesco/SalesTransform.scala
Your job is to finish the implementation of this file
Implement the main function to read all data files in and transform them in RDD's of the case classes
The customer data of birth of has come from many systems. You need to clean the DOB. You can assume that the examples in customer.tsv contain all the cases of formats of DOB.
Implement transformData to transform the data into Sale case class
Write the Sale RDD to a Parquet file
Return your final code to us as a zip file, along with any instructions on how to build. Please do not upload your code to a public Git Repo
General Instructions

Unit test as much as possible. You won't have access to a live cluster for this test, so think how you can test your code as much as possible
Structure your code in a way you see fit (add new classes, split code into multiple files, change package structure etc.)
Remember we may want to re-use logic e.g. we may need to transform the customer data in the same way in several locations. How would you structure your code to achieve this?
Add any additional libraries you want to use
If you make any assumptions, please comment about them in your code
Example TSV files are included under resources. You can assume the columns and ordering of the case classes correspond exactly to the TSV's
The POM contains suggested target Java, Scala and Spark versions. You may change these to suit your setup.
