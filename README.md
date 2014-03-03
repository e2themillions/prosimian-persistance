prosimian-persistance
=====================

Simple MySQL persistence library (ORM) that allows you easily perfom CRUD operations on your .NET objects to MySQL.

This is an ultra simple persistence library that allows you easily perfom CRUD (create/read/update/delete) operations on your .NET object without touching SQL.. However - it also gets out of your way and let you control the SQL directly when you need.

There is no cascading (except from what you setup in MySQL).

Benefits: Learning curve is much smaller than that of nhibernate.

Mapping is done by simply naming your object properties and table columns the same..

The source code includes a simple test project to show how it is set up, and there is also a pdf in the downloads section with a quick start guide..

Make sure to download the two required libraries and update the references to System.Data and log4net.
