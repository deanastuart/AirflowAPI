# AirflowAPI
This project set up an airflow workflow to make a series of REST API requests from The Movie DB (https://www.themoviedb.org/). An actor's name is restructured to make a search API request. The result is filtered to aquire the actor's id number which is used for a second API request to receive their movie history. That result is written into a csv file named with the actor's name. 


