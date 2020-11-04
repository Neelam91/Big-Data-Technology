In this project I used AWS Elastic search, EC2, Terminal, Containerization and Python scripting to load and analyze millions of NYC Parking Violation data. 

To ingest the NYC parking data I used Socrata API in my python scripting to directly connect to NYC Open Data and pushes that information into an Elasticsearch. This way, the data was never saved into your EC2 instance but instead streamed directly to Elasticsearch.
Once data was called into EC2, I converted thenm into required format and uploaded into Elastic search. I was able to ingest 10 Million data into elastic search for this project. Once data was ingested, I used Kibana to visualize the violation dataset and created a dashboard for the same.
 

Docker build and run instructions:

In order to build and run my docker image for NYC parking violation data in terminal, I used the following command.

Open my EC2 terminal 
	
Run this command to open Project01 filebrowser:
docker run -dv "$PWD":/srv -p 5001:80 filebrowser/filebrowser


Go to Project01 by using cd Project01
	

Build docker image by using below command:
docker build -t bigdata1:1.0 . 
	
Run the docker image by using the below command:
docker run -v ${PWD}:/app -e DATASET_ID='nc67-uf89' -e APP_TOKEN='APP_TOKEN' -e ES_HOST= ‘ES_HOST' 
-e ES_USERNAME='USERNAME' -e ES_PASSWORD=’ES_PASSWORD' bigdata1:1.0 --page_size=100

