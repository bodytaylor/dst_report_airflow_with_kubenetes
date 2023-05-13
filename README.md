Hey there! Let me tell you about a project that I came up with to spice up my boring work routine. 
It's all about automating the tedious tasks that I have to do every month!

Basically, I'm responsible for keeping track of the revenue from all the different properties that my company owns. 
And at the end of each month, I have to submit a report so that everyone gets their fair share of the commission. 
But here's the thing: the revenue data comes in a super boring PDF format. 
And my team has a habit of submitting things late, which can really mess with our accounting schedule.

But fear not! With my new automation project, I can easily collect all the revenue data and compile it into a snazzy report. 
And not only that, but I can also generate quarterly reports for the head of my department. 
No more half-days spent slaving away at my computer! Now I can sit back, relax, and let the magic of automation do its thing.

Here's the scope: I'm using Airflow to streamline the process of collecting revenue reports from my team. 
(I asked them to set auto send in Opera system)
I've set specific times for my team to send me emails from our Opera system, 
and Airflow will automatically read those emails and convert them into neat monthly reports.

At the end of each quarter, Airflow will trigger a task to merge all the monthly reports to create a quarterly report. 
And get this: the finished product is automatically uploaded to Google Bigquery. 
All I have to do is connect my data to Looker Studio.

With this automation project, I can finally say goodbye to those messy and time-consuming manual tasks. 
Now I can focus on the other taks and my team never get late on commission pay again!

Read about this project on Google Colab.
https://colab.research.google.com/drive/1aIBrljimixMtUMLuNntylvZijZciFs9I?usp=sharing
