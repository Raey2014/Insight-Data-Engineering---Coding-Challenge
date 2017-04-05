Insight Data Engineering - Coding Challenge Application for May 2017

Submitted by: Merzu Kebede Belete

The python script is placed under the src folder. The process_log.py uses bash script run.sh placed in the main folder. It solves all the challenge questions, which reads the log file, clean up and put it in to a dataframe. The cleaned output file of the original file is placed in the main directory. It ranks the most active IP hosts, bandwidth consumption and busiest 60-minute period in the extended duration of time or days. It also finds three failed login attempts from the same IP address in 20sec duration and helps to block all further attempts for the next 5 minutes.


 Bonus:
In the other folder (insight_testsuite), bonus.py script helps to visualize the distribution of delta time from the first failed login attempts to the third . Clearly, the distribution is a mixture of two Poisson distributions. This can suggest us the failed attempts are most probably two different mechanisms (say:  cyber attack vs incorrect login password?). In turn failed login attempts can be remodeled not only 20secs  but two different distributions (I did not include this model in the data analysis).

This script uses pandas and numpy libraries.

For more information on the coding-challenge check out https://github.com/InsightDataScience/fansite-analytics-challenge.
