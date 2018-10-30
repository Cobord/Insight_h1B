# Language and Imports used

Scala

scala.io.Source

scala.collection.mutable.{Map,HashMap,PriorityQueue}

java.io._

# PROBLEM

One has a csv file with data of H1B workers. For all of the CERTIFIED workers, 
we would like to count how many of them work in given occupations and how many work in given states.
We would like to output the top 10 for each of these. If there are fewer than 10 such occupations or states
then simply all of them in order.

# APPROACH

We approach the problem more generally. Instead of just CERTIFIED and their STATES and OCCUPATIONS, we allow this to be abstracted.

Instead let there be a list of tuples like [(CASE_STATUS,CERTIFIED),(EMPLOYER_STATE,CA)]. This would instead of only counting amongst
CERTIFIED workers would only count CERTIFIED workers whose employer is based in California. This list is adjustable in a single line in
the main function.

Also instead of just two variables to count being state and occupations, this is also adjustable. This is specified by another list
like ("EMPLOYER_STATE","SOC_NAME","WORKSITE_STATE") would count from those 3 columns of the input.csv file. If not enough locations are
provided for the output files then the program throws an exception. In this case one must provide 3 files for the output to go. For notation,
say there are K such columns being tracked.

We also allow the top 10 to be adjustable into the top top_how_many which is adjustable in a single line of the main function.

From this we make K dictionaries that are keyed on the String that is a potential "SOC_NAME" etc with value being the count of how many
workers meeting the specified criteria have that as their "SOC_NAME" etc. At the same time we also keep track of the total number of workers
meeting the criteria. These strings are cleaned up first in the sense that there may be entries with "SOFTWARE DEVELOPERS, APPLICATIONS"
or SOFTWARE DEVELOPERS, APPLICATIONS and those should be counted together.

This is done by going line by line through the csv, checking if that worker meets all the list of criteria. If so all K dictionaries get updated
with that workers information. If distributed computed such as Spark was allowed, one could break this workload up.

From this list of K dictionaries we apply a function to everything that gives us a list of K heaps. That is we take each entry in the dictionary,
and put it in to a PriorityQueue so that the one with the highest counts are at the top priority. Ties in counts are broken by alphabetical order
in the key of the dictionary after they are cleaned up.

For each of the K heaps, we then write into each of the K output files. (If there aren't K, the program would have thrown exception earlier).
So for the top_how_many, one keeps dequeue'ing from the heap to get the most common, then the next most common and so on. The total_count that was
found already when building the dictionaries is used here to calculate the percentages without repeating that workload of counting.

# RUN

run.sh compiles with scalac to give the class H1BStats. The deprecation flag is on to warn on outdated functions. This is then executed with
several arguments. The first is the location of the input file. The next K are the locations of the output files. In the case of the problem at
hand K=2.

# POSSIBLE IMPROVEMENTS

This is the canonical example for MapReduce so if distributed computing was available, then that would be beneficial for larger inputs. The algorithm used
above is already the same sort, so minimal changes would be necessary.

# CLARIFICATIONS

Asked for this clarification in email, but did not hear back before submission. So assuming that input is not incorrectly formatted.

There is a note saying that each year has different columns. For example, in the H1b_FY_2014 file there are columns for 
LCA_CASE_WORKLOC1_STATE and LCA_CASE_WORKLOC2_STATE. Which one should be interpreted as the WORKSITE_STATE?
Similarly does LCA_CASE_SOC_NAME correspond to SOC_NAME? If these change from test case to test case, is 
there a pattern or might there be a time when the column is labelled JUNK and it is actually storing the STATE? 
The case above also means it is not just the substring STATE to look for to know which column is the correct column
to look at.