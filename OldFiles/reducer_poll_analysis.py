#!/usr/bin/env python


from operator import itemgetter
import sys
"""This program received a stdin in pipe from mappers. Each mapper will send a the pair <voted name, 1>
, and so multiple names will represents as multiple entries in the list. 
Hadoop sorts the by voted name. Therefore, reducing this requires us
to count similar names and print output voted names + count when a transition from one name
to the next occurs. We have a special end case, because the last name will not see 
a transition to a new name, we need to add one last print outside the loop."""
current_vote = None
current_count = 0
vote = None
for line in sys.stdin: # read all stdin line by line (like it was a file)

    # strip and split the vote count pair    
    line = line.strip()
    vote, count = line.split('\t')
    count = int(count)
    
    # if we have a new vote
    if current_vote != vote and current_vote != None:
        print '%s\t%s' % (current_vote, current_count)
        current_count = 0
    
    current_vote = vote
    current_count += count

# since at the end of our loop, we haven't seen a 'transition' from one name to another
# we need to print this here.
print '%s\t%s' % (current_vote, current_count)

