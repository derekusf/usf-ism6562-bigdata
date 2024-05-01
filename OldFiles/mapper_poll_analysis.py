#!/usr/bin/env python

# each line contains name of only 1 voted candidate
# for every line, get the first word received as the voted name, and count 1
# check if the voted name is valid aka in the candidate list. 
# Hadoop will sort these voted names
# stdin to the reducer.
import sys

candidates = ("Jaja","Jiji", "Jojo", "Juju","Jinjin")   #define valid candidate names
for line in sys.stdin: # look through data that is piped into this program 
    line = line.strip() # remove redundant spaces
    words = line.split() 
    voted = words[0] #expect the candidate name 

    #check if the voted is valid in candidate list
    #if the voted is not in the candidate list -> invalid
    #Write pair <voted, 1>, to be sorted and shuffled, then processed by reducers
    if voted in candidates:  
        print '%s\t%s' % (voted, 1)
    else: 
        print '%s\t%s' % ("Invalid", 1)  

