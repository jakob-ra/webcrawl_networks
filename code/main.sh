#!/bin/bash 
echo "Merge individual files into a single file..." 
python3 aggData.py 
echo "Done.\n
--------------------" 
echo "Clean data and detect language of website text..." 
python3 preprocess.py 
echo "Done.\n
--------------------" 
echo "Remove non-relevant content from webpages..." 
python3 relevantText.py "within"  
echo "Done.\n
--------------------" 
echo "Embed text data..."
python3 embed.py
echo "Done.\n
--------------------" 
echo "Remove non-relevant content from webpages..." 
python3 relevantText.py "between"  
echo "Done.\n
--------------------"
exit 
