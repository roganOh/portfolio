# Cause about do_load.py comments are so many, I'll make as md file
# These things work when only target tables are exist in integration db and columns are change
# Let target table name is origin
## do_lode works like these when rules are among merge and increasement
### increasement
``` 
1. origin change to origin_tmp  
2. make origin table with containing all columns which are in origin_tmp and new csv_files without raws
3. append from origin_tmp to origin
4. append from csv file to origin
5. remove origin_tmp
```
### merge
```
1. work increasement's 1~3
2. replace origin_tmp with csv file
3. merge into origin from origin_tmp
4. remove origin_tmp 

```