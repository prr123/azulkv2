# azulkv2

copy from azulkv
An experimental kv store written in golang. 

Aimed ad small to medium kv dbs (small < 1000 entries; medium < 10,000 entries).

## Documentation

### add
adds a kv tuple  
### upd
updates the value of a tuple with the key “key”  
### del
deletes a tuple with key “key”  
### get/ find
returns the value of tuple with the key “key”  
### list
lists all kv tuples in the store (db)  
### entries
list the total number of tuples in the store (db)  
### info
returns info of the store (db)  


## Difference to Map

Uses Slices to store data instead of map.

Use of hash function to find and compare keys.

## Speed Improvements

1. simple linear search up to xx entries. tbd with experiments
2. two level table look-up and linear search. 

