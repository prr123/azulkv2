# azulkv

An experimental kv store written in golang. 

Aimed ad small to medium kv dbs (small < 1000 entries; medium < 10,000 entries).

## Difference to Map

Uses Slices to store data instead of map.

Use of hash function to find and compare keys.

## Speed Improvements

1. simple linear search up to xx entries. tbd with experiments
2. two level table look-up and linear search. 

