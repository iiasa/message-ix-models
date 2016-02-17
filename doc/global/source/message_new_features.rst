A list of current features of the new MESSAGE database framework
==========================

Import and clean-up of Message Sqlite dump into ORACLE database
----

- import complete Message sqlite dump
- drop undefined constraints (i.e., if there exists no associated relation definition)
- check for duplicate short identifiers, replace if necessary
- replace all short identifiers in parameter keys (make short id’s obsolete later)
- replace "Message infinity" by proper infinity

Export data from ORACLE database
----

- write complete LDB files ("undoing" clean-up during import) 
- write partial files (update files) - *currently not maintained*
- write to simple text files ("manual" GAMS files, AMPL, etc.)
- write complete GAMS data files (GDX)
- add auxiliary parameters during export (e.g., duration of periods)

Change database entries
----

- per-item changelog and annotations (old/new values, user, date-time, script, comments)
- **to do:** restructure changelog as relational database
- import list of set and parameter changes using Excel template 
- **to do:** import directly from original data sources (using Excel, R, etc.)
- includes sanity checks (“forgotten” technologies or years, nonsensical values, etc.)
- **to do:** include more sanity checks

Cut and merge datasets
---- 

- compare entire datasets (by set elements and parameter values)
- cut/merge by set of technologies, etc. based on an Excel table
- "seed" parameters for new technologies (etc.) from existing technologies (etc.)
- **to do:** add new years by dissection (e.g., insert year 2015 between 2010 and 2020) 

Reference time series and standard look-up tables for aggregations
----

- **to do:** import reference data from IEA and other sources for comparison/calibration
- **to do:** implement look-up tables for standard aggregation/conversion/etc.  

Spatial and temporal disaggregation
----

- implemented a completely flexible hierarchical structure for spatial and temporal disaggregation
- import of disaggregation structure via Excel template  
- **to do:** modify parameters across hierarchical levels 