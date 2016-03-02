A list of current features of the new MESSAGE database framework
================================================================

Import and clean-up of Message Sqlite dump into ORACLE database
----

- import complete Message sqlite dump
- replace parameter names by longer, more explicit names
- replace the trick that a value of "-1" in investment cost means "no investment possible" by a category-mapping (cat_investment('none',...)
- drop undefined constraints (i.e., if there exists no associated relation definition)
- check for duplicate short identifiers, replace if necessary
- replace all short identifiers in parameter keys (make short id’s obsolete later)
- replace "Message infinity" by proper infinity

Export data from ORACLE database
----

- write complete Message LDB files ("undoing" clean-up during import) 
- write partial files (update files) - *currently not maintained*
- write to simple text files ("manual" GAMS files, AMPL, etc.)
- write complete GAMS data files (GDX)
- add auxiliary parameters during (e.g., duration of periods)

Change specific database entries
----

- per-item changelog and annotations (old/new values, user, date-time, script, comments)
- **to do:** function to automatically undo specific changes
- import set and parameter changes directly from original data sources  

  + import changes using Excel spreadsheet following the IAMC-template
  + using any Python/R data-processing script plus a Java interface
  + (for technology cost projection: import changes from Nils' Excel spreadsheet using Java) 
  + using "manual" Java functions

- includes sanity checks ("forgotten" technologies or years, nonsensical values, etc.)
- **to do:** include more sanity checks ("forgotten" technologies or years, nonsensical values, etc.)
- **to do:** restructure changelog as relational database

Compare, cut and merge Message datastructure
---- 

- compare entire datasets (by set elements and parameter values)
- cut/merge by set of technologies, etc. based on an Excel table
- "seed" parameters for new technologies (etc.) from existing technologies (etc.)
- **to do:** add new years by dissection (e.g., insert year 2015 between 2010 and 2020) 

Reference time series and standard look-up tables for aggregations
----
This will be useful for calibration of model runs and for a semi-automated formatting and aggregation of model results.

- list and synomym replacement list for nodes and hierarchy level
  (including a consistency check so that only time series can be imported that relate to nodes declared in the node list)
- **to do:** list and synonym replacement list for fuels and  hierarchy levels 
- index key ("variable" in IAMC lingo) replacement using relational database for more efficient storage and access time
- import of reference time series (directly from original data sources)

  + import changes using Excel spreadsheet following the IAMC-template
  + using any Python/R data-processing script plus a Java interface
  + using "manual" Java functions

- **to do:** implement look-up tables for standard aggregation/conversion/etc.  
  
Spatial and temporal disaggregation
----
The database is built on a completely flexible hierarchical structure for spatial and temporal disaggregation in the Message datastructure

- import of disaggregation structure for a run id via an Excel template  
- **to do:** easy tools for modification of parameters across hierarchical levels (cut, merge and "seed" could be used for that)
- **to do:** implement sanity and consistency checks
  
