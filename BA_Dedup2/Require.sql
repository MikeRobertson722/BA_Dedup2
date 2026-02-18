
--- Approach must be 
1. Identify WHERE we can skip NO need TO run through process
2. Can it be solved WITH python code (mostly 95% TO 100% match)
3. Finaly use AI TO resolve (this may be somewhat costly $.5 TO $2 per 100 records IS what it estimated, but need TO verify )
4. THEN AT SOME point, possibly a once AFTER python run, AND again AFTER AI run have a manual process ok updates.
5. ALWAYS have an undo AFTER ANY run OR manual intervention
6. ALWAYS protect PII (ssn/tin, bank ACCOUNT numbers, etc.). 
7. FOR AI resolving CHECK FOR vulnerablilities. 


select  a.*, b.SSN, REGEXP_REPLACE(B.ssn, '[^0-9]', '')
from dgo.enertia.vw_business_associate_address a
JOIN dgo.enertia.vw_business_associate b
ON a.HDRCODE = b.BUS_ASSOC_CODE
WHERE a.LASTUPDATED <= '2026-02-06' AND b.ATT_TYPE = 'TaxInfo' AND a.HDRTYPECODE = 'BusAssoc'




-- Do not dedupe any like this
AND b.SSN LIKE ANY ('000%', '666%');
AND REGEXP_REPLACE(B.ssn, '[^0-9]', '') IN ('000000000','111111111','222222222','333333333','444444444', '555555555','666666666','777777777','888888888','999999999')

-- Identify all that need to be deduped do not run everything through.  Is this by ssn/name, name only (sometime missing ssn), etc.

-- Once Identified run through fuzzy match python program and if a high % of a match can be determined then auto match (95%+ )

-- All others will use AI match at the higher cost   --  Does the AI match have to use the API?

-- Always keep changes made by the program in a log and make sure there is a process that can be run to revert back to other times that the process has run, kinda an undo.

-- Use this for SSN or any other PII : TO_VARCHAR(SHA2(ssn_norm || '|SOME_SECRET_SALT_VALUE', 256), 'HEX') AS ssn_token_hex

-- Need a hard list of exceptions like Trusts, Departments (I think Steve at one time wanted a different BA if the department in the name field was different.  Other companies may differ)

-- Need super simple way to allow a user to verify and merge, as well as a way to unmerge, in other words need to be able to go back to any point of time - data is cheap

-- What are down sides of merging what's not to be merged

-- How do we score matching? At what % do we auto merge?  Down side of too many manual verifications if presented in an easy way to combine and undo combine?

-- Is there a down side to choosing a nickname "Bill" vs "William", should we code to change to more formal?

-- What about Jr. / Sr / second, 2nd, third, esq., etc.

-- Should we remove dr. drs. mr, mrs., miss, ms ,etc 

-- if there are commas in name (hdrname) field Usualy the last /sir name rather than first name.

-- Building a loop now.  This will allow a human to train at first, then we can give it some data and targets we know are good and have it train itself 
--		Version Tracking Table - Tracks each run with metadata
--		Backup Manager - Automated backup before each run
--		Restore Script - Easy rollback to any previous state
--		Modified Dedup Scripts - Auto-backup integration

-- Above it is using a backup and restore, but would like for it to 





ANDERSON, JAMES
ANDERSON, JAMES A. & ANNETTA
ANDERSON, JAMES CLARK SINGLE
ANDERSON, JOHN W.


FOLLOWING should MERGE:   Ignore spacing, simple mispellings AND special chars, CRLF CR LF, etc.
"AMOCO PRODUCTION COMPANY" ≠ "AMOCO PRODUCTION CO"
"AMERICAN HEART ASSOCIATION" ≠ "AMER HEART ASSOC"
"SPRINGFIELD MEDICAL ASSOCIATES" ≠ "SPRINGFIELD MEDICAL ASSOC"
Typos and spacing

"NorthShore Physicians Group" ≠ "Northshore Physicians Group"


*** Forcing human intervention FOR trust/departments.  May need TO ADD MORE here


*** Added trust, department FOR manual review: 


Trust entities require verification - similar trust names may belong to different beneficiaries
Trust entities require verification - similar trust names may belong to different beneficiaries
Estate entities require verification - similar estate names may belong to different individuals
Estate entities require verification - similar estate names may belong to different individuals
Estate entities require verification - similar estate names may belong to different individuals
Trust entities require verification - similar trust names may belong to different beneficiaries
Trust entities require verification - similar trust names may belong to different beneficiaries
Contains keyword "TR " which requires human review
Trust entities require verification - similar trust names may belong to different beneficiaries
Contains keyword "TR " which requires human review
Trust entities require verification - similar trust names may belong to different beneficiaries
Trust entities require verification - similar trust names may belong to different beneficiaries
Contains keyword "DEPT" which requires human review
Contains keyword "DEPT" which requires human review
Contains keyword "DEPT" which requires human review



-- Next step is to have the system create an interface to review and accept or reject.  Already keeps track of any changes, so if there is a mistake it can be easily recovered.  Last step will be to update or create a new 
-- Ba to consolidate any billing or corespondance.

((((((*)  Stopping FOR tonight getting overloaded AND need TO eat AND sleep. 


***** I assume this IS FOR the python code (FREE run) TO avoid costly AI (Costly being again $.5 TO $2 per 100 records)  May NOT be an ISSUE FOR larger companies, but will allow FOR marketing TO smaller companies. 

FYI
WHEN TO compact: /compact
2. Task Complexity
Simple Q&A: Can go higher (80-85%) comfortably
Complex debugging: Better to compact earlier (70%) to keep focus
Code generation: Needs clear context, compact at 60-70%

*********  
-- I'm trying to do this with agents, but not sure I'm working on this properly.  Seem to be running mostly at 50 to 80% context used.  Need to see if I can work on individual agents separately. Please research.
-- Seems to be doing a lot of what I'm not asking it to do.  Will allow now, but need to be sure this is necessary. 
-- Coimpacting anytime it get about 60%

-- See C:\ClaudeMain\BA_Dedup2\BA_Dedup2\ClaudeWork_2_17.docx for work done tonight.  Will be more.


Phase 1: Database & Security ✅
Fixed 4 SQL injection vulnerabilities
Created migration for 6 performance indexes
Optimized batch operations (90%+ fewer queries)
Phase 2: Memory Optimization ✅ (Substantial)
Removed 6 unnecessary DataFrame copies
Vectorized 3 critical operations
Expected Results:
60-80% memory reduction for large datasets
5-10x speedup in matching operations
Can now handle 500K+ records
Would you like me to:

Continue with Phase 3 (Performance infrastructure: benchmarking, monitoring, profiling)?
Skip to Phase 4 (Advanced optimizations: caching, parallel processing)?
Run tests to verify everything still works correctly?
Create documentation for what's been implemented so far?
The core optimizations are complete. Phases 3-4 add monitoring and advanced features.


-- At this point I'm about 10% sure what it IS doing.  It's caching now, 

-- Learning about blocking (groups that need to be considered to combine).  Make sure they are a specific as possible, it not cardinality grows expo.  **** This is very important and may differ between clients.
Be careful allowing it defaults: It was trying to use State only.
What Changed?
Old Approach ❌

State blocking:      4.6M pairs (too broad!)
Missing data fallback: 3.3M pairs (compared all against all)
Total:              7.6M pairs = 63 minutes






select hdrname, ADDRCONTACT, ADDRCITY, ADDRSTATE, ADDRZIPCODE, addrphone, ADDREMAIL, ADDRCONTACT
from dgo.enertia.vw_business_associate_address a
JOIN dgo.enertia.vw_business_associate b
ON a.HDRCODE = b.BUS_ASSOC_CODE
WHERE a.LASTUPDATED <= '2026-02-06' AND b.ATT_TYPE = 'TaxInfo' AND a.HDRTYPECODE = 'BusAssoc'
order by hdrname
limit 10000

