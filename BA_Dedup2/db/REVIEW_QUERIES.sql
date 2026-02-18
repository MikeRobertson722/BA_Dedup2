-- ============================================================================
-- HUMAN REVIEW QUEUE - SQL QUERIES FOR UI DEVELOPMENT
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. GET ALL PENDING REVIEWS (Main view)
-- ----------------------------------------------------------------------------
SELECT * FROM pending_reviews;

-- Or with more details:
SELECT
    id,
    name_parsed as name,
    address,
    city,
    state,
    zip,
    phone,
    email,
    review_keywords,
    review_reason,
    flagged_date,
    has_address,
    has_phone
FROM pending_reviews
ORDER BY flagged_date DESC;


-- ----------------------------------------------------------------------------
-- 2. FILTER BY KEYWORD TYPE (for filtered views)
-- ----------------------------------------------------------------------------
-- Get all TRUST records
SELECT * FROM human_review_queue
WHERE review_keywords LIKE '%TRUST%'
  AND review_status = 'pending'
ORDER BY name_parsed;

-- Get all ESTATE records
SELECT * FROM human_review_queue
WHERE review_keywords LIKE '%ESTATE%'
  AND review_status = 'pending'
ORDER BY name_parsed;

-- Get all DEPARTMENT records
SELECT * FROM human_review_queue
WHERE review_keywords LIKE '%DEPARTMENT%'
  AND review_status = 'pending'
ORDER BY name_parsed;


-- ----------------------------------------------------------------------------
-- 3. FIND POTENTIAL DUPLICATES WITHIN REVIEW QUEUE
-- ----------------------------------------------------------------------------
-- Records with same name (likely duplicates)
SELECT
    name_parsed,
    COUNT(*) as duplicate_count,
    GROUP_CONCAT(id) as review_ids,
    GROUP_CONCAT(city || ', ' || state) as locations
FROM human_review_queue
WHERE review_status = 'pending'
GROUP BY name_parsed
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;


-- ----------------------------------------------------------------------------
-- 4. GET SINGLE RECORD DETAILS (for detail view)
-- ----------------------------------------------------------------------------
SELECT *
FROM human_review_queue
WHERE id = ?;  -- Replace ? with actual ID


-- ----------------------------------------------------------------------------
-- 5. APPROVE A RECORD (keep as separate entity)
-- ----------------------------------------------------------------------------
UPDATE human_review_queue
SET
    review_status = 'approved',
    decision = 'keep_separate',
    reviewed_by = ?,  -- Username
    reviewed_date = CURRENT_TIMESTAMP,
    review_notes = ?,  -- Optional notes
    updated_date = CURRENT_TIMESTAMP
WHERE id = ?;


-- ----------------------------------------------------------------------------
-- 6. REJECT A RECORD (mark for deletion)
-- ----------------------------------------------------------------------------
UPDATE human_review_queue
SET
    review_status = 'rejected',
    decision = 'delete',
    reviewed_by = ?,
    reviewed_date = CURRENT_TIMESTAMP,
    review_notes = ?,
    updated_date = CURRENT_TIMESTAMP
WHERE id = ?;


-- ----------------------------------------------------------------------------
-- 7. MARK FOR MERGING (merge with another cluster)
-- ----------------------------------------------------------------------------
UPDATE human_review_queue
SET
    review_status = 'merged',
    decision = 'merge_with_cluster',
    merge_with_cluster_id = ?,  -- Target cluster ID
    reviewed_by = ?,
    reviewed_date = CURRENT_TIMESTAMP,
    review_notes = ?,
    updated_date = CURRENT_TIMESTAMP
WHERE id = ?;


-- ----------------------------------------------------------------------------
-- 8. BULK APPROVE SIMILAR RECORDS
-- ----------------------------------------------------------------------------
-- Approve all records with same name
UPDATE human_review_queue
SET
    review_status = 'approved',
    decision = 'keep_separate',
    reviewed_by = ?,
    reviewed_date = CURRENT_TIMESTAMP,
    review_notes = 'Bulk approved - similar name pattern',
    updated_date = CURRENT_TIMESTAMP
WHERE name_parsed = ?
  AND review_status = 'pending';


-- ----------------------------------------------------------------------------
-- 9. GET REVIEW STATISTICS (for dashboard)
-- ----------------------------------------------------------------------------
-- Overall stats
SELECT
    review_status,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM human_review_queue), 1) as percentage
FROM human_review_queue
GROUP BY review_status
ORDER BY count DESC;

-- By keyword
SELECT
    review_keywords,
    COUNT(*) as pending_count
FROM human_review_queue
WHERE review_status = 'pending'
GROUP BY review_keywords
ORDER BY pending_count DESC;

-- By reviewer
SELECT
    reviewed_by,
    COUNT(*) as reviews_completed,
    MIN(reviewed_date) as first_review,
    MAX(reviewed_date) as last_review
FROM human_review_queue
WHERE review_status != 'pending'
GROUP BY reviewed_by
ORDER BY reviews_completed DESC;


-- ----------------------------------------------------------------------------
-- 10. SEARCH FUNCTIONALITY
-- ----------------------------------------------------------------------------
-- Search by name
SELECT * FROM human_review_queue
WHERE name_parsed LIKE '%' || ? || '%'
  AND review_status = 'pending'
ORDER BY name_parsed;

-- Search by city/state
SELECT * FROM human_review_queue
WHERE (city LIKE '%' || ? || '%' OR state = ?)
  AND review_status = 'pending'
ORDER BY state, city, name_parsed;


-- ----------------------------------------------------------------------------
-- 11. GET RECORDS NEEDING ATTENTION
-- ----------------------------------------------------------------------------
-- Oldest pending reviews
SELECT id, name_parsed, city, state, flagged_date
FROM human_review_queue
WHERE review_status = 'pending'
ORDER BY flagged_date ASC
LIMIT 50;

-- Records with missing data (might need more investigation)
SELECT id, name_parsed, review_keywords
FROM human_review_queue
WHERE review_status = 'pending'
  AND (address IS NULL OR address = '')
  AND (phone IS NULL OR phone = '')
ORDER BY flagged_date DESC;


-- ----------------------------------------------------------------------------
-- 12. UNDO A REVIEW (reset to pending)
-- ----------------------------------------------------------------------------
UPDATE human_review_queue
SET
    review_status = 'pending',
    decision = NULL,
    merge_with_cluster_id = NULL,
    reviewed_by = NULL,
    reviewed_date = NULL,
    review_notes = review_notes || ' [RESET: ' || datetime('now') || ']',
    updated_date = CURRENT_TIMESTAMP
WHERE id = ?;


-- ----------------------------------------------------------------------------
-- 13. EXPORT APPROVED RECORDS
-- ----------------------------------------------------------------------------
-- Get all approved records ready for production
SELECT
    name_parsed as name,
    address,
    city,
    state,
    zip,
    phone,
    email,
    contact_person,
    reviewed_by,
    reviewed_date
FROM human_review_queue
WHERE review_status = 'approved'
ORDER BY name_parsed;


-- ----------------------------------------------------------------------------
-- 14. GET RECORDS FOR BATCH REVIEW
-- ----------------------------------------------------------------------------
-- Get next N records for review (pagination)
SELECT *
FROM human_review_queue
WHERE review_status = 'pending'
ORDER BY flagged_date DESC
LIMIT ? OFFSET ?;


-- ----------------------------------------------------------------------------
-- 15. DELETE COMPLETED REVIEWS (cleanup)
-- ----------------------------------------------------------------------------
-- WARNING: Only run this after exporting/backing up data
DELETE FROM human_review_queue
WHERE review_status IN ('approved', 'rejected', 'merged')
  AND reviewed_date < datetime('now', '-30 days');


-- ============================================================================
-- EXAMPLE WORKFLOW IN UI
-- ============================================================================

-- 1. Load pending reviews page:
--    SELECT * FROM pending_reviews LIMIT 50;
--
-- 2. User clicks on record ID 123:
--    SELECT * FROM human_review_queue WHERE id = 123;
--
-- 3. User approves it:
--    UPDATE human_review_queue
--    SET review_status = 'approved', decision = 'keep_separate',
--        reviewed_by = 'john_doe', reviewed_date = CURRENT_TIMESTAMP
--    WHERE id = 123;
--
-- 4. Show updated dashboard:
--    SELECT review_status, COUNT(*) FROM human_review_queue GROUP BY review_status;
