"""
Query Groups and Similarity Scores.
Demonstrates how to view cluster_id and similarity_score in both source and deduplicated tables.
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from data.db_connector import DatabaseConnector
from utils.logger import get_logger

logger = get_logger('query_groups')

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)


def main():
    """Query and display groups and similarity scores."""

    logger.info("=" * 80)
    logger.info("QUERYING GROUPS AND SIMILARITY SCORES")
    logger.info("=" * 80)
    logger.info("")

    db = DatabaseConnector()
    engine = db.db.get_engine()

    # 1. View all source records with their cluster assignments
    logger.info("1. SOURCE RECORDS WITH CLUSTER ASSIGNMENTS:")
    logger.info("-" * 80)

    query = """
    SELECT
        source_record_id,
        name,
        city,
        state,
        zip,
        cluster_id,
        ROUND(similarity_score * 100, 1) as match_percent
    FROM ba_source_records
    ORDER BY cluster_id, source_record_id
    """

    source_df = pd.read_sql(query, engine)
    print(source_df.to_string(index=False))
    logger.info("")

    # 2. View duplicate groups (cluster_id >= 0)
    logger.info("2. DUPLICATE GROUPS (Records that were matched):")
    logger.info("-" * 80)

    query = """
    SELECT
        cluster_id as group_id,
        COUNT(*) as record_count,
        GROUP_CONCAT(name, ' | ') as names,
        MAX(similarity_score) * 100 as max_match_percent
    FROM ba_source_records
    WHERE cluster_id >= 0
    GROUP BY cluster_id
    ORDER BY cluster_id
    """

    groups_df = pd.read_sql(query, engine)
    if not groups_df.empty:
        print(groups_df.to_string(index=False))
    else:
        print("No duplicate groups found")
    logger.info("")

    # 3. View deduplicated records with cluster info
    logger.info("3. DEDUPLICATED RECORDS (Golden Records):")
    logger.info("-" * 80)

    query = """
    SELECT
        name,
        address,
        city,
        state,
        zip,
        cluster_id as group_id,
        ROUND(similarity_score * 100, 1) as match_percent
    FROM business_associates_deduplicated
    ORDER BY cluster_id
    """

    try:
        dedup_df = pd.read_sql(query, engine)
        print(dedup_df.to_string(index=False))
    except Exception as e:
        logger.warning(f"Could not query deduplicated records: {e}")

    logger.info("")

    # 4. Show detailed view of one group
    logger.info("4. DETAILED VIEW OF FIRST DUPLICATE GROUP:")
    logger.info("-" * 80)

    if not groups_df.empty:
        first_cluster = groups_df.iloc[0]['group_id']

        query = f"""
        SELECT
            sr.source_record_id,
            sr.name,
            sr.address,
            sr.city,
            sr.state,
            sr.zip,
            sr.cluster_id as group_id,
            ROUND(sr.similarity_score * 100, 1) as match_percent,
            ma.match_method,
            ma.ai_reasoning
        FROM ba_source_records sr
        LEFT JOIN ba_merge_audit ma ON sr.cluster_id = ma.cluster_id
        WHERE sr.cluster_id = {first_cluster}
        """

        detail_df = pd.read_sql(query, engine)
        print(detail_df.to_string(index=False))
        logger.info("")

    # 5. Summary statistics
    logger.info("5. SUMMARY STATISTICS:")
    logger.info("-" * 80)

    total_source = len(source_df)
    duplicates = len(source_df[source_df['cluster_id'] >= 0])
    unique_records = len(source_df[source_df['cluster_id'] == -1])
    num_groups = len(groups_df)

    logger.info(f"Total source records: {total_source}")
    logger.info(f"Records in duplicate groups: {duplicates}")
    logger.info(f"Unique records (no duplicates): {unique_records}")
    logger.info(f"Number of duplicate groups: {num_groups}")

    if duplicates > 0:
        avg_score = source_df[source_df['cluster_id'] >= 0]['match_percent'].mean()
        logger.info(f"Average match percentage: {avg_score:.1f}%")

    logger.info("")
    logger.info("=" * 80)
    logger.info("QUERY COMPLETE")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
