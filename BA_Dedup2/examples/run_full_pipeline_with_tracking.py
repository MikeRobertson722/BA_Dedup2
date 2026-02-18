"""
Full Pipeline with Import Tracking and Audit Trail.
Demonstrates complete deduplication with source tracking and lineage.
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from data.import_tracker import ImportTracker
from utils.geo_lookup import GeoLookup
from agents.validation_agent import ValidationAgent
from agents.hybrid_matching_agent import HybridMatchingAgent
from agents.merge_agent import MergeAgent
from state.state_manager import StateManager
from utils.logger import get_logger

logger = get_logger('full_pipeline')


def main():
    """Run complete pipeline with import tracking and audit trail."""

    logger.info("=" * 80)
    logger.info("FULL DEDUPLICATION PIPELINE WITH TRACKING")
    logger.info("=" * 80)
    logger.info("")

    # Configuration
    input_file = 'input/sample_data.csv'

    try:
        # STEP 1: Import and Track CSV
        logger.info("Step 1: Importing CSV with tracking...")
        tracker = ImportTracker()
        import_id = tracker.import_csv_to_database(
            input_file,
            metadata={'source': 'sample_data', 'purpose': 'deduplication_test'}
        )
        logger.info(f"✓ Import complete: {import_id}")
        logger.info("")

        # Load the imported data
        df = pd.read_csv(input_file)
        df['import_id'] = import_id
        df['source_record_id'] = [f"{import_id}_R{idx+1:04d}" for idx in range(len(df))]

        logger.info(f"Loaded {len(df)} records")
        logger.info("")

        # STEP 2: Geographic Enrichment
        logger.info("Step 2: Geographic enrichment (ZIP/City/State lookups)...")
        geo_lookup = GeoLookup()
        df = geo_lookup.enrich_dataframe(df)
        logger.info("✓ Geographic enrichment complete")
        logger.info("")

        # STEP 3: Validation and Normalization
        logger.info("Step 3: Validation and normalization...")
        validation_agent = ValidationAgent()
        df = validation_agent.execute(df)
        logger.info(f"✓ Validated {len(df)} records")
        logger.info("")

        # STEP 3.5: Infer missing location data
        logger.info("Step 3.5: Inferring missing state/ZIP values...")
        from utils.helpers import infer_missing_location_data, normalize_zip
        df = infer_missing_location_data(df)

        # Re-normalize ZIP codes after pre-fill
        df['zip_normalized'] = df['zip'].apply(normalize_zip)

        logger.info("✓ Missing data inference complete")
        logger.info("")

        # STEP 4: Hybrid Matching (Fuzzy + AI)
        logger.info("Step 4: Hybrid matching (Fuzzy + AI)...")
        logger.info("Thresholds:")
        logger.info("  Auto-merge: ≥95% (high confidence)")
        logger.info("  AI analysis: 75-95% (uncertain)")
        logger.info("  Auto-reject: <75% (low confidence)")
        logger.info("")

        # Check if AI is enabled
        import os
        api_key = os.getenv('ANTHROPIC_API_KEY')
        ai_enabled = api_key and api_key != 'your-api-key-here'

        if not ai_enabled:
            logger.warning("⚠️  AI matching disabled (no API key)")
            logger.info("Uncertain cases (75-95%) will not be analyzed")
            logger.info("")

        hybrid_agent = HybridMatchingAgent({
            'fuzzy_threshold_high': 0.95,  # Auto-merge only 95%+
            'fuzzy_threshold_low': 0.75,
            'ai_threshold': 0.80,
            'ai_enabled': ai_enabled,
            'blocking_fields': ['state']  # Use state blocking (not ZIP)
        })

        df = hybrid_agent.execute(df)

        # Show matching statistics
        stats = hybrid_agent.get_stats()
        logger.info("")
        logger.info("Matching Statistics:")
        logger.info(f"  High confidence (≥95%): {stats['high_confidence_pairs']} pairs → Auto-merged")
        logger.info(f"  Uncertain (75-95%): {stats['uncertain_pairs']} pairs → AI analysis")

        if ai_enabled and stats['ai_analyzed_pairs'] > 0:
            logger.info(f"  AI confirmed: {stats['ai_confirmed_pairs']} pairs")
            logger.info(f"  AI rejected: {stats['ai_rejected_pairs']} pairs")
            savings = 100 * (1 - stats['uncertain_pairs']/max(stats['total_pairs'], 1))
            logger.info(f"  Cost savings: {savings:.1f}% vs AI-only approach")

        logger.info(f"  Low confidence (<75%): {stats['low_confidence_pairs']} pairs → Rejected")
        logger.info("✓ Matching complete")
        logger.info("")

        # Update source records with cluster assignments
        logger.info("Updating source records with cluster assignments...")
        tracker.update_source_records_with_clusters(df)
        logger.info("✓ Source records updated")
        logger.info("")

        # STEP 5: Merge Duplicates with Audit Trail
        logger.info("Step 5: Merging duplicates with audit trail...")
        merge_agent = MergeAgent({'merge_strategy': 'most_complete'})
        df_merged = merge_agent.execute(df)

        # Record each merge in audit trail
        logger.info("Recording merge audit trail...")
        for audit_entry in merge_agent.merge_audit:
            cluster_id = audit_entry['cluster_id']
            cluster = df[df['cluster_id'] == cluster_id]

            # Get source record IDs
            source_record_ids = cluster['source_record_id'].tolist() if 'source_record_id' in cluster.columns else []

            # Get match method and score
            match_method = cluster['match_method'].iloc[0] if 'match_method' in cluster.columns else 'fuzzy'
            similarity_score = cluster['similarity_score'].max() if 'similarity_score' in cluster.columns else 0.85

            # Record merge
            merge_info = {
                'cluster_id': cluster_id,
                'golden_record_id': f"GOLDEN_{import_id}_C{cluster_id}",
                'source_record_ids': source_record_ids,
                'merge_strategy': 'most_complete',
                'similarity_score': similarity_score,
                'match_method': match_method,
                'ai_reasoning': cluster['ai_reasoning'].iloc[0] if 'ai_reasoning' in cluster.columns else '',
                'field_selections': {}  # Track which fields came from which source
            }

            tracker.record_merge(merge_info)

        logger.info(f"✓ Recorded {len(merge_agent.merge_audit)} merge operations")
        logger.info("")

        # STEP 6: Save deduplicated results to database
        logger.info("Step 6: Saving deduplicated results...")
        from data.table_writer import TableWriter
        writer = TableWriter()
        writer.write_table(df_merged, 'business_associates_deduplicated', if_exists='replace')
        logger.info(f"✓ Saved {len(df_merged)} deduplicated records")
        logger.info("")

        # STEP 7: Final Summary
        logger.info("=" * 80)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 80)
        logger.info("")
        logger.info(f"Input Records: {len(df)}")
        logger.info(f"Output Records: {len(df_merged)}")
        logger.info(f"Duplicates Merged: {len(df) - len(df_merged)}")
        logger.info(f"Duplicate Clusters: {len(merge_agent.merge_audit)}")
        logger.info("")
        logger.info(f"Import ID: {import_id}")
        logger.info(f"Source Table: ba_source_records")
        logger.info(f"Audit Table: ba_merge_audit")
        logger.info("")

        # Show sample lineage
        logger.info("Sample Record Lineage:")
        logger.info("-" * 80)
        if len(merge_agent.merge_audit) > 0:
            sample_cluster = merge_agent.merge_audit[0]
            cluster_id = sample_cluster['cluster_id']
            golden_id = f"GOLDEN_{import_id}_C{cluster_id}"

            logger.info(f"\nGolden Record: {golden_id}")
            lineage = tracker.trace_record_lineage(golden_id)

            if 'error' not in lineage:
                logger.info(f"Merge Date: {lineage['merge_date']}")
                logger.info(f"Merge Strategy: {lineage['merge_strategy']}")
                logger.info(f"Match Method: {lineage['match_method']}")
                logger.info(f"Confidence: {lineage['similarity_score']:.2%}")

                if lineage.get('ai_reasoning'):
                    logger.info(f"AI Reasoning: {lineage['ai_reasoning']}")

                logger.info(f"\nSource Records ({len(lineage['source_records'])}):")
                for i, source in enumerate(lineage['source_records'], 1):
                    logger.info(f"  {i}. {source['source_record_id']}")
                    logger.info(f"     Name: {source['name']}")
                    logger.info(f"     Address: {source['address']}")
                    logger.info(f"     From: {lineage['import_info'].get('source_file', 'N/A')}")

        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ ALL STEPS COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)

        # Show how to query the audit trail
        logger.info("")
        logger.info("Query Audit Trail:")
        logger.info(f"  Import history: SELECT * FROM ba_imports")
        logger.info(f"  Source records with groups: SELECT * FROM ba_source_records WHERE import_id = '{import_id}'")
        logger.info(f"  Deduplicated records: SELECT * FROM business_associates_deduplicated")
        logger.info(f"  Merge audit: SELECT * FROM ba_merge_audit")
        logger.info("")
        logger.info("View Groups:")
        logger.info(f"  By cluster: SELECT cluster_id, COUNT(*) as count FROM ba_source_records GROUP BY cluster_id ORDER BY count DESC")
        logger.info(f"  With scores: SELECT source_record_id, name, cluster_id, similarity_score FROM ba_source_records WHERE cluster_id >= 0")
        logger.info("")

        return 0

    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
