"""
Output Skills - Reusable report generation and export functions.
Extracted from OutputAgent for standalone use.
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
from utils.logger import get_logger

logger = get_logger(__name__)


def export_golden_records(
    golden_df: pd.DataFrame,
    output_path: str = 'output/golden_records.csv',
    columns: List[str] = None
) -> str:
    """
    Export golden records to CSV file.

    Args:
        golden_df: DataFrame with golden records
        output_path: Output file path
        columns: Optional list of columns to export (default: all)

    Returns:
        Path to exported file

    Example:
        export_golden_records(golden_df, 'output/golden_records.csv')
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    if columns:
        export_df = golden_df[columns].copy()
    else:
        export_df = golden_df.copy()

    export_df.to_csv(output_path, index=False)

    logger.info(f"Exported {len(export_df)} golden records to {output_path}")

    return output_path


def export_duplicate_report(
    df: pd.DataFrame,
    output_path: str = 'output/duplicate_report.csv',
    include_score: bool = False
) -> str:
    """
    Export duplicate report with cluster information.

    Args:
        df: DataFrame with cluster_id column
        output_path: Output file path
        include_score: Whether to include completeness scores

    Returns:
        Path to exported file

    Example:
        export_duplicate_report(df, 'output/duplicates.csv', include_score=True)
    """
    if 'cluster_id' not in df.columns:
        raise ValueError("DataFrame must have 'cluster_id' column")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    export_df = df.copy()

    # Add completeness score if requested
    if include_score:
        from skills.merge_skills import calculate_completeness_score
        export_df['completeness_score'] = export_df.apply(calculate_completeness_score, axis=1)

    # Sort by cluster_id
    export_df = export_df.sort_values('cluster_id')

    export_df.to_csv(output_path, index=False)

    logger.info(f"Exported duplicate report to {output_path}")

    return output_path


def generate_statistics(df: pd.DataFrame, clustered_df: pd.DataFrame = None) -> Dict[str, Any]:
    """
    Generate deduplication statistics.

    Args:
        df: Original DataFrame before deduplication
        clustered_df: DataFrame after deduplication with cluster_id column (optional)

    Returns:
        Dictionary with statistics

    Example:
        stats = generate_statistics(original_df, deduplicated_df)
        print(f"Duplicates found: {stats['duplicate_count']}")
    """
    stats = {
        'total_records': len(df),
        'timestamp': datetime.now().isoformat()
    }

    if clustered_df is not None and 'cluster_id' in clustered_df.columns:
        duplicate_count = (clustered_df['cluster_id'] != -1).sum()
        cluster_count = clustered_df[clustered_df['cluster_id'] != -1]['cluster_id'].nunique()
        unique_count = len(clustered_df) - duplicate_count + cluster_count

        stats.update({
            'duplicate_records': duplicate_count,
            'cluster_count': cluster_count,
            'unique_records': unique_count,
            'singleton_records': (clustered_df['cluster_id'] == -1).sum(),
            'deduplication_rate': f"{(duplicate_count / len(df) * 100):.1f}%"
        })

    return stats


def generate_summary_report(
    original_df: pd.DataFrame,
    golden_df: pd.DataFrame,
    all_locations_df: pd.DataFrame = None,
    output_path: str = 'output/summary_report.txt'
) -> str:
    """
    Generate human-readable summary report.

    Args:
        original_df: Original DataFrame before deduplication
        golden_df: Golden records DataFrame
        all_locations_df: All locations DataFrame (optional)
        output_path: Output file path

    Returns:
        Path to report file

    Example:
        generate_summary_report(original_df, golden_df, all_locations_df)
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    report_lines = []
    report_lines.append("="*80)
    report_lines.append("DEDUPLICATION SUMMARY REPORT")
    report_lines.append("="*80)
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("")

    # Input statistics
    report_lines.append("INPUT DATA")
    report_lines.append("-" * 80)
    report_lines.append(f"Total records: {len(original_df):,}")
    report_lines.append(f"Columns: {len(original_df.columns)}")
    report_lines.append("")

    # Output statistics
    report_lines.append("OUTPUT DATA")
    report_lines.append("-" * 80)
    report_lines.append(f"Golden records (unique businesses): {len(golden_df):,}")

    if all_locations_df is not None:
        report_lines.append(f"All locations preserved: {len(all_locations_df):,}")

    duplicates_removed = len(original_df) - len(golden_df)
    dedup_rate = (duplicates_removed / len(original_df) * 100) if len(original_df) > 0 else 0

    report_lines.append(f"Duplicates merged: {duplicates_removed:,}")
    report_lines.append(f"Deduplication rate: {dedup_rate:.1f}%")
    report_lines.append("")

    # Cluster statistics
    if 'cluster_id' in golden_df.columns:
        clusters = golden_df[golden_df['cluster_id'] != -1]
        if len(clusters) > 0:
            report_lines.append("CLUSTER STATISTICS")
            report_lines.append("-" * 80)
            report_lines.append(f"Number of clusters: {clusters['cluster_id'].nunique():,}")
            report_lines.append(f"Records in clusters: {len(clusters):,}")
            report_lines.append(f"Singleton records: {(golden_df['cluster_id'] == -1).sum():,}")
            report_lines.append("")

    # Data quality
    report_lines.append("DATA QUALITY")
    report_lines.append("-" * 80)

    for field in ['address', 'phone', 'email']:
        if field in golden_df.columns:
            non_null = golden_df[field].notna() & (golden_df[field].astype(str).str.strip() != '')
            pct = (non_null.sum() / len(golden_df) * 100) if len(golden_df) > 0 else 0
            report_lines.append(f"Records with {field}: {non_null.sum():,} ({pct:.1f}%)")

    report_lines.append("")
    report_lines.append("="*80)

    # Write report
    report_text = "\n".join(report_lines)

    with open(output_path, 'w') as f:
        f.write(report_text)

    logger.info(f"Generated summary report: {output_path}")

    # Also log to console
    print("\n" + report_text)

    return output_path


def create_excel_report(
    golden_df: pd.DataFrame,
    all_locations_df: pd.DataFrame = None,
    cluster_summary_df: pd.DataFrame = None,
    output_path: str = 'output/deduplication_report.xlsx'
) -> str:
    """
    Create Excel report with multiple sheets.

    Args:
        golden_df: Golden records DataFrame
        all_locations_df: All locations DataFrame (optional)
        cluster_summary_df: Cluster summary DataFrame (optional)
        output_path: Output file path

    Returns:
        Path to Excel file

    Example:
        from skills.merge_skills import get_cluster_summary

        cluster_summary = get_cluster_summary(df)
        create_excel_report(golden_df, all_locations_df, cluster_summary)
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Sheet 1: Golden Records
        golden_df.to_excel(writer, sheet_name='Golden Records', index=False)

        # Sheet 2: All Locations (if provided)
        if all_locations_df is not None:
            all_locations_df.to_excel(writer, sheet_name='All Locations', index=False)

        # Sheet 3: Cluster Summary (if provided)
        if cluster_summary_df is not None:
            cluster_summary_df.to_excel(writer, sheet_name='Cluster Summary', index=False)

        # Sheet 4: Statistics
        stats_data = {
            'Metric': [
                'Total Golden Records',
                'Total Locations',
                'Number of Clusters',
                'Records with Address',
                'Records with Phone',
                'Records with Email'
            ],
            'Value': [
                len(golden_df),
                len(all_locations_df) if all_locations_df is not None else 0,
                golden_df[golden_df['cluster_id'] != -1]['cluster_id'].nunique() if 'cluster_id' in golden_df.columns else 0,
                (golden_df['address'].notna() & (golden_df['address'].astype(str).str.strip() != '')).sum() if 'address' in golden_df.columns else 0,
                (golden_df['phone'].notna() & (golden_df['phone'].astype(str).str.strip() != '')).sum() if 'phone' in golden_df.columns else 0,
                (golden_df['email'].notna() & (golden_df['email'].astype(str).str.strip() != '')).sum() if 'email' in golden_df.columns else 0,
            ]
        }
        stats_df = pd.DataFrame(stats_data)
        stats_df.to_excel(writer, sheet_name='Statistics', index=False)

    logger.info(f"Created Excel report: {output_path}")

    return output_path


def export_all(
    original_df: pd.DataFrame,
    golden_df: pd.DataFrame,
    all_locations_df: pd.DataFrame = None,
    output_dir: str = 'output'
) -> Dict[str, str]:
    """
    Export all output files (CSV, Excel, reports).

    Args:
        original_df: Original DataFrame
        golden_df: Golden records DataFrame
        all_locations_df: All locations DataFrame (optional)
        output_dir: Output directory

    Returns:
        Dictionary mapping file type to file path

    Example:
        files = export_all(original_df, golden_df, all_locations_df)
        print(f"Golden records: {files['golden_csv']}")
        print(f"Excel report: {files['excel']}")
    """
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"Exporting all outputs to {output_dir}")

    files = {}

    # Export golden records CSV
    files['golden_csv'] = export_golden_records(
        golden_df,
        output_path=str(output_dir_path / 'golden_records.csv')
    )

    # Export all locations CSV (if provided)
    if all_locations_df is not None:
        files['locations_csv'] = export_golden_records(
            all_locations_df,
            output_path=str(output_dir_path / 'all_locations.csv')
        )

    # Export duplicate report (if cluster_id present)
    if 'cluster_id' in golden_df.columns:
        files['duplicate_report'] = export_duplicate_report(
            golden_df,
            output_path=str(output_dir_path / 'duplicate_report.csv')
        )

    # Generate summary report
    files['summary_report'] = generate_summary_report(
        original_df,
        golden_df,
        all_locations_df,
        output_path=str(output_dir_path / 'summary_report.txt')
    )

    # Create Excel report
    from skills.merge_skills import get_cluster_summary

    cluster_summary = None
    if 'cluster_id' in golden_df.columns:
        cluster_summary = get_cluster_summary(golden_df)

    files['excel'] = create_excel_report(
        golden_df,
        all_locations_df,
        cluster_summary,
        output_path=str(output_dir_path / 'deduplication_report.xlsx')
    )

    logger.info(f"Exported {len(files)} output files")

    return files


def print_statistics(df: pd.DataFrame, golden_df: pd.DataFrame = None):
    """
    Print statistics to console.

    Args:
        df: Original DataFrame
        golden_df: Golden records DataFrame (optional)

    Example:
        print_statistics(original_df, golden_df)
    """
    print("\n" + "="*80)
    print("DEDUPLICATION STATISTICS")
    print("="*80)

    print(f"\nOriginal records: {len(df):,}")

    if golden_df is not None:
        print(f"Golden records: {len(golden_df):,}")
        duplicates_removed = len(df) - len(golden_df)
        dedup_rate = (duplicates_removed / len(df) * 100) if len(df) > 0 else 0
        print(f"Duplicates merged: {duplicates_removed:,} ({dedup_rate:.1f}%)")

        if 'cluster_id' in golden_df.columns:
            clusters = golden_df[golden_df['cluster_id'] != -1]
            if len(clusters) > 0:
                print(f"\nClusters: {clusters['cluster_id'].nunique():,}")
                print(f"Records in clusters: {len(clusters):,}")
                print(f"Singleton records: {(golden_df['cluster_id'] == -1).sum():,}")

    print("="*80 + "\n")
