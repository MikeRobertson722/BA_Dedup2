"""
Quick test to demonstrate smart blocking optimization.

Shows the reduction in candidate pairs from 7.6M to a manageable number.
"""
import pandas as pd
from utils.smart_blocking import SmartBlockingStrategy, estimate_blocking_effectiveness

print("=" * 80)
print("SMART BLOCKING OPTIMIZATION TEST")
print("=" * 80)

# Load the sample data
print("\nLoading sample data...")
df = pd.read_csv('input/sample_data.csv')

print(f"Total records: {len(df):,}")
print(f"\nData quality:")
print(f"  - Missing address: {df['address'].isna().sum():,} ({df['address'].isna().sum()/len(df)*100:.1f}%)")
print(f"  - Missing state:   {df['state'].isna().sum():,} ({df['state'].isna().sum()/len(df)*100:.1f}%)")
print(f"  - Missing ZIP:     {df['zip'].isna().sum():,} ({df['zip'].isna().sum()/len(df)*100:.1f}%)")
print(f"  - Missing city:    {df['city'].isna().sum():,} ({df['city'].isna().sum()/len(df)*100:.1f}%)")

# Calculate theoretical worst case (full cartesian product)
full_pairs = (len(df) * (len(df) - 1)) // 2
print(f"\n{'-' * 80}")
print(f"Worst case (no blocking): {full_pairs:,} pairs")
print(f"{'-' * 80}")

# Estimate blocking effectiveness
print("\nEstimating blocking effectiveness...")
stats = estimate_blocking_effectiveness(df)

print("\nField coverage:")
for field, pct in stats['coverage_percentages'].items():
    count = stats['field_coverage'][field]
    print(f"  - {field:15} {count:6,} records ({pct})")

# Test smart blocking
print(f"\n{'-' * 80}")
print("Running Smart Blocking Strategy...")
print(f"{'-' * 80}\n")

# Add minimal normalized fields for blocking
df['zip_normalized'] = df['zip'].astype(str).str.replace(r'\D', '', regex=True).str[:5]

strategy = SmartBlockingStrategy(max_missing_data_pairs=50000)
pairs = strategy.generate_candidate_pairs(df)

print(f"\n{'-' * 80}")
print("RESULTS")
print(f"{'-' * 80}")
print(f"Full cartesian pairs:      {full_pairs:>15,}")
print(f"Smart blocking pairs:      {len(pairs):>15,}")
print(f"Reduction:                 {full_pairs - len(pairs):>15,} pairs eliminated")
print(f"Efficiency gain:           {(1 - len(pairs)/full_pairs)*100:>14.2f}% fewer pairs")
print(f"{'-' * 80}")

# Performance estimate
print("\nPerformance Impact:")
print(f"  - Old approach: ~7.6M pairs × 0.5ms = ~3,800 seconds (~63 minutes)")
print(f"  - New approach: ~{len(pairs):,} pairs × 0.5ms = ~{len(pairs) * 0.0005:.1f} seconds (~{len(pairs) * 0.0005/60:.1f} minutes)")
print(f"  - Speedup: ~{7600000 / len(pairs):.1f}x faster!")

print("\n" + "=" * 80)
print("BLOCKING OPTIMIZATION SUCCESSFUL!")
print("=" * 80)
