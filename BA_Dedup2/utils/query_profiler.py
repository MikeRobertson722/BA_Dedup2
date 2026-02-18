"""
Database Query Profiler for performance optimization.

Provides utilities for:
- Logging SQL queries with execution time
- EXPLAIN QUERY PLAN analysis for SQLite
- Identifying slow queries
- Query optimization recommendations

Usage:
    from utils.query_profiler import QueryProfiler

    profiler = QueryProfiler(db_connection)
    with profiler.profile_query("SELECT * FROM table"):
        result = cursor.execute(query)
"""
import time
import sqlite3
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from utils.logger import get_logger

logger = get_logger(__name__)


class QueryProfiler:
    """
    Profile database queries for performance optimization.

    Tracks:
    - Query execution time
    - Query plans (EXPLAIN)
    - Slow queries
    - Query frequency
    """

    def __init__(self, db_connection: sqlite3.Connection,
                 slow_query_threshold: float = 1.0,
                 enable_explain: bool = False):
        """
        Initialize query profiler.

        Args:
            db_connection: SQLite database connection
            slow_query_threshold: Threshold in seconds to flag slow queries
            enable_explain: Enable EXPLAIN QUERY PLAN analysis (adds overhead)
        """
        self.db = db_connection
        self.slow_query_threshold = slow_query_threshold
        self.enable_explain = enable_explain
        self.query_log = []
        self.slow_queries = []
        self.query_count = 0

    @contextmanager
    def profile_query(self, query: str, params: Optional[tuple] = None):
        """
        Context manager to profile a query execution.

        Args:
            query: SQL query to profile
            params: Query parameters

        Usage:
            with profiler.profile_query("SELECT * FROM table WHERE id = ?", (1,)):
                cursor.execute(query, params)
                result = cursor.fetchall()
        """
        start_time = time.time()
        query_plan = None

        # Get query plan if enabled
        if self.enable_explain and query.strip().upper().startswith('SELECT'):
            query_plan = self._get_query_plan(query, params)

        try:
            yield
        finally:
            duration = time.time() - start_time
            self._log_query(query, params, duration, query_plan)

    def execute_and_profile(self, cursor: sqlite3.Cursor, query: str,
                           params: Optional[tuple] = None) -> List:
        """
        Execute query and profile it.

        Args:
            cursor: Database cursor
            query: SQL query
            params: Query parameters

        Returns:
            Query results
        """
        with self.profile_query(query, params):
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()

    def _get_query_plan(self, query: str, params: Optional[tuple] = None) -> List[Dict]:
        """
        Get EXPLAIN QUERY PLAN for a query.

        Args:
            query: SQL query
            params: Query parameters

        Returns:
            List of query plan steps
        """
        try:
            cursor = self.db.cursor()
            explain_query = f"EXPLAIN QUERY PLAN {query}"

            if params:
                cursor.execute(explain_query, params)
            else:
                cursor.execute(explain_query)

            plan = []
            for row in cursor.fetchall():
                # SQLite EXPLAIN QUERY PLAN returns: (id, parent, notused, detail)
                plan.append({
                    'id': row[0],
                    'parent': row[1],
                    'detail': row[3] if len(row) > 3 else row[2]
                })

            return plan

        except Exception as e:
            logger.warning(f"Failed to get query plan: {e}")
            return []

    def _log_query(self, query: str, params: Optional[tuple],
                   duration: float, query_plan: Optional[List[Dict]]):
        """
        Log query execution details.

        Args:
            query: SQL query
            params: Query parameters
            duration: Execution time in seconds
            query_plan: Query execution plan
        """
        self.query_count += 1

        # Create query log entry
        log_entry = {
            'query_id': self.query_count,
            'query': query[:200],  # Truncate long queries
            'params': str(params)[:100] if params else None,
            'duration': duration,
            'timestamp': time.time()
        }

        if query_plan:
            log_entry['query_plan'] = query_plan

        self.query_log.append(log_entry)

        # Check if slow query
        if duration >= self.slow_query_threshold:
            self.slow_queries.append(log_entry)
            logger.warning(f"⚠️  SLOW QUERY ({duration:.3f}s): {query[:100]}...")

            # Analyze slow query
            if query_plan:
                self._analyze_slow_query(query, query_plan, duration)

        # Debug log all queries
        logger.debug(f"Query {self.query_count}: {duration:.3f}s - {query[:80]}...")

    def _analyze_slow_query(self, query: str, query_plan: List[Dict], duration: float):
        """
        Analyze slow query and provide optimization suggestions.

        Args:
            query: SQL query
            query_plan: Query execution plan
            duration: Execution time
        """
        suggestions = []

        # Check for table scans
        for step in query_plan:
            detail = step['detail'].upper()

            if 'SCAN TABLE' in detail:
                table_name = detail.split('SCAN TABLE')[1].split()[0]
                suggestions.append(
                    f"Table scan detected on '{table_name}'. Consider adding an index."
                )

            if 'USING INTEGER PRIMARY KEY' in detail:
                # This is good - using primary key
                pass

            if 'USING INDEX' not in detail and 'SCAN' in detail:
                suggestions.append(
                    "Query not using index efficiently. Review WHERE clauses."
                )

        # Check for missing WHERE clause in SELECT
        if 'SELECT' in query.upper() and 'WHERE' not in query.upper():
            suggestions.append(
                "Query has no WHERE clause. Consider adding filters to reduce rows."
            )

        # Log suggestions
        if suggestions:
            logger.warning(f"Optimization suggestions for slow query:")
            for suggestion in suggestions:
                logger.warning(f"  - {suggestion}")

    def get_slow_queries(self) -> List[Dict]:
        """
        Get all slow queries.

        Returns:
            List of slow query entries
        """
        return self.slow_queries

    def get_query_stats(self) -> Dict[str, Any]:
        """
        Get query statistics.

        Returns:
            Dict with query statistics
        """
        if not self.query_log:
            return {
                'total_queries': 0,
                'slow_queries': 0,
                'avg_duration': 0,
                'max_duration': 0
            }

        durations = [q['duration'] for q in self.query_log]

        return {
            'total_queries': self.query_count,
            'slow_queries': len(self.slow_queries),
            'avg_duration': sum(durations) / len(durations),
            'max_duration': max(durations),
            'min_duration': min(durations),
            'total_time': sum(durations)
        }

    def print_summary(self):
        """Print query profiling summary."""
        stats = self.get_query_stats()

        print("\n" + "=" * 80)
        print("DATABASE QUERY PROFILING SUMMARY")
        print("=" * 80)
        print(f"Total Queries:       {stats['total_queries']}")
        print(f"Slow Queries:        {stats['slow_queries']} "
              f"(threshold: {self.slow_query_threshold}s)")
        print(f"Average Duration:    {stats['avg_duration']:.3f}s")
        print(f"Max Duration:        {stats['max_duration']:.3f}s")
        print(f"Total Query Time:    {stats['total_time']:.3f}s")
        print("=" * 80)

        # Show slowest queries
        if self.slow_queries:
            print("\nTop 5 Slowest Queries:")
            print("-" * 80)
            sorted_slow = sorted(self.slow_queries,
                               key=lambda x: x['duration'], reverse=True)[:5]
            for i, q in enumerate(sorted_slow, 1):
                print(f"{i}. {q['duration']:.3f}s - {q['query'][:70]}...")
                if 'query_plan' in q:
                    print(f"   Plan: {q['query_plan'][0]['detail']}")

        print()

    def export_report(self, filepath: str):
        """
        Export profiling report to JSON.

        Args:
            filepath: Output file path
        """
        import json
        from pathlib import Path

        report = {
            'stats': self.get_query_stats(),
            'slow_queries': self.slow_queries,
            'all_queries': self.query_log
        }

        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2)

        logger.info(f"Query profiling report exported to: {filepath}")


def analyze_index_usage(db_connection: sqlite3.Connection, table_name: str) -> Dict:
    """
    Analyze index usage for a table.

    Args:
        db_connection: Database connection
        table_name: Table to analyze

    Returns:
        Dict with index analysis
    """
    cursor = db_connection.cursor()

    # Get all indexes for table
    cursor.execute("""
        SELECT name, sql FROM sqlite_master
        WHERE type='index' AND tbl_name=?
    """, (table_name,))

    indexes = cursor.fetchall()

    # Get table info
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()

    return {
        'table': table_name,
        'columns': [col[1] for col in columns],
        'indexes': [{'name': idx[0], 'sql': idx[1]} for idx in indexes],
        'index_count': len(indexes)
    }


def suggest_indexes(db_connection: sqlite3.Connection,
                   query_log: List[Dict]) -> List[str]:
    """
    Suggest indexes based on query patterns.

    Args:
        db_connection: Database connection
        query_log: List of query log entries

    Returns:
        List of index suggestions
    """
    suggestions = []

    # Analyze WHERE clauses
    where_columns = {}

    for entry in query_log:
        query = entry['query'].upper()

        if 'WHERE' in query:
            # Simple heuristic: extract column names after WHERE
            # This is a simplified version - could be enhanced
            where_part = query.split('WHERE')[1]

            # Look for common patterns: WHERE column = value
            import re
            pattern = r'\b([a-z_]+)\s*[=<>]'
            matches = re.findall(pattern, where_part.lower())

            for match in matches:
                if match not in where_columns:
                    where_columns[match] = 0
                where_columns[match] += 1

    # Generate suggestions for frequently filtered columns
    for column, count in where_columns.items():
        if count >= 3:  # Appears in at least 3 queries
            suggestions.append(
                f"Consider adding index on column '{column}' "
                f"(used in {count} WHERE clauses)"
            )

    return suggestions


# Convenience decorator for profiling functions
def profile_queries(slow_threshold: float = 1.0):
    """
    Decorator to profile all queries in a function.

    Args:
        slow_threshold: Slow query threshold in seconds

    Usage:
        @profile_queries(slow_threshold=0.5)
        def my_database_function(conn):
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Try to find db connection in args/kwargs
            db_conn = None
            for arg in args:
                if isinstance(arg, sqlite3.Connection):
                    db_conn = arg
                    break

            if db_conn is None:
                db_conn = kwargs.get('conn') or kwargs.get('connection')

            if db_conn:
                profiler = QueryProfiler(db_conn, slow_threshold)
                # Store profiler for access
                kwargs['_profiler'] = profiler

            result = func(*args, **kwargs)

            if db_conn and '_profiler' in kwargs:
                profiler.print_summary()

            return result
        return wrapper
    return decorator
