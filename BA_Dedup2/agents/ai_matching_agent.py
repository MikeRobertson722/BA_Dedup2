"""
AI Matching Agent - Uses Claude API for intelligent duplicate detection.
Leverages LLM understanding of semantic similarity, context, and business logic.
"""
import pandas as pd
import os
from typing import Dict, Any, List, Tuple
from agents.base_agent import BaseAgent
from config import settings
import anthropic
import json
from collections import defaultdict


class AIMatchingAgent(BaseAgent):
    """
    Agent that uses Claude AI to identify duplicate records through semantic understanding.
    More intelligent than fuzzy matching - understands context, abbreviations, and business rules.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize AI matching agent.

        Args:
            config: Configuration dictionary with keys:
                - api_key: Anthropic API key
                - model: Claude model to use
                - similarity_threshold: Confidence threshold for matches (0-1)
                - batch_size: Number of record pairs to send per API call
                - fields_to_compare: List of fields to send to AI
        """
        super().__init__('ai_matching', config)
        self.api_key = self.config.get('api_key', os.getenv('ANTHROPIC_API_KEY'))
        self.model = self.config.get('model', os.getenv('AI_MODEL', 'claude-sonnet-4-20250514'))
        self.similarity_threshold = self.config.get('similarity_threshold', 0.80)
        self.batch_size = self.config.get('batch_size', 10)
        self.fields_to_compare = self.config.get('fields_to_compare',
                                                 ['name', 'address', 'city', 'state', 'zip', 'phone'])

        if not self.api_key or self.api_key == 'your-api-key-here':
            raise ValueError("Anthropic API key not configured. Set ANTHROPIC_API_KEY in .env file")

        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.duplicate_pairs = []

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Find duplicate/similar records using AI analysis.

        Args:
            data: Input DataFrame to analyze

        Returns:
            DataFrame with duplicate cluster IDs added
        """
        self.logger.info(f"Finding duplicates using AI in {len(data)} records")
        self.logger.info(f"Model: {self.model}")
        self.logger.info(f"Confidence threshold: {self.similarity_threshold}")

        # Add a unique internal ID for tracking (use assign to avoid copy)
        if 'record_id' not in data.columns:
            df = data.assign(record_id=range(len(data)))
        else:
            df = data

        # Generate candidate pairs (using simple blocking first to reduce API calls)
        candidate_pairs = self._generate_candidate_pairs(df)
        self.logger.info(f"Generated {len(candidate_pairs)} candidate pairs for AI analysis")

        # Use AI to analyze pairs in batches
        duplicate_pairs = self._ai_analyze_pairs(df, candidate_pairs)
        self.logger.info(f"AI identified {len(duplicate_pairs)} duplicate pairs above threshold")

        # Cluster duplicates into groups
        df = self._cluster_duplicates(df, duplicate_pairs)

        duplicate_count = (df['cluster_id'] != -1).sum()
        cluster_count = df[df['cluster_id'] != -1]['cluster_id'].nunique()

        self.logger.info(f"Identified {duplicate_count} duplicate records in {cluster_count} clusters")

        return df

    def _generate_candidate_pairs(self, df: pd.DataFrame) -> List[Tuple[int, int]]:
        """
        Generate candidate pairs using simple blocking to reduce API costs.
        Only compare records in the same state or with similar names.

        Args:
            df: Input DataFrame

        Returns:
            List of (index1, index2) tuples representing candidate pairs
        """
        pairs = []

        # Block by state to reduce comparisons
        if 'state' in df.columns:
            for state in df['state'].dropna().unique():
                state_records = df[df['state'] == state].index.tolist()
                # Generate pairs within this state
                for i in range(len(state_records)):
                    for j in range(i + 1, len(state_records)):
                        pairs.append((state_records[i], state_records[j]))
        else:
            # Fall back to all pairs if no state field (expensive!)
            indices = df.index.tolist()
            for i in range(len(indices)):
                for j in range(i + 1, len(indices)):
                    pairs.append((indices[i], indices[j]))

        return pairs

    def _ai_analyze_pairs(self, df: pd.DataFrame,
                         candidate_pairs: List[Tuple[int, int]]) -> List[Dict[str, Any]]:
        """
        Use Claude AI to analyze pairs and determine if they're duplicates.

        Args:
            df: Input DataFrame
            candidate_pairs: List of candidate pairs to analyze

        Returns:
            List of duplicate pair dictionaries with confidence scores
        """
        duplicate_pairs = []

        # Process pairs in batches to optimize API usage
        for batch_start in range(0, len(candidate_pairs), self.batch_size):
            batch_end = min(batch_start + self.batch_size, len(candidate_pairs))
            batch_pairs = candidate_pairs[batch_start:batch_end]

            self.logger.debug(f"Processing batch {batch_start//self.batch_size + 1} "
                            f"({len(batch_pairs)} pairs)")

            # Analyze this batch
            batch_results = self._analyze_batch(df, batch_pairs)
            duplicate_pairs.extend(batch_results)

        return duplicate_pairs

    def _analyze_batch(self, df: pd.DataFrame,
                      pairs: List[Tuple[int, int]]) -> List[Dict[str, Any]]:
        """
        Analyze a batch of pairs using Claude API.

        Args:
            df: Input DataFrame
            pairs: List of pairs to analyze

        Returns:
            List of duplicate matches with confidence scores
        """
        # Prepare records for AI analysis
        pairs_data = []
        for idx1, idx2 in pairs:
            rec1 = df.loc[idx1]
            rec2 = df.loc[idx2]

            # Extract relevant fields
            record1 = {field: str(rec1.get(field, '')) for field in self.fields_to_compare
                      if field in rec1.index}
            record2 = {field: str(rec2.get(field, '')) for field in self.fields_to_compare
                      if field in rec2.index}

            pairs_data.append({
                'pair_id': f"{idx1}_{idx2}",
                'record1': record1,
                'record2': record2,
                'idx1': idx1,
                'idx2': idx2
            })

        # Build prompt for Claude
        prompt = self._build_analysis_prompt(pairs_data)

        try:
            # Call Claude API
            message = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )

            # Parse response
            response_text = message.content[0].text
            results = self._parse_ai_response(response_text, pairs_data)

            return results

        except Exception as e:
            self.logger.error(f"AI analysis failed: {e}")
            return []

    def _build_analysis_prompt(self, pairs_data: List[Dict]) -> str:
        """
        Build a prompt for Claude to analyze record pairs.

        Args:
            pairs_data: List of record pairs with metadata

        Returns:
            Formatted prompt string
        """
        prompt = """You are an expert at identifying duplicate business associate records in healthcare data.

Analyze the following pairs of Business Associate records and determine if they represent the same entity.
Consider:
- Name variations, nicknames, abbreviations (e.g., "Mike" = "Michael", "ABC Medical" = "A.B.C. Medical")
- Address variations (e.g., "Street" vs "St", "Suite 100" vs "#100")
- Missing data (one record may have more complete information)
- Typos and formatting differences

For each pair, respond with:
1. A confidence score (0.0-1.0) indicating how confident you are they're duplicates
2. A brief reasoning

Format your response as JSON array:
[
  {
    "pair_id": "0_1",
    "is_duplicate": true,
    "confidence": 0.95,
    "reasoning": "Same name (Mike = Michael), same address, same city/state/zip"
  },
  ...
]

Here are the record pairs to analyze:

"""

        for i, pair in enumerate(pairs_data, 1):
            prompt += f"\n--- Pair {i} (ID: {pair['pair_id']}) ---\n"
            prompt += f"Record 1: {json.dumps(pair['record1'], indent=2)}\n"
            prompt += f"Record 2: {json.dumps(pair['record2'], indent=2)}\n"

        prompt += "\n\nProvide your analysis as a JSON array:"

        return prompt

    def _parse_ai_response(self, response_text: str,
                          pairs_data: List[Dict]) -> List[Dict[str, Any]]:
        """
        Parse Claude's response and extract duplicate pairs.

        Args:
            response_text: Raw response from Claude
            pairs_data: Original pairs data for reference

        Returns:
            List of duplicate pair dictionaries
        """
        duplicate_pairs = []

        try:
            # Extract JSON from response (handle markdown code blocks)
            json_start = response_text.find('[')
            json_end = response_text.rfind(']') + 1

            if json_start == -1 or json_end == 0:
                self.logger.warning("Could not find JSON in AI response")
                return []

            json_text = response_text[json_start:json_end]
            results = json.loads(json_text)

            # Process results
            for result in results:
                if result.get('is_duplicate', False):
                    confidence = result.get('confidence', 0.0)

                    if confidence >= self.similarity_threshold:
                        # Find the original pair data
                        pair_id = result['pair_id']
                        pair_info = next((p for p in pairs_data if p['pair_id'] == pair_id), None)

                        if pair_info:
                            duplicate_pairs.append({
                                'record_id_1': pair_info['idx1'],
                                'record_id_2': pair_info['idx2'],
                                'similarity_score': confidence,
                                'ai_reasoning': result.get('reasoning', '')
                            })

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse AI response as JSON: {e}")
            self.logger.debug(f"Response text: {response_text}")
        except Exception as e:
            self.logger.error(f"Error parsing AI response: {e}")

        return duplicate_pairs

    def _cluster_duplicates(self, df: pd.DataFrame,
                          duplicate_pairs: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Cluster duplicate records into groups using connected components.

        Args:
            df: Input DataFrame
            duplicate_pairs: List of duplicate pair dictionaries

        Returns:
            DataFrame with cluster_id column added
        """
        # Initialize cluster IDs (-1 means no cluster)
        df['cluster_id'] = -1
        df['similarity_score'] = 0.0
        df['ai_reasoning'] = ''

        if not duplicate_pairs:
            return df

        # Build adjacency list for clustering
        graph = defaultdict(set)

        for pair in duplicate_pairs:
            id1 = pair['record_id_1']
            id2 = pair['record_id_2']
            graph[id1].add(id2)
            graph[id2].add(id1)

        # Find connected components (clusters)
        visited = set()
        cluster_id = 0

        def dfs(node, cluster):
            """Depth-first search to find connected records."""
            if node in visited:
                return
            visited.add(node)
            cluster.append(node)
            for neighbor in graph[node]:
                dfs(neighbor, cluster)

        # Assign cluster IDs
        for record_id in graph.keys():
            if record_id not in visited:
                cluster = []
                dfs(record_id, cluster)

                # Assign cluster ID to all records in this cluster
                for rec_id in cluster:
                    mask = df['record_id'] == rec_id
                    df.loc[mask, 'cluster_id'] = cluster_id

                cluster_id += 1

        # Store AI reasoning for duplicates
        for pair in duplicate_pairs:
            id1_mask = df['record_id'] == pair['record_id_1']
            id2_mask = df['record_id'] == pair['record_id_2']

            # Keep the highest similarity score and reasoning for each record
            current_score1 = df.loc[id1_mask, 'similarity_score'].values[0]
            if pair['similarity_score'] > current_score1:
                df.loc[id1_mask, 'similarity_score'] = pair['similarity_score']
                df.loc[id1_mask, 'ai_reasoning'] = pair['ai_reasoning']

            current_score2 = df.loc[id2_mask, 'similarity_score'].values[0]
            if pair['similarity_score'] > current_score2:
                df.loc[id2_mask, 'similarity_score'] = pair['similarity_score']
                df.loc[id2_mask, 'ai_reasoning'] = pair['ai_reasoning']

        return df

    def validate(self, result: pd.DataFrame) -> bool:
        """
        Validate AI matching results.

        Args:
            result: DataFrame to validate

        Returns:
            True if valid, False otherwise
        """
        if not super().validate(result):
            return False

        # Check that cluster_id column was added
        if 'cluster_id' not in result.columns:
            self.logger.error("cluster_id column not found in result")
            return False

        return True
