#!/usr/bin/env python3
"""
Test the fixed ground truth labeling logic
"""

import sys

sys.path.insert(0, "src")

from youtubeviz.model_benchmark_system import ModelBenchmarkSystem


def test_ground_truth():
    """Test if the fixed ground truth logic finds labels."""

    print("🧪 TESTING FIXED GROUND TRUTH LOGIC")
    print("=" * 50)

    benchmark_system = ModelBenchmarkSystem()

    try:
        # Try to fetch a small dataset
        dataset = benchmark_system.fetch_benchmark_dataset(sample_size=100)

        print(f"📊 Dataset size: {len(dataset)}")

        if len(dataset) > 0:
            print(f"✅ SUCCESS! Found {len(dataset)} comments with ground truth labels")

            # Show distribution
            if "ground_truth" in dataset.columns:
                distribution = dataset["ground_truth"].value_counts()
                print(f"📈 Label distribution: {distribution.to_dict()}")

                # Show some examples
                print(f"\n💬 Example labeled comments:")
                for sentiment in ["positive", "negative", "neutral"]:
                    examples = dataset[dataset["ground_truth"] == sentiment].head(2)
                    if not examples.empty:
                        print(f"\n{sentiment.upper()} examples:")
                        for _, row in examples.iterrows():
                            comment = (
                                row["comment_text"][:60] + "..."
                                if len(row["comment_text"]) > 60
                                else row["comment_text"]
                            )
                            print(f"   \"{comment}\" (👍 {row['like_count']})")
            else:
                print("⚠️  No ground_truth column found")
        else:
            print("❌ Still no comments found with ground truth labels")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_ground_truth()
