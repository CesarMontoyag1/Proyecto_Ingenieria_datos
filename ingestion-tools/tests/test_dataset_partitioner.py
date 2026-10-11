import unittest
import pandas as pd
from src.dataset_partitioner import (
    AWID3_ATTACK_CATEGORIES,
    CATEGORY_FILE_PREFIX,
    get_category_file_paths,
    get_category_path,
    normalize_kaggle_path,
    partition_file_list,
    split_dataframe_rows,
)


class TestDatasetPartitioner(unittest.TestCase):
    def test_normalize_kaggle_path(self):
        self.assertEqual(normalize_kaggle_path("1.Deauth"), "CSV/1.Deauth")
        self.assertEqual(normalize_kaggle_path("CSV/1.Deauth"), "CSV/1.Deauth")
        self.assertEqual(normalize_kaggle_path("CSV\\10.SQL_Injection"), "CSV/10.SQL_Injection")
        self.assertEqual(normalize_kaggle_path("2.Disas/Disas_0.csv"), "CSV/2.Disas/Disas_0.csv")
        # Test autocorrect Rogue_AP_0.csv to RogueAP_0.csv
        self.assertEqual(
            normalize_kaggle_path("CSV/4.Rogue_AP/Rogue_AP_0.csv"),
            "CSV/4.Rogue_AP/RogueAP_0.csv"
        )

    def test_get_category_file_paths(self):
        # Rogue AP uses RogueAP_0.csv
        self.assertEqual(
            get_category_file_paths("4.Rogue_AP", limit_files=1),
            ["CSV/4.Rogue_AP/RogueAP_0.csv"]
        )
        self.assertEqual(
            get_category_file_paths("1.Deauth", limit_files=2),
            ["CSV/1.Deauth/Deauth_0.csv", "CSV/1.Deauth/Deauth_1.csv"]
        )

    def test_all_13_categories_have_prefixes(self):
        for cat in AWID3_ATTACK_CATEGORIES:
            paths = get_category_file_paths(cat, limit_files=1)
            self.assertEqual(len(paths), 1)
            self.assertTrue(paths[0].startswith(f"CSV/{cat}/"))
            self.assertTrue(paths[0].endswith("_0.csv"))

    def test_partition_file_list_even(self):
        files = ["file_0.csv", "file_1.csv", "file_2.csv", "file_3.csv"]
        batch, stream = partition_file_list(files, ratio=0.5)
        self.assertEqual(batch, ["file_0.csv", "file_1.csv"])
        self.assertEqual(stream, ["file_2.csv", "file_3.csv"])

    def test_partition_file_list_odd(self):
        files = ["file_0.csv", "file_1.csv", "file_2.csv"]
        batch, stream = partition_file_list(files, ratio=0.5)
        self.assertEqual(batch, ["file_0.csv"])
        self.assertEqual(stream, ["file_1.csv", "file_2.csv"])

    def test_partition_file_list_single(self):
        files = ["single.csv"]
        batch, stream = partition_file_list(files, ratio=0.5)
        self.assertEqual(batch, ["single.csv"])
        self.assertEqual(stream, ["single.csv"])

    def test_split_dataframe_rows(self):
        df = pd.DataFrame({
            "id": range(10),
            "val": [f"val_{i}" for i in range(10)]
        })
        batch_df, stream_df = split_dataframe_rows(df, ratio=0.5)
        self.assertEqual(len(batch_df), 5)
        self.assertEqual(len(stream_df), 5)
        self.assertEqual(list(batch_df["id"]), [0, 1, 2, 3, 4])
        self.assertEqual(list(stream_df["id"]), [5, 6, 7, 8, 9])


if __name__ == "__main__":
    unittest.main()


