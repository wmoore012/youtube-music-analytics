def test_dataset_adapter_returns_v2_shape():
    import importlib.util

    from datasets.music_industry_sentiment_dataset import get_music_industry_dataset

    ds = get_music_industry_dataset()

    # Basic shape expectations (v2)
    assert hasattr(ds, "entries")
    assert hasattr(ds, "get_train_test_split")
    has_pyd = importlib.util.find_spec("pydantic") is not None
    assert len(ds.entries) >= (200 if has_pyd else 100)

    # If pydantic is available, adapter should give v2 with intent/aspect
    e0 = ds.entries[0]
    assert hasattr(e0, "phrase") and hasattr(e0, "sentiment")
    if has_pyd:
        assert hasattr(e0, "intent") and hasattr(e0, "aspect")
