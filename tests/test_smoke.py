def test_smoke():
    from app.main import app
    assert app.title == "incremental-ingestion-service"
