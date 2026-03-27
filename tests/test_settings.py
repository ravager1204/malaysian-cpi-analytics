from config.settings import get_settings


def test_get_settings_resolves_project_paths():
    settings = get_settings()

    assert settings.project_root.name == "malaysian-cpi-analytics"
    assert settings.raw_data_dir == settings.project_root / "data" / "raw"
    assert settings.logs_dir == settings.project_root / "logs"
    assert settings.database.sqlalchemy_url.startswith("postgresql://")

