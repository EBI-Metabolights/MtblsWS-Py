from pathlib import Path

application_path = Path(__file__).parent.parent

build_number_file = application_path / Path(".build_number")
__build_number__ = ""
if build_number_file.exists():
    __build_number__ = build_number_file.read_text(encoding="utf-8").replace("\n", "").strip()
    
app_version_file = application_path / Path("app_version")

app_version_number = "1.7.5"
if app_version_file.exists():
    app_version_number = app_version_file.read_text(encoding="utf-8").replace("\n", "").strip()
    
__app_version__ = app_version_number + f"-{__build_number__}" if __build_number__ else app_version_number

api_version_file = application_path / Path("api_version")

api_version_number = "1.7.5"
if api_version_file.exists():
    api_version_number = app_version_file.read_text(encoding="utf-8").replace("\n", "").strip()

__api_version__ = api_version_number + f"-{__build_number__}" if __build_number__ else api_version_number
