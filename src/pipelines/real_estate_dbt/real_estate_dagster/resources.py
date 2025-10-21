"""
Resources for the real estate Dagster project.
Provides database connections and dbt configuration.
"""
from pathlib import Path
import yaml
from dagster import ConfigurableResource
from dagster_dbt import DbtCliResource


class DatabaseResource(ConfigurableResource):
    """Database connection resource that reads from dbt profiles"""

    def get_connection_string(self) -> str:
        """Get database connection string from dbt profiles"""
        profiles_paths = [
            Path.home() / '.dbt' / 'profiles.yml',
            Path('profiles.yml'),
            Path('~/.dbt/profiles.yml').expanduser()
        ]

        for path in profiles_paths:
            if path.exists():
                try:
                    with open(path, 'r') as f:
                        profiles = yaml.safe_load(f)

                    profile_name = 'my_real_estate_project'
                    target = 'dev'

                    if profile_name in profiles:
                        profile = profiles[profile_name]
                        if 'outputs' in profile and target in profile['outputs']:
                            output = profile['outputs'][target]

                            if output.get('type') == 'postgres':
                                host = output.get('host', 'localhost')
                                port = output.get('port', 5432)
                                user = output.get('user')
                                password = output.get('password', '')
                                dbname = output.get('dbname')

                                return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
                except Exception as e:
                    continue

        raise ValueError("Could not find database configuration in dbt profiles")
