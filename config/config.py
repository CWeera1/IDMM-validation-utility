
import json

class Configuration:
    def load_config(self, config_file_path):
        """
        Load and parse the JSON configuration file.

        Args:
            config_file_path (str): Path to the JSON configuration file.

        Returns:
            dict: Parsed configuration data as a dictionary.
        """
        with open(config_file_path) as config_file:
            config_data = json.load(config_file)
        return config_data